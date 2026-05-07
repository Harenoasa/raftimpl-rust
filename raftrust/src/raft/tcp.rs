use crate::raft::node::{Node, NodeClonable};
use rand::RngExt;
use serde::de::IntoDeserializer;
use serde_json::map::Values;
use serde_json::{Value, json};
use std::backtrace::Backtrace;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Error, Formatter};
use std::io::{Bytes, Write};
use std::net::SocketAddr;
use std::ops::DerefMut;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Interval, sleep, timeout};

use crate::raft::tcp;

#[derive(Debug)]
pub struct TcpConnections {
    id_connection: HashMap<u16, TcpStream>,
}

impl TcpConnections {
    pub async fn initialize_connection(node_clonable: NodeClonable) -> TcpConnections {
        // println!("Initializing TCP connection for {:?}", node_clonable);
        // let node_clonable = node_clonable.clone();
        let upper_min_index = node_clonable.this_nodeid() + 1;
        let upper_max_index = node_clonable.node_len();
        let thisid = node_clonable.clone().this_nodeid();
        // println!("thisid: {}", thisid);
        //Listening on node whose id is smaller than this node.
        //被动监听下游节点
        let node_clonable_clone = node_clonable.clone();
        let downstream_establishment_handler = tokio::spawn(async move {
            let listener = TcpListener::bind(node_clonable_clone.get_this_socketaddr_ref())
                .await
                .expect("bind tcp error");
            let node_set = node_clonable_clone.return_remain_peer_set();
            // 小节点使用的异步处理循环终止set
            let arc_mutex_down_node_set = Arc::new(Mutex::new(node_set));
            // println!("set have elements {:?}", connection_not_established_set);
            // listen establishment
            Self::random_sleep(10, 20).await;
            // println!("connecting to {:?}", node_set);
            let max_connect_retry = 200;
            let mut while_count = 0;
            //创建mpsc，异步出去消费者给Self::receiver，生产者传给建立stream的异步线程
            let (stream_p, stream_c) = mpsc::unbounded_channel();
            // 监听下游节点的接收者
            let downstream_result_handler =
                Self::receiver(stream_c, arc_mutex_down_node_set.clone());
            // 把未建立连接的节点放在set里，以异步的方式完成连接并删除set相应元素，以判断是否完成所有连接
            while !arc_mutex_down_node_set.clone().lock().await.is_empty() {
                // println!("this nodeid : {}, set: {:?}",thisid , arc_mutex_down_node_set.clone().lock_owned().await);
                while_count += 1;
                if while_count > max_connect_retry {
                    panic!("out of maximum retry times of connection .")
                }

                let mut stream = match timeout(Duration::from_millis(100), listener.accept()).await
                {
                    Ok(Ok((stream, _))) => stream,
                    Ok(Err(e)) => {
                        eprintln!("Accept err:{}", e);
                        continue;
                    }
                    Err(_) => {
                        println!("no new connection incoming, keep wating... ");
                        continue;
                    }
                };
                // 克隆新实例传入异步线程闭包，传入外面变量可能导致超越其作用域周期，所以编译不会给过
                let nodeset_clone = arc_mutex_down_node_set.clone();
                let producer_clone = stream_p.clone();
                tokio::spawn(async move {
                    Self::random_sleep(100, 200).await;
                    // println!("waiting to read coming msgs");
                    let read_data = Self::retry_stream_read_operation(&mut stream, 10);
                    // unwrap一定是ok，失败重试，重试超出次数panic
                    let bytes = read_data.await.unwrap();
                    // println!("read_data completes");
                    let id = match TcpConnections::read_and_parse_id(bytes) {
                        Ok(id) => id,
                        Err(e) => {
                            println!("read_and_parse_id err:{}", e);
                            return Err(e);
                        }
                    };
                    // 尽可能减少持有锁的作用域
                    let contains_id: bool;
                    {
                        let mut nodeset = nodeset_clone.lock().await;
                        // println!("got set lock,set status :{:?}", nodeset);
                        contains_id = nodeset.contains(&id);
                        // println!("containsid?: {}", contains_id);
                    }

                    if contains_id == true {
                        // 响应，让发送方存储此连接
                        // println!("node id : {} 响应，让发送方存储此连接", id);
                        let json = String::from("{\"admission\": true}\n");
                        let bytes = json.into_bytes();
                        match Self::retry_write(&mut stream, bytes, 10).await {
                            Ok(_) => (),
                            Err(_) => (),
                        };
                        producer_clone
                            .send((id, stream))
                            .unwrap_or_else(|e| println!("mpsc error :: {}", e));
                    } else {
                        // 如果id重复，就发送拒接此连接的通知， 防止发送方超时等待
                        // println!(
                        //     "node id : {} 如果id重复，就发送拒接此连接的通知， 防止发送方超时等待",
                        //     id
                        // );
                        let json = String::from("{\"admission\": false}\n");
                        let bytes = json.into_bytes();
                        match Self::retry_write(&mut stream, bytes, 10).await {
                            Ok(_) => (),
                            Err(_) => (),
                        };
                        producer_clone
                            .send((id, stream))
                            .unwrap_or_else(|e| println!("mpsc error :: {}", e));
                    }
                    Ok(())
                });
                Self::random_sleep(900, 1000).await;
            }
            downstream_result_handler
                .await
                .map_err(|e| format!("error unwrapping downstream_result_handler ::{}", e))
        });

        // 主动连接上游节点
        let mut upper_node_stream_producers = Vec::new();
        for i in upper_min_index..upper_max_index + 1 {
            // println!("up node is {}",i);
            let thisnodeid = node_clonable.this_nodeid();
            let socket = node_clonable.get_socket_by_id(i);
            let stream_produce = tokio::spawn(async move {
                let max_reties = 20;
                let mut retry_counter = 0;
                // let retry_interval = Duration::from_millis(20);
                loop {
                    // println!("log 3");
                    if retry_counter >= max_reties {
                        panic!("out of maximum retry times of connecting.")
                    }

                    // Self::random_sleep(10, 300).await;
                    // println!("log 4");
                    // println!(
                    //     "node id = {}, retry time = {}",
                    //     bulleye_on_thisnodeid, retry_counter
                    // );
                    let mut stream = match Self::retry_conncetion(socket, 10).await {
                        Ok(stream) => stream,
                        Err(_) => panic!("tried maximum times"),
                    };
                    // println!("log 5 : connection received");
                    // thisnodeid: 这个向高节点发送连接请求的节点id
                    let request_json = format!("{{\"id\":{}}}\n", thisnodeid);
                    let bytes = request_json.into_bytes();
                    match Self::retry_write(&mut stream, bytes, 10).await {
                        Ok(_) => (),
                        //lib64println!("log 6 : write successful"),
                        Err(e) => println!("write failed {}", e),
                    };

                    match Self::check_response(&mut stream).await {
                        Ok(_) => {
                            // println!("log 7: response correct");
                            return stream;
                        }
                        Err(_) => continue,
                    };
                }
            });
            upper_node_stream_producers.push(stream_produce);
        }

        //等待所有handler完成，收集所有节点
        let downnodes = match downstream_establishment_handler.await.unwrap() {
            Ok(rcmutex) => rcmutex,
            Err(e) => {
                panic!("{}", e)
            }
        };
        let mut upnodes = HashMap::new();

        let mut id_index = upper_min_index;
        for handle in upper_node_stream_producers.into_iter() {
            if let Ok(stream) = handle.await {
                upnodes.insert(id_index, stream);
                id_index += 1;
            } else {
                panic!("handle err in innitialize up nodes");
            }
        }
        //merge two hashmap
        upnodes.extend(downnodes);
        // println!("node stream for [{}] created! : {:?}", thisid, upnodes);
        TcpConnections {
            id_connection: upnodes,
        }
    }

    fn receiver(
        mut downstream_receiver: UnboundedReceiver<(u16, TcpStream)>,
        mutex_remain_set: Arc<Mutex<HashSet<u16>>>,
    ) -> JoinHandle<HashMap<u16, TcpStream>> {
        tokio::spawn(async move {
            let mut downstream = HashMap::new();
            if mutex_remain_set.lock().await.is_empty() {
                return downstream;
            }
            loop {
                let (id, stream) = downstream_receiver.recv().await.unwrap();
                {
                    let mut locked_set = mutex_remain_set.lock().await;
                    // println!("receiver get this lock");
                    if !locked_set.contains(&id) {
                        continue;
                    }
                    downstream.insert(id, stream);
                    locked_set.remove(&id);
                    if locked_set.is_empty() {
                        return downstream;
                    }
                }
            }
        })
    }
    pub async fn read(stream: &mut TcpStream) -> Result<Vec<u8>, String> {
        // println!("log 1 start read function");
        let mut allbytes = Vec::new();
        let mut buffer = [0; 1024];
        // let bt = Backtrace::capture();
        // println!("{:#?}", bt);
        loop {
            let n = match timeout(Duration::from_millis(1500), stream.read(&mut buffer)).await {
                Ok(Ok(n)) if n == 0 => {
                    // println!("0 read detected during iteration.");
                    continue;
                }
                Ok(Ok(n)) => {
                    // println!("received msg size ; => {}", n);
                    n
                }
                Ok(Err(e)) => {
                    return {
                        // println!("read err {}", e);
                        Err(e.to_string())
                    };
                }
                Err(e) => {
                    // println!("log 2 : elapsed err {}", e);
                    return Err(format!("connect overtime {}", e.to_string()));
                }
            };
            if n == 0 {
                println!("log 2 =>  end read function");
                break;
            }
            allbytes.extend_from_slice(&buffer[0..n]);
            // 换行符\n对应的ASCII码：10
            let end_flag = 10;
            if end_flag == buffer[n - 1] {
                break;
            };
        }
        // println!("read end");
        Ok(allbytes)
    }
    fn read_json(bytes: Vec<u8>) -> Result<Value, String> {
        let json = match String::from_utf8(bytes) {
            Ok(string) => string,
            Err(e) => return Err(format!("Failed to parse id cause e : {}", e.to_string())),
        };
        match serde_json::from_str(&json) {
            Ok(value) => Ok(value),
            Err(e) => Err(format!("Failed to parse json : {}", e.to_string())),
        }
    }
    fn get_from_json_value<'a>(value: &'a Value, index: &str) -> Option<&'a Value> {
        value.get(index)
    }

    fn parse_value_to_u16(id_value: &Value) -> Result<u16, String> {
        match id_value
            .as_u64()
            .ok_or(String::from("None value return  in parsing to u64."))?
            .try_into()
        {
            Ok(id) => Ok(id),
            Err(e) => Err(format!("Failed to parse id cause e : {}", e.to_string())),
        }
    }
    fn read_and_parse_id(bytes: Vec<u8>) -> Result<u16, String> {
        let v: Value = Self::read_json(bytes)?;
        // println!("v : {}", v.to_string());
        let id_value = match v.get("id") {
            Some(value) => value.clone(),
            None => {
                println!("id not found in json data .");
                return Err("id not found in json data .".to_string());
            }
        };
        let val_64 = match id_value.as_u64() {
            Some(val_64) => val_64,
            None => return Err("parse u64 err".to_string()),
        };
        val_64
            .try_into()
            .map_err(|e| "TryFromIntError occured during parsing u16".to_string())
    }

    async fn sleep_function(
        duration: Duration,
        max_try: u32,
        retry_count: &mut u32,
        calculator: impl Fn(Duration, u32) -> Duration,
    ) -> Result<(), String> {
        if *retry_count >= max_try {
            return Err(String::from(
                "cannot establish connection! at clousure of initialize_connection. ",
            ));
        }
        let sleep_time = calculator(duration, *retry_count);
        sleep(sleep_time).await;
        *retry_count += 1;
        Ok(())
    }

    async fn connection_error_handler<E: Display>(
        e: E,
        retry_interval: Duration,
        max_reties: u32,
        retry_counter: &mut u32,
    ) -> Result<(), String> {
        println!(
            "connect err occured while establishing tcp connection: {}",
            e
        );
        match Self::sleep_function(retry_interval, max_reties, retry_counter, |a, b| {
            a * 10 * (2 ^ b)
        })
        .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn check_response(stream: &mut TcpStream) -> Result<(), String> {
        // println!("try read response");
        let bytedata = match Self::retry_stream_read_operation(stream, 10).await {
            Ok(data) => data,
            Err(e) => return Err(e),
        };
        // println!("response received");

        let values = Self::read_json(bytedata).unwrap();
        // println!("response values : {:?}", values);
        match values.get("admission") {
            Some(value) => match value.as_bool() {
                Some(true) => Ok(()),
                Some(false) => Err(String::from("wrong admission detected om check response.")),
                None => Err(String::from("parse bool err detected om check response.")),
            },
            None => Err(String::from("None value detect in check_response")),
        }
    }

    async fn random_sleep(min: u64, max: u64) {
        let ms;
        {
            let mut rng = rand::rng();
            ms = rng.random_range(min..=max); // 100 到 2000 毫秒
        }
        // println!("Sleeping for {} ms", ms);
        sleep(Duration::from_millis(ms)).await;
    }

    async fn retry_stream_read_operation(
        stream: &mut TcpStream,
        max_attempts: u32,
    ) -> Result<Vec<u8>, String> {
        Self::random_sleep(100, 200).await;
        for attempt in 1..=max_attempts {
            match Self::read(stream).await {
                Ok(val) => return Ok(val),
                Err(e) if attempt < max_attempts => {
                    println!("Attempt read {} failed: {}, retrying...", attempt, e);
                    Self::random_sleep(100, 200).await;
                }
                Err(e) => panic!("All {} attempts failed. Last error: {}", max_attempts, e),
            }
        }
        unreachable!()
    }
    pub async fn retry_write(
        stream: &mut TcpStream,
        bytes: Vec<u8>,
        max_attempts: u32,
    ) -> Result<(), String> {
        for attempt in 1..=max_attempts {
            match stream.write_all(&bytes).await {
                Ok(val) => return Ok(val),
                Err(e) if attempt < max_attempts => {
                    println!("Attempt write {} failed: {}, retrying...", attempt, e);
                    tokio::time::sleep(Duration::from_millis(100 as u64)).await;
                }
                Err(e) => panic!("All {} attempts failed. Last error: {}", max_attempts, e),
            }
        }
        unreachable!()
    }
    async fn retry_conncetion(socket: SocketAddr, max_attempts: u32) -> Result<TcpStream, String> {
        for attempt in 1..=max_attempts {
            let tcpsocket = TcpSocket::new_v4().unwrap();
            match timeout(Duration::from_millis(200), tcpsocket.connect(socket)).await {
                Ok(Ok(val)) => return Ok(val),
                Ok(Err(e)) if attempt < max_attempts => {
                    println!("Attempt connection {} failed: {}, retrying...", attempt, e);
                    tokio::time::sleep(Duration::from_millis(100 as u64)).await;
                }
                Ok(Err(e)) => panic!("All {} attempts failed. Last error: {}", max_attempts, e),
                Err(e) if attempt < max_attempts => {
                    println!("Attempt connect {} timeout: {}, retrying...", attempt, e);
                    tokio::time::sleep(Duration::from_millis(100 as u64)).await;
                }
                Err(_) => panic!("Timed out while establishing connection."),
            }
        }
        unreachable!()
    }

    async fn retry<F, D, Fut, T>(mut f: F, max_attempts: u32) -> Result<T, String>
    where
        F: FnMut() -> Fut,
        D: Display,
        Fut: Future<Output = Result<T, D>>,
    {
        for attempt in 1..=max_attempts {
            match f().await {
                Ok(val) => return Ok(val),
                Err(e) if attempt < max_attempts => {
                    println!("Attempt {} failed: {}, retrying...", attempt, e);
                    tokio::time::sleep(Duration::from_millis(100 as u64)).await;
                }
                Err(e) => panic!("All {} attempts failed. Last error: {}", max_attempts, e),
            }
        }
        unreachable!()
    }
}

async fn try_establish_tcp_with(id: u16, peer_socket: SocketAddr) -> (u16, TcpStream) {
    let max_retries = 10;
    let base_delay = Duration::from_millis(50);

    for attempt in 0..max_retries {
        match TcpStream::connect(peer_socket).await {
            Ok(stream) => {
                println!("Successfully established to {}", peer_socket);
                return (id, stream);
            }
            Err(e) => {
                println!(
                    "Failed to connect to {}: {} (attempt {})",
                    peer_socket,
                    e,
                    attempt + 1
                );
                if attempt < max_retries - 1 {
                    sleep(base_delay * 2u32.pow(attempt)).await;
                } else {
                    panic!("Failed to connect to peer {} after retries", peer_socket);
                }
            }
        }
    }
    unreachable!()
}

struct TimeOut {}

impl Display for TimeOut {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "future has timeout.")
    }
}
