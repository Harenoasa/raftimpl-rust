use crate::raft::node::{Node, NodeClonable};
use std::collections::{HashMap, HashSet};
use std::io::{Bytes, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;

use serde::de::Error;
use serde_json::map::Values;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::raft::tcp;

#[derive(Debug)]
pub struct TcpConnections {
    id_connection: HashMap<u16, TcpStream>,
}

impl TcpConnections {
    pub async fn initialize_connection(node_clonable: NodeClonable) -> TcpConnections {
        // println!("Initializing TCP connection for {:?}", node_clonable);
        let nodeclonable_clone = node_clonable.clone();

        //Listening on node whose id is smaller than this node.
        let joinhandle = tokio::spawn(async move {
            let listener = TcpListener::bind(node_clonable.get_this_socketaddr_ref())
                .await
                .expect("bind tcp error");
            let connection_not_established_set = node_clonable.return_remain_peer_set();
            TcpConnections::listen_on_coming_establishing_connection(
                listener,
                node_clonable,
                connection_not_established_set,
            )
            .await
        });
        // println!("Initializing TCP connection successfull: {:?}", nodeclonable_clone.this_nodeid());

        //Actively try establish connection to node which has bigger id
        let handles = Self::send_tcp_establish_request(nodeclonable_clone);
        let mut downnodes = joinhandle.await.unwrap();
        let mut upnodes = HashMap::new();
        for handle in handles.into_iter() {
            if let Ok((id, stream)) = handle.await.unwrap() {
                upnodes.insert(id, stream);
            } else {
                panic!("handle err in innitialize up nodes");
            }
        }
        //merge two hashmap
        downnodes.extend(upnodes);

        TcpConnections {
            id_connection: downnodes,
        }
    }

    async fn listen_on_coming_establishing_connection(
        listener: TcpListener,
        node_clonable: NodeClonable,
        mut connection_not_established_set: HashSet<u16>,
    ) -> HashMap<u16, TcpStream> {
        let mut downnodes = HashMap::new();
        // println!("set have elements {:?}", connection_not_established_set);

        // create some retry settings
        let mut retry_counter = 0;
        let max_retry = 5 * node_clonable.node_len();
        let retry_interval = Duration::from_millis(20);
        loop {
            println!("connecting to {:?}", connection_not_established_set);
            if connection_not_established_set.is_empty() {
                println!("break once");
                break;
            }
            match listener.accept().await {
                Ok((mut stream, _)) => {
                    let bytes = TcpConnections::read(&mut stream).await;
                    let id = match TcpConnections::read_and_parse_id(bytes) {
                        Ok(id) => id,
                        Err(e) => {
                            println!("parse id err occured retying : {}", e);
                            if retry_counter as u16 >= max_retry {
                                panic!(
                                    "cannot establish connection! at clousure of initialize_connection. "
                                )
                            };
                            retry_counter += 1;
                            sleep(retry_interval).await;
                            continue;
                        }
                    };
                    if connection_not_established_set.contains(&id) {
                        downnodes.insert(id, stream);
                        connection_not_established_set.remove(&id);
                    }
                }
                Err(e) => {
                    println!("esdablishing tcp conn err, retrying: {}", e);
                    if retry_counter as u16 >= max_retry {
                        panic!(
                            "cannot establish connection! at clousure of initialize_connection. "
                        )
                    };
                    retry_counter += 1;
                    sleep(retry_interval).await;
                }
            }
        }
        downnodes
    }
    //send establish request only to ndoe with bigger id.
    fn send_tcp_establish_request(
        node: NodeClonable,
    ) -> Vec<JoinHandle<Result<(u16, TcpStream), String>>> {
        let mut handles = Vec::new();
        for i in node.this_nodeid() + 1..node.node_len() + 1 {
            let bulleye_on_thisnodeid = node.this_nodeid();
            let socket = node.get_socket_by_id(i);
            let handle = tokio::spawn(async move {
                let max_reties = 10;
                let mut retry_counter = 0;
                let retry_interval = Duration::from_millis(20);

                loop {
                    println!(
                        "node id = {}, retry time = {}",
                        bulleye_on_thisnodeid, retry_counter
                    );
                    let tcpsocket = TcpSocket::new_v4().unwrap();
                    let mut stream = match tcpsocket.connect(socket).await {
                        Ok(stream) => stream,
                        Err(e) => {
                            match Self::sleep_function(retry_interval,
                                                 max_reties,
                                                 &mut retry_counter,
                                                 String::from("cannot establish connection! at clousure of initialize_connection. "),
                                                 |a, b| { a * 10 * (2 ^ b) }).await {
                                Ok(_) => continue,
                                Err(e) => panic!("{}",e),
                            }
                        }
                    };
                    stream
                        .write(json!({"id": i}).to_string().as_bytes())
                        .await
                        .expect("write unsuccessful");
                }
            });
            handles.push(handle);
        }
        handles
    }
    async fn read(stream: &mut TcpStream) -> Vec<u8> {
        println!("log 1");
        let mut allbytes = Vec::new();
        let mut buffer = [0; 1024];

        loop {
            let n = stream.read(&mut buffer).await.unwrap();
            println!("log 2");

            if n == 0 {
                break;
            }
            allbytes.extend_from_slice(&buffer[0..n]);
        }
        allbytes
    }
    fn read_json(bytes: Vec<u8>) -> Result<Value, String> {
        let json = match String::from_utf8(bytes) {
            Ok(string) => string,
            Err(e) => return Err(format!("Failed to parse id cause e : {}", e.to_string())),
        };
        Ok(serde_json::from_str(&json).unwrap())
    }
    fn get_from_json_value<'a>(value: &'a Value, index: &str) -> Option<&'a Value> {
        value.get(index)
    }

    async fn write(stream: &mut TcpStream, bytes: &Vec<u8>) -> i8 {
        match stream.write(bytes).await {
            Ok(n) => {
                if n > 0 {
                    1
                } else {
                    0
                }
            }
            Err(e) => -1,
        }
    }

    fn parse_string_to_u16(id_value: &Value) -> Result<u16, String> {
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
        let id_value = Self::get_from_json_value(&v["id"], "id")
            .ok_or(String::from("id not found in json data ."))?;
        Self::parse_string_to_u16(id_value)
    }

    async fn sleep_function(
        duration: Duration,
        max_try: u32,
        retry_count: &mut u32,
        err_msg: String,
        calculator: impl Fn(Duration, u32) -> Duration,
    ) -> Result<(), String> {
        if *retry_count >= max_try {
            return Err(err_msg);
        }
        let sleep_time = calculator(duration, *retry_count);
        sleep(sleep_time).await;
        *retry_count += 1 ;
        Ok(())
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
