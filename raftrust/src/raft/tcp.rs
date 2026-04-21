use crate::raft::node::{Node, NodeClonable};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use serde::de::Error;
use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
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
            TcpConnections::listen_on_coming_establish_connection(listener, node_clonable, connection_not_established_set).await
        });
        // println!("Initializing TCP connection successfull: {:?}", nodeclonable_clone.this_nodeid());


        let node = nodeclonable_clone;
        let mut handles = Vec::new();
        for i in node.this_nodeid() + 1..node.node_len() + 1 {
            let bulleye_on_thisnodeid = node.this_nodeid();
            let socket = node.get_socket_by_id(i);
            let handle = tokio::spawn(async move {
                let max_reties = 10;
                let mut retry_counter = 0;
                let retry_interval = Duration::from_millis(20);

                loop {
                    println!("node id = {}, retry time = {}", bulleye_on_thisnodeid, retry_counter);
                    let tcpsocket = TcpSocket::new_v4().unwrap();
                    match tcpsocket.connect(socket).await{
                        Ok(stream) => {
                            return Ok((i,stream))
                        },
                        Err(e) => {
                            retry_counter += 1;
                            println!("node {} Failed to connect to peer {} (attempt {}/{}): {}",
                                     bulleye_on_thisnodeid,i, retry_counter, max_reties, e);
                            if retry_counter >= max_reties {
                                return Err("cannot establish connection! at clousure of initialize_connection. ")
                            }
                            // sleep(retry_interval * 10 *(2^retry_counter)).await;
                            sleep(retry_interval).await;
                            // println!("sleep passed");
                            continue;
                        }
                    }
                }
            });
            handles.push(handle);
        }

        let mut downnodes = joinhandle.await.unwrap();
        let mut upnodes = HashMap::new();
        for handle in handles.into_iter(){
            if let Ok((id,stream)) = handle.await.unwrap(){
                upnodes.insert(id,stream);
            }else {
                panic!("handle err in innitialize up nodes");
            }
        }
        //merge two hashmap
        downnodes.extend(upnodes);

        TcpConnections {
            id_connection: downnodes,
        }
    }

    async fn listen_on_coming_establish_connection(listener: TcpListener,
                                                   node_clonable: NodeClonable,
                                                   mut connection_not_established_set: HashSet<u16>) -> HashMap<u16,TcpStream>{
        let mut downnodes = HashMap::new();
        // println!("set have elements {:?}", connection_not_established_set);

        // create some retry settings
        let mut retry_counter = 0;
        let max_retry = 5 * node_clonable.node_len();
        let retry_interval = Duration::from_millis(20);
        loop {
            println!("connecting to {:?}", connection_not_established_set);
            if connection_not_established_set.is_empty() { println!("break once");break; }
            match listener.accept().await {
                Ok((mut stream,_)) => {

                    let bytes= TcpConnections::readtcp(&mut stream).await;
                    let id = match TcpConnections::read_and_parse_id(bytes){
                        Ok(id) => {
                            id
                        },
                        Err(e) => {
                            println!("parse id err occured retying : {}",e);
                            if retry_counter as u16 >= max_retry { panic!("cannot establish connection! at clousure of initialize_connection. ")};
                            retry_counter += 1;
                            sleep(retry_interval).await;
                            continue
                        }
                    };
                    if connection_not_established_set.contains(&id) {
                        downnodes.insert(id,stream);
                        connection_not_established_set.remove(&id);
                    }
                },
                Err(e) => {
                    println!("esdablishing tcp conn err, retrying: {}", e);
                    if retry_counter as u16 >= max_retry { panic!("cannot establish connection! at clousure of initialize_connection. ")};
                    retry_counter += 1;
                    sleep(retry_interval).await;
                }
            }
        }
        downnodes
    }
    async fn readtcp(stream: &mut TcpStream) -> Vec<u8> {
        println!("log 1");
        let mut allbytes = Vec::new();
        let mut buffer = [0;1024];

        loop{
            let n = stream.read(&mut buffer).await.unwrap();
            println!("log 2");

            if n == 0 {
                break;
            }
            allbytes.extend_from_slice(&buffer[0..n]);
        }
        allbytes
    }

    fn read_and_parse_id(bytes: Vec<u8>) -> Result<u16,String>{
        let json = match String::from_utf8(bytes) {
            Ok(string) => string,
            Err(e) => return Err(format!("Failed to parse id cause e : {}", e.to_string())),
        };
        let v:Value = serde_json::from_str(&json).unwrap();
        let id = match v.get("id") {
            Some(id_value) => id_value.as_u64().unwrap() as u16,
            None => return Err(format!("no any id found check json : {}", json)),
        };
        Ok(id)
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
