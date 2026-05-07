use raftrust::raft::node::Node;
use raftrust::raft::tcp;
use raftrust::raft::tcp::TcpConnections;
use raftrust::raft::*;
use serde_json::map::Values;
use serde_json::{Value, value};
use std::fs::File;
use tokio::io::AsyncWriteExt;

#[tokio::test]
pub async fn test_initialize_connection() {
    let config = File::open("./config.yaml").unwrap();
    let mut cluster = Node::insert_node_on_single_server(config).unwrap();

    let mut handles = Vec::new();
    for node in cluster.iter() {
        let nodeclonable = node.clonable();
        // println!("clonable node id ::  {}", nodeclonable.this_nodeid());
        let joinhandle =
            tokio::spawn(async move { TcpConnections::initialize_connection(nodeclonable).await });
        handles.push(joinhandle);
    }
    let mut i = 0;
    for handle in handles {
        // println!("set node stream start");
        cluster[i].set_node_stream(handle.await.unwrap());
        // println!("set node stream ends");
        i += 1;
    }

    for node in cluster {
        println!("id:{}::{:?}", node.this_nodeid(), node.get_node_stream());
    }
}

#[tokio::test]
pub async fn test_init_node() {
    let request_json = format!("{{\"id\":{}}}\n", 1);
    let value: Value = serde_json::from_str(request_json.as_str()).unwrap();
    let str = value.get("id").unwrap().clone();
    println!("init node id : {}", str);
}
