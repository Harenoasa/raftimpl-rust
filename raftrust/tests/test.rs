use std::fs::File;
use tokio::io::AsyncWriteExt;
use raftrust::raft::*;
use raftrust::raft::node::Node;
use raftrust::raft::tcp;
use raftrust::raft::tcp::TcpConnections;

#[tokio::test]
pub async fn test_initialize_connection(){
    let config = File::open("./config.yaml").unwrap();
    let mut cluster = Node::insert_node_on_single_server(config).unwrap();

    let mut handles = Vec::new();
    for node in cluster.iter() {
        let nodeclonable = node.clonable();
        let joinhandle
            = tokio::spawn(async move { TcpConnections::initialize_connection(nodeclonable).await });
        handles.push(joinhandle);
    }
    let mut i = 0;
    for handle in handles {
        cluster[i].set_node_stream(handle.await.unwrap());
        i += 1;
    }

    for node in cluster {
        println!("{:?}", node.get_node_stream());
    }
}