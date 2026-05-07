use raftrust::raft::node::Node;
use raftrust::raft::raftrpc::AppendEntriesRpcRequest;
use std::error::Error;
use std::fs::File;
use std::time::Duration;

async fn listen(node: Node) {
    let mut heartbeat_interval = tokio::time::interval(Duration::from_millis(node.get_heartbeat()));
    loop {
        tokio::select! {
            _ = heartbeat_interval.tick(),if node.is_leader() => {

            },
        }
    }
}

fn send_heartbeat(node: Node) {
    for (nodeid, socketaddr) in node.get_nodelist() {
        let heartbeatrpc = AppendEntriesRpcRequest::create_heartbeatrpc(
            node.this_term(),
            node.this_nodeid(),
            node.node_prev_index(*nodeid),
            node.node_prev_term(*nodeid),
            node.leader_commit(),
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = File::open("./config.yaml").unwrap();
    let cluster = Node::insert_node_on_single_server(config).unwrap();
    let mut handles = Vec::new();
    for node in cluster {
        let handle = tokio::spawn(async move {
            listen(node).await;
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.await?;
    }
    Ok(())
}
