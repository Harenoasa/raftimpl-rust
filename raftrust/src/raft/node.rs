use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque, btree_set::Intersection},
    error::Error,
    fmt::{Debug, Display},
    fs::File,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    rc::Rc,
    sync::Arc,
    usize,
};

use rand::{RngExt, distr::Uniform};
use serde::Deserialize;
#[derive(Debug)]
enum Role {
    Leader,
    Candidate,
    Follower,
}
#[derive(Debug)]
pub struct Node {
    //persistent states
    current_term: u64,
    voted_for: Option<u16>,
    log: Vec<String>,
    //Volatile state on all servers
    commit_index: u64,
    last_applied: u64,
    //Volatile state on leaders
    next_index: Vec<u64>,
    match_index: Vec<u64>,
    // role: Role,
    // id: u16,
    // node_list: Option<HashMap<u16, Rc<SocketAddr>>>,
    // socketaddr: Option<Rc<SocketAddr>>,
    // heartbeat: u16,
    // uniform: Arc<Uniform<u16>>,
    // election_timout: Option<u16>,
}
#[derive(Debug, Deserialize)]
pub struct Configure {
    nodes: Vec<Ipid>,
    max_election_timeout: u16,
    min_election_timeout: u16,
    heartbeat: u16,
}
#[derive(Debug, Deserialize)]
pub struct Ipid {
    id: String,
    ip: String,
}
impl Node {
    // test in single server with this function: create cluster Vec<Node> and return.
    fn update_node_on_single_server(configuration_file: File) -> Result<Vec<Node>, Box<dyn Error>> {
        let mut config = Node::read_file(configuration_file);

        //parse ip into SocketAddr
        let ip_socketaddrs = Node::return_sockets(&config.nodes);
        // init Rc<Uniform>
        let uniform: Arc<Uniform<u16>> = Node::get_fixed_uniform(&config); // create cluster 1;
        let mut cluster: Vec<Node> = Node::create_nodes(ip_socketaddrs, &uniform, config.heartbeat);
        Node::install_socket_list(&mut cluster);
        Ok(cluster)
    }

    fn read_file(configuration_file: File) -> Configure {
        match Node::load_config(configuration_file) {
            Ok(config) => config,
            Err(e) => {
                panic!()
            }
        }
    }
    fn return_sockets(nodes: &Vec<Ipid>) -> HashMap<u16, SocketAddr> {
        nodes
            .iter()
            .map(|ipid| -> (u16, SocketAddr) {
                (
                    ipid.id.parse().expect("parse String err"),
                    Node::parse_socketaddr(&ipid.ip).unwrap(),
                )
            })
            .collect()
    }
    fn get_fixed_uniform(config: &Configure) -> Arc<Uniform<u16>> {
        Arc::new(
            Uniform::new(config.min_election_timeout, config.max_election_timeout)
                .expect("initialize fixed Uniform err"),
        )
    }
    fn create_nodes(
        ip_socketaddrs: HashMap<u16, SocketAddr>,
        uniform: &Arc<Uniform<u16>>,
        heartbeat: u16,
    ) -> Vec<Node> {
        ip_socketaddrs
            .into_iter()
            .map(|ip_socketaddr| -> Node {
                Node::new(ip_socketaddr.0, ip_socketaddr.1, uniform, heartbeat)
            })
            .collect()
    }
    fn install_socket_list(cluster: &mut Vec<Node>) {
        let node_len: u16 = cluster.len().try_into().expect("parse u16 err");
        let mut id_index: u16 = 1;
        loop {
            if id_index >= node_len {
                break;
            }

            let peers_some = Option::Some(
                cluster
                    .iter()
                    .filter(|node_inter| id_index != node_inter.id)
                    .map(|node_inter| -> (u16, Rc<SocketAddr>) {
                        (
                            node_inter.id,
                            node_inter
                                .socketaddr
                                .as_ref()
                                .expect("node_inter: Option unwarp err")
                                .clone(),
                        )
                    })
                    .collect::<HashMap<u16, Rc<SocketAddr>>>(),
            );
            let id_index_usize: usize = id_index.try_into().expect("into err");
            cluster
                .get_mut(id_index_usize)
                .expect("getmut err")
                .node_list = peers_some;
            id_index += 1;
        }
    }

    // read <project-root>/config parsing it to Configure instance and return.
    fn load_config(configuration_file: File) -> Result<Configure, Box<dyn Error>> {
        let file = File::open("./config.yaml").expect("Failed to open config.yaml");
        let config: Configure = serde_yaml::from_reader(file)?;
        Ok(config)
    }
    // given a socket-formatted string and return a SocketAddr instance.
    fn parse_socketaddr(socket_string: &str) -> Result<SocketAddr, Box<dyn Error>> {
        let mut _ip_segs: Vec<&str> = socket_string.split(".").collect();

        let _the_last_seg_and_port: Vec<&str> = _ip_segs
            .pop()
            .expect("str_segment_error")
            .split(":")
            .collect();
        let (new_last, port_str) = (_the_last_seg_and_port[0], _the_last_seg_and_port[1]);
        _ip_segs.push(new_last);
        let ip: IpAddr = IpAddr::V4(Ipv4Addr::new(
            _ip_segs[0].parse()?,
            _ip_segs[1].parse()?,
            _ip_segs[2].parse()?,
            _ip_segs[3].parse()?,
        ));
        let port = port_str.parse()?;
        Ok(SocketAddr::new(ip, port))
    }
    //init node except node_list,timeout (cause node is initializing by batch porcess)
    fn new(
        _loop_index: u16,
        socketaddr: SocketAddr,
        uniform: &Arc<Uniform<u16>>,
        heart_sec: u16,
    ) -> Node {
        Node {
            current_term: 0,
            role: Role::Follower,
            id: _loop_index,
            node_list: Option::None,
            socketaddr: Option::Some(Rc::new(socketaddr)),
            heartbeat: heart_sec,
            uniform: uniform.clone(),
            election_timout: None,
        }
    }

    fn rand_timout(&self) -> u16 {
        rand::rng().sample(self.uniform.as_ref())
    }
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "term:{:?} role:{:?} id:{:?} peeers:{:?} socketaddr:{:?}",
            self.current_term, self.role, self.id, self.node_list, self.socketaddr,
        )
    }
}
#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_get_cluster() {
        let configuration_file =
            File::open("./config.yaml").expect("err occured during reading fils");
        let cluster = Node::update_node_on_single_server(configuration_file);
        cluster
            .iter()
            .for_each(|node| println!("hello!!{:?}", node));
    }

    // #[test]
    // fn test_loadconfig() {
    //     println!("{:?}", Node::load_config().expect("error occured"))
    // }

    // #[test]
    // fn test_initialization() {
    //     let ip = String::from("192.168.0.1:8080");
    //     match Node::parse_socketaddr(&ip) {
    //         Ok(socket) => println!("socket {:?}", socket),
    //         Err(msg) => println!("{}", msg),
    //     }
    // }
}
