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

use crate::raft::node::Role::Leader;
use rand::{RngExt, distr::Uniform};
use serde::Deserialize;
use crate::raft::entry::Entry;

#[derive(Debug)]
pub enum Role {
    Leader,
    Candidate,
    Follower,
}
#[derive(Debug)]
pub struct Node {
    //persistent states
    current_term: u64,
    voted_for: Option<u16>,
    log: Vec<Entry>,
    //Volatile state on all servers
    commit_index: u64,
    last_applied: u64,
    //Volatile state on leaders
    next_index: HashMap<u16, u64>,
    match_index: HashMap<u16, u64>,
    // other definitions
    id: u16,
    role: Role,
    node_list: HashMap<u16, SocketAddr>,
    heartbeat: u64,
    uniform: Arc<Uniform<u64>>,
    election_timout: Option<u64>,
}


#[derive(Debug, Deserialize)]
pub struct Configure {
    nodes: Vec<Ipid>,
    max_election_timeout: u64,
    min_election_timeout: u64,
    heartbeat: u64,
}
#[derive(Debug, Deserialize)]
pub struct Ipid {
    id: String,
    ip: String,
}
impl Node {
    // test in single server with this function: create cluster Vec<Node> and return.
    pub fn insert_node_on_single_server(configuration_file: File) -> Result<Vec<Node>, String> {
        let mut config = Node::read_file(configuration_file);
        //parse ip into SocketAddr, associate it with nodeid
        let nodelist = match Node::return_sockets(&config.nodes) {
            Ok(map) => map,
            Err(string) => return Err(string),
        };
        // init Rc<Uniform>
        let uniform: Arc<Uniform<u64>> = Node::get_fixed_uniform(&config); // create cluster 1;
        let cluster: Vec<Node> = Node::create_nodes(nodelist, &uniform, config.heartbeat);
        Ok(cluster)
    }

    fn read_file(configuration_file: File) -> Configure {
        Node::load_config(configuration_file).unwrap_or_else(|e| {
            println!("{}", e);
            panic!()
        })
    }
    fn return_sockets(nodes: &Vec<Ipid>) -> Result<HashMap<u16, SocketAddr>, String> {
        let mut node_iter = nodes.into_iter();
        let mut idip_map: HashMap<u16, SocketAddr> = HashMap::new();
        let mut counter: u16 = 1;
        while let Some(ipid) = node_iter.next() {
            let id_u16: u16 = match ipid.id.parse() {
                Ok(id_u16) => id_u16,
                Err(_) => {
                    return Err(String::from(
                        "err occured in return sockets: unable to parse String to u16 : {} => u16",
                    ));
                }
            };
            if id_u16 != counter {
                println!(
                    "config filr error: node is not declared in ascending order. please check your config file"
                );
                return Err(String::from(
                    "config filr error: node is not declared in ascending order. please check your config file",
                ));
            }
            idip_map.insert(
                id_u16,
                match Node::parse_socketaddr(&ipid.ip) {
                    Ok(socket) => socket,
                    Err(_) => {
                        return Err(String::from(
                            "Err ocurred when parsing String into SocketAddr ",
                        ));
                    }
                },
            );
            counter += 1;
        }
        Ok(idip_map)
    }
    fn get_fixed_uniform(config: &Configure) -> Arc<Uniform<u64>> {
        Arc::new(
            Uniform::new(config.min_election_timeout, config.max_election_timeout)
                .expect("initialize fixed Uniform err"),
        )
    }
    fn create_nodes(
        nodelist: HashMap<u16, SocketAddr>,
        uniform: &Arc<Uniform<u64>>,
        heartbeat: u64,
    ) -> Vec<Node> {
        let copy_nodelist = nodelist.clone();

        let mut node_iter = nodelist.into_iter();
        let mut cluster = Vec::new();
        while let Some((id, _)) = node_iter.next() {
            // give init 1 value to each other node
            let next_index: HashMap<u16, u64> = copy_nodelist
                .iter()
                .filter(|(inter_id, _)| id != **inter_id)
                .map(|(id, _)| (*id, 1))
                .collect();
            // give init 0 value to each other node
            let match_index = copy_nodelist
                .iter()
                .filter(|(inter_id, _)| id != **inter_id)
                .map(|(id, _)| (*id, 0))
                .collect();
            cluster.push(Node::new(
                id,
                next_index,
                match_index,
                &copy_nodelist,
                uniform,
                heartbeat,
            ));
        }
        cluster
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
        id: u16,
        next_index: HashMap<u16, u64>,
        match_index: HashMap<u16, u64>,
        nodelist: &HashMap<u16, SocketAddr>,
        uniform: &Arc<Uniform<u64>>,
        heart_milisec: u64,
    ) -> Node {
        Node {
            // node basic setup
            role: Role::Follower,
            id: id,
            node_list: nodelist.clone(),
            heartbeat: heart_milisec,
            uniform: uniform.clone(),
            election_timout: None,
            //raft feature
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            //leader
            // create two line
            next_index,
            match_index,
        }
    }

    fn generate_election_timout(&self) -> u64 {
        rand::rng().sample(self.uniform.as_ref())
    }

    pub fn get_election_timout(&self) -> u64 {
        self.generate_election_timout()
    }

    pub fn get_heartbeat(&self) -> u64 {
        self.heartbeat
    }

    pub fn get_nodelist(&self) -> &HashMap<u16, SocketAddr> {
        &self.node_list
    }

    pub fn is_leader(&self) -> bool {
        match self.role {
            Leader => true,
            _ => false,
        }
    }

    pub fn this_nodeid(&self) -> u16 {
        self.id
    }

    pub fn this_term(&self) -> u64{
        self.current_term
    }
    pub fn node_prev_index(&self, id: u16) -> u64 {
        let nextindex = self.next_index.get(&id).expect("get next index err").clone();
        nextindex - 1
    }

    pub fn node_prev_term(&self, nodeid: u16) -> u64 {
        let logindex :usize= self.node_prev_index(nodeid)
            .try_into().unwrap();
        self.log[logindex].read_term()
    }

    pub fn leader_commit(&self) -> u64 {
        self.commit_index
    }
}

// impl Display for Node {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(
//             f,
//             "term:{:?} role:{:?} id:{:?} peeers:{:?} socketaddr:{:?}",
//             self.current_term, self.role, self.id, self.node_list, self.socketaddr,
//         )
//     }
// }
