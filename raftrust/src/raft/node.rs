use std::{
    collections::HashMap,
    error::Error,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
/// [ip_initializer()] render a specific ip to Node from configuration.yaml
///
enum Role {
    Leader,
    Candidate,
    Follower,
}

pub struct Node {
    term: u64,
    role: Role,
    id: u64,
    peers: HashMap<u64, SocketAddr>,
    socket: SocketAddr,
}
impl Node {
    fn parse_socketaddr(socket_string: &str) -> Result<SocketAddr, Box<dyn Error>> {
        let (ipref, portref) = socket_string
            .split_once(':')
            .ok_or("ip_render: illegal input")?;
        let mut _ip_segs: Vec<&str> = ipref.split(".").collect();

        let _the_last_seg_and_port: Vec<&str> = _ip_segs
            .pop()
            .expect("str_segment_error")
            .split(":")
            .collect();
        let (new_last, portstr) = (_the_last_seg_and_port[0], _the_last_seg_and_port[1]);
        _ip_segs.push(new_last);
        let ip: IpAddr = IpAddr::V4(Ipv4Addr::new(
            ipref.parse()?,
            ipref[4..7].parse()?,
            ipref[8..11].parse()?,
            ipref[12..14].parse()?,
        ));
        println!("debug ip : {}", ip);
        let port = portref.parse()?;
        println!("debug port : {}", port);
        Ok(SocketAddr::new(ip, port))
    }
}
#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_initialization() {
        let ip = String::new("192.168.0.1:8080");
        Node::parse_socketaddr(ip);
    }
}
