#[cfg(test)]
mod tests {
    use std::fs::File;
    use crate::raft::node::Node;
    use super::*;

    #[test]
    fn test_get_cluster() {
        let configuration_file =
            File::open("./config.yaml").expect("err occured during reading fils");
        match Node::insert_node_on_single_server(configuration_file){
            Ok(vec) => vec.iter().for_each(|node| println!("{:?}", node)),
            Err(_) => panic!(),
        };
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