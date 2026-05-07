use std::ops::Deref;

use super::tcp::TcpConnections;
use postcard::to_vec;
use serde::{Deserialize, Deserializer, Serialize};
use tokio::{io::AsyncWriteExt, net::TcpStream};
const MAX_SIZE: usize = 64 * 1024;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Command {
    // Raft RPC 类型
    // RequestVote RPC
    RV,
    // AppendEntries RPC
    AE,
    // InstallSnapshot RPC
    IS,

    //Other,
    Print { msg: String },
    Ping,
    Pong,
    Shutdown,
}

/// 生成字节流
pub fn encode(cmd: &Command) -> Result<heapless::Vec<u8, MAX_SIZE>, String> {
    let serial_cmd = postcard::to_vec::<_, MAX_SIZE>(&cmd).unwrap();
    let len = serial_cmd.len() as u32;
    let mut packet: heapless::Vec<u8, MAX_SIZE> = heapless::Vec::new();
    packet.extend_from_slice(&len.to_be_bytes()).unwrap();
    packet.extend_from_slice(&serial_cmd).unwrap();
    Ok(packet)
}

/// 解析字节流
pub fn decode(packet: heapless::Vec<u8, MAX_SIZE>) -> Result<Command, postcard::Error> {
    postcard::from_bytes(&packet)
}

pub async fn send(stream: &mut TcpStream, cmd: &Command) -> Result<(), String> {
    let packet = match encode(cmd) {
        Ok(packet) => packet,
        Err(e) => {
            println!("{}", e);
            return Err(e.to_string());
        }
    };
    stream.write_all(&packet).await.map_err(|e| e.to_string())
}
