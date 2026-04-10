use std::collections::HashMap;
use std::fmt::Error;
use serde::{Deserialize, Serialize};
use crate::raft::state_machine::StateMachine;

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Set {key: String, value: String},
    Get {key: String },
    Del {key: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    SetOk,
    GetOk(Option<String>),
    DelOk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Kv{
    data: HashMap<String, String>,
}

impl StateMachine for Kv{
    type Command = (Command);
    type Response = (Response);
    type Error = (Error);

    fn apply(&mut self, command: Self::Command) -> Result<Self::Response, Self::Error> {
          match command {
              Command::Set {key, value} => {
                  self.data.insert(key, value);
                  Result::Ok(Response::SetOk)
              }
              Command::Get { key } => {
                  let entry = self.data.get(&key).expect("failed to get key");
                  Result::Ok(Response::GetOk(Some(entry.clone():self.to_owned())))
              },
              Command::Del { key } => {
                  self.data.remove(&key);
                  Result::Ok(Response::DelOk)
              }
          }

    }

    fn snapshot(&self) -> Vec<u8> {
        todo!()
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), Self::Error> {
        todo!()
    }
}