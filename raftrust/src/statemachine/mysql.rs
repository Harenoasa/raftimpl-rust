use crate::raft::state_machine;
use crate::raft::state_machine::StateMachine;

enum Command {
    Insert(String),
    Remove(String),
    Update(String, String),
}

struct Mysql {



}

impl StateMachine for Mysql {
    type Command = ();
    type Response = ();
    type Error = ();

    fn apply(&mut self, command: Self::Command) -> Result<Self::Response, Self::Error> {
        todo!()
    }

    fn snapshot(&self) -> Vec<u8> {
        todo!()
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), Self::Error> {
        todo!()
    }
}