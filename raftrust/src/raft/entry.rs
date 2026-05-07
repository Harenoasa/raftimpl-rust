use serde::{Deserialize, Serialize};

use crate::raft::command::Command;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    term: u64,
    cmd: Command,
}

impl Entry {
    pub fn read_term(&self) -> u64 {
        self.term
    }
    pub fn read_command(&self) -> &Command {
        &self.cmd
    }
}
