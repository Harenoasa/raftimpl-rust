use serde::{Deserialize, Serialize};

#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct Entry {
    term: u64,
    command: Vec<u8>,
}
impl Entry {
    pub fn read_term (&self) -> u64{
        self.term
    }
}




