use crate::raft::entry;
use crate::raft::entry::Entry;

pub struct AppendEntriesRpcRequest {
    //leader
    leader_term: u64,
    leader_id: u16,
    prev_log_index: u64,
    prev_log_term: u64,
    leader_commit: u64,
    //entry
    entries: Vec<Entry>,
}
pub struct AppendEntriesRpcResponse {
    //Results
    term_results: Option<u64>,
    success: Option<bool>,
}

pub struct RequestVoteRpcRequest {
    // Arguments
    term_candidate: u64,
    candidate_id: u16,
    last_log_index: u64,
    last_log_term: u16,
}
pub struct RequestVoteRpcResponse {
    //Results
    term_results: u64,
    vote_granted: bool,
}
pub struct InstallSnapshotRpcRequest {
    // Arguments
    term_leader: u64,
    leader_id: u16,
    last_included_index: u64,
    last_included_term: u64,
    offset: u64,
    data: Vec<u8>,
    done: bool,
}
pub struct InstallSnapshotRpcResponse {
    //Results
    term_result: u64,
}

impl AppendEntriesRpcRequest {
    pub fn create_heartbeatrpc(
        //leader
        leader_term: u64,
        leader_id: u16,
        prev_log_index: u64,
        prev_log_term: u64,
        leader_commit: u64,
    ) -> AppendEntriesRpcRequest
    {
        AppendEntriesRpcRequest {
            leader_term,
            leader_id,
            prev_log_index,
            prev_log_term,
            leader_commit,
            entries: vec![],
        }
    }
}
impl AppendEntriesRpcResponse {

}



