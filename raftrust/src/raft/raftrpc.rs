enum RaftRpc {
    AppendEntries(AppendEntriesRequest),
    RequestVoteRpc(RequestVoteRpcRequest),
    InstallSnapshotRpc(InstallSnapshotRpcRequest),
}

struct AppendEntriesRequest {
    //leader
    term_leader: u64,
    leader_id: u64,
    prev_log_index: u64,
    prev_log_term: u64,
    leader_commit: u64,
    //Results
    term_results: u64,
    success: bool,
}
struct RequestVoteRpcRequest {
    // Arguments
    term_candidate: u64,
    candidate_id: u16,
    last_log_index: u64,
    last_log_term: u16,
    //Results
    term_results: u64,
    vote_granted: bool,
}
struct InstallSnapshotRpcRequest {
    // Arguments
    term_leader: u64,
    leader_id: u16,
    last_included_index: u64,
    last_included_term: u64,
    offset: u64,
    data: Vec<u8>,
    done: bool,
    //Results
    term_result: u64,
}