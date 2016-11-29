service! {
    rpc AppendEntries(term: u64, leaderId: u64, prev_log_id: u64, prev_log_term: u64, entries: Vec<Vec<u8>>, leader_commit: u64) -> u64; //Err for not success
    rpc RequestVote(term: u64, candidate_id: u64, last_log_id: u64, last_log_term: u64) -> (u64, bool); // term, voteGranted
    rpc InstallSnapshot(term: u64, leader_id: u64, lasr_included_index: u64, last_included_term: u64, data: Vec<u8>, done: bool) -> u64;
}