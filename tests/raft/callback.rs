use bifrost::raft::*;
use bifrost::raft::state_machine::master::ExecError;
use bifrost::rpc::Server;

raft_state_machine! {
    def cmd trigger();
    def sub trigged();
}

