use bifrost::raft;

#[test]
fn startup(){
    let server = raft::start_server(&String::from("127.0.0.1:2000"));
}