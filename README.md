# bifrost
WIP Raft and RPC impelementation

### Objective

The objective of bifrost is to build a solid foundation for distributed systems in rust.
It is similar to another of my Clojure project [cluster-connecter](https://github.com/shisoft/cluster-connector), but no longer require any third-party software like Zookeeper or etcd. 
Bifrost will ship with it's own reliable data store based on raft [consensus algorithm](https://raft.github.io/) state machines. Users are also able to build their own reliable data structures by implementing state machine commands.  

###Progress Check List

- [ ] RPC
    - [x] TCP Server
    - [x] Protocol
    - [x] Event driven server
    - [x] Sync client
    - [ ] Async client
- [ ] Raft (data replication)
    - [x] Leader election
    - [x] Log replication
    - [x] Master/subs state machine framework
    - [ ] Master state machine snapshot
        - [x] Generate
        - [x] Install
        - [ ] Generate in chunks
        - [ ] Install in chunks
        - [ ] Automation
        - [ ] Persistent to disk
        - [ ] Recover from disk
        - [ ] Incremental snapshot
    - [ ] Membership changes
        - [x] State machine
            - [x] New Member
            - [x] Delete Member
            - [x] Snapshot
            - [x] Recover
        - [ ] Interfaces
        - [ ] Update procedures
    - [ ] Client
        - [x] Command 
        - [x] Query 
            - [x] Concurrency
        - [x] Failover
        - [ ] Membership changes 
    - [ ] Raft Group
    - [ ] Tests
        - [ ] State machine framework
        - [ ] Leader election
        - [ ] Log replication
        - [ ] Snapshot
        - [ ] Membership changes
        - [ ] Safety
        - [ ] Stress and benchmark
        - [ ] Stress + Safety
- [ ] Sharding
    - [ ] DHT
- [ ] Reliable data store
    - [ ] Raft interface
    - [ ] Client group membership
    - [ ] Client group leader election
    - [ ] Map
    - [ ] Set
    - [ ] Array
    - [ ] Queue
    - [ ] Value
    - [ ] Number
    - [ ] Lock
- [ ] Integration (API)
    - [ ]  gPRC
    