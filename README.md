# bifrost
WIP Raft and RPC impelementation

### Objective

The objective of bifrost is to build a solid foundation for distributed systems in rust.
It is similar to one of my Clojure project [cluster-connecter](https://github.com/shisoft/cluster-connector), but no longer require any third-party software like Zookeeper or etcd. 
Bifrost will ship with it's own reliable data store based on [raft consensus algorithm](https://raft.github.io/) state machines. Users are also able to build their own reliable data structures by implementing state machine commands.  

**Bifrost is still in very early stage of development and it is not suggested to be used in any kinds of projects until it is stabilized and fully tested** 

###Progress Check List

- [ ] RPC
    - [x] TCP Server
    - [x] Protocol
    - [x] Event driven server
    - [x] Sync client
    - [ ] Async client
    - [ ] Multiplexing pluggable services
- [ ] Raft (data replication)
    - [x] Leader election
    - [x] Log replication
    - [x] Master/subs state machine framework
    - [ ] State machine client
        - [x] Sync
        - [ ] PubSub
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
        - [X] Interfaces
        - [X] Update procedures
    - [ ] Cluster bootstrap
    - [ ] Client
        - [x] Command 
        - [x] Query 
            - [x] Concurrency
        - [x] Failover
        - [x] Membership changes 
    - [ ] Raft Group
    - [ ] Tests
        - [x] State machine framework
        - [x] Leader selection
        - [x] Log replication
        - [ ] Snapshot
        - [ ] Membership changes
            - [x] New member
            - [x] Delete member
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
    - [x] Value
    - [x] Number
    - [ ] Lock
- [ ] Integration (API)
    - [ ]  gPRC
    