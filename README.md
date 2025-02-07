# Sahamati (सहमतिः)
Sanskrit for Consensus

## About

Sahamati is a [Raft](https://raft.github.io) implementation in Go and has full support for the following features:

* Leader Election + Log Replication
* Persistence
* Membership Changes
* Log Compaction

Sahamati uses GRPC-compatible [Connect protocol](https://connectrpc.com/docs/introduction/) over HTTP3 (QUIC) transport for communicating between peers in the cluster.

Sahamati uses a leveldb based WAL for resilience.

> **NOTE**
> 
> While Sahamati is functionally complete and has all the features mentioned above, it is still a work
> in progress and as such is currently in `alpha`.
> 
> `v1.0.0` release is when it can be expected to be production ready.
> 

## Optimizations

### Split Vote Issue

A split vote issue is one where multiple nodes in a cluster become candidates simultaneously and
by design each votes for itself creating a situation where no other candidate gets a majority for that term.

Sahamati addresses this issue taking a simple approach based on node IDs in the cluster:

* when an election starts, nodes start off voting for themselves as candidates (if not already voted for another)
* however, here, even after self-vote, if a node comes across a node ID lesser than its own, it will discard it's self vote
  and vote for this other candidate.
* since node IDs are unique and monotonic in a cluster, this helps mitigate split vote issue to a great extent without the need
  for additional latency/complexity introduced by pre-votes or other network based approaches.

In addition to that, Sahamati implements the election timeout with the following tweak:

* the timeout scales based on the node ID (max timeout is still 300ms), this way, nodes with smaller
IDs are more likely to have smaller election timeouts


## Example

* [An example key-value store](example) is provided for reference on how to use Sahamati.
* [`example/kvstore-server/kvstore.go`](example/kvstore-server/kvstore.go) is a basic key-value store that uses Sahamati for consensus
* [`example/kvstore-server/server.go`](example/kvstore-server/server.go) is a basic Chi-router based http server that allows interacting with `kvstore`
* [`example/kvstore-server/main.go`](example/kvstore-server/main.go) builds the service and runs it.

### Quick Run
* To give it a quick spin:
```shell
# 1. Clone the repo
git clone https://github.com/althk/sahamati.git

# 2. cd into it
cd sahamati

# 3. Run a local cluster (default size is 3 nodes)
make run_example

# The above command will start:
# a. 3 raft nodes on addrs localhost:6001, localhost:6002, localhost:6003
# b. 3 "kvstore" http server, a basic key-value store (example use of Sahamati) serving on ports 8001,8002,8003 
```
The above commands will print some basic information include the node that became the leader.

To test the above running cluster (assuming `localhost:6001` raft node is the leader)
```shell
# send the request to the kvstore http service
curl -X POST http://localhost:8001/kvs -d '{"key":"sahamati", "value": "सहमतिः"}'
# the service should respond with `OK` if the Raft cluster committed the entry (majority of nodes accepted it). 
```
To read the value back (from another node):
```shell
curl -X GET http://localhost:8002/kvs/sahamati
```
It should print the value we stored earlier.

To stop the cluster:
```shell
make stop_example
```

## Benchmarks

### Example `kvstore` `POST` adding new entries [benchmark code](./example/bench/bench_test.go)
```shell
zsh 5424  (git)-[main]-% go test -bench . ./example/bench -args --post-url "http://localhost:8001/kvs" --get-url "http://localhost:8002/kvs"
goos: linux
goarch: amd64
pkg: github.com/althk/sahamati/example/bench
cpu: AMD Ryzen 7 7700X 8-Core Processor             
BenchmarkPut-16    	   18567	    106863 ns/op	     14718 creates/s
PASS
ok  	github.com/althk/sahamati/example/bench	3.142s
```
This is with a cluster size of 3 nodes.

## TODOs (in no particular order)

* Allow TLS certificate validation for Raft cluster nodes (incl mutual validation)
* Optimize client communication by proxying request to the leader instead of rejecting a request
* Add node and cluster-level telemetry out of the box (otel compatible)

## Credits
* Apart from the Raft papers at raft.github.io, [Eli Bendersky's blog posts](https://eli.thegreenplace.net/2020/implementing-raft-part-0-introduction/) helped me get started initially.

