# Sahamati (सहमतिः)
Sanskrit for Consensus

## About

Sahamati is a [Raft](https://raft.github.io) implementation in Go and has full support for the  following features:

* Leader Election + Log Replication
* Persistence
* Membership Changes
* Log Compaction

It uses GRPC-compatible [Connect protocol](https://connectrpc.com/docs/introduction/) HTTP2 transport for communicating between peers in the cluster.

### Optimizations
In addition to the above features, the implementation has a few optimizations/enhancements:
* Addresses the _runaway server_ problem due to network partitioning
* Optimized `AppendEntries` conflict resolution

## Usage
