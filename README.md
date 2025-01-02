# Sahamati (सहमतिः)
Sanskrit for Consensus

## About

Sahamati is a [Raft](https://raft.github.io) implementation in Go and has full support for the  following features:

* Leader Election + Log Replication
* Persistence
* Membership Changes
* Log Compaction

It uses GRPC-compatible [Connect protocol](https://connectrpc.com/docs/introduction/) HTTP2 transport for communicating between peers in the cluster.

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
