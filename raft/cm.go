package raft

import (
	"context"
	"fmt"
	pb "github.com/althk/sahamati/proto/v1"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type Peer struct {
	ID     int
	Addr   string // "host:port"
	Client peerClient
}

type peerClient interface {
	RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
}

type commitApplier func(entries []*pb.LogEntry)

func getRPCClient(peer Peer) peerClient {
	return peer.Client
}

type ConsensusModule struct {
	id    int
	peers map[int]Peer
	state State

	currentTerm int
	votedFor    int
	log         []*pb.LogEntry
	realIdx     int64

	votesReceived   int
	electionTimeout time.Duration
	electionReset   time.Time
	heartbeatTicker *time.Ticker

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	aeReadyCh chan bool
	applyCB   commitApplier

	mu     sync.RWMutex
	logger *slog.Logger
}

func NewConsensusModule(id int, peers map[int]Peer, logger *slog.Logger, applyCB commitApplier) *ConsensusModule {
	return &ConsensusModule{
		id:          id,
		votedFor:    -1,
		log:         make([]*pb.LogEntry, 0),
		currentTerm: 0,
		peers:       peers,
		logger:      logger,
		aeReadyCh:   make(chan bool),
		applyCB:     applyCB,
	}
}

func (c *ConsensusModule) Init() {
	// load from snapshot if available here

	c.becomeFollower(0)
	go c.logState()
}

func (c *ConsensusModule) runElectionTimer() {
	t := time.NewTicker(20 * time.Millisecond)

	for {
		<-t.C
		c.mu.RLock()
		if c.state == Leader {
			c.mu.RUnlock()
			t.Stop()
			return
		}
		if elapsed := time.Since(c.electionReset); elapsed >= c.electionTimeout {
			c.mu.RUnlock()
			t.Stop()
			c.startElection()
			return
		}
		c.mu.RUnlock()
	}
}

func (c *ConsensusModule) becomeFollower(term int) {
	c.mu.Lock()
	c.state = Follower
	c.currentTerm = term
	c.votedFor = -1
	c.electionTimeout = electionTimeout()
	c.electionReset = time.Now()
	c.mu.Unlock()
	go c.runElectionTimer()
}

func (c *ConsensusModule) startElection() {
	c.logger.Info("starting election",
		slog.String("term", fmt.Sprintf("%v", c.currentTerm)))
	c.mu.Lock()
	c.state = Candidate
	c.currentTerm += 1
	c.votedFor = c.id
	c.votesReceived = 1
	lastLogIdx, lastLogTerm := c.lastLogIndexAndTerm()
	c.mu.Unlock()

	var wg sync.WaitGroup
	for _, peer := range c.peers {
		wg.Add(1)
		go func() {
			c.sendVoteRequest(peer, c.currentTerm, lastLogIdx, lastLogTerm)
			wg.Done()
		}()
	}
	wg.Wait()
	c.electionReset = time.Now()
	go c.runElectionTimer()
}

func (c *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(c.log) == 0 {
		return -1, 0
	}
	return len(c.log) - 1, int(c.log[len(c.log)-1].Term)
}

func (c *ConsensusModule) sendVoteRequest(peer Peer, term int, idx int, logTerm int) {
	client := getRPCClient(peer)
	c.logger.Info("sending vote request", slog.String("peer", peer.Addr))
	c.mu.RLock()
	if c.state != Candidate || term != c.currentTerm || c.id != c.votedFor {
		c.mu.RUnlock()
		return
	}
	req := pb.RequestVoteRequest{
		Term:        int32(term),
		CandidateId: int32(c.id),
		LastLogIdx:  int32(idx),
		LastLogTerm: int32(logTerm),
	}
	c.mu.RUnlock()
	resp, err := client.RequestVote(context.TODO(), &req)
	if err != nil {
		// handle error
		return
	}
	c.processVoteResponse(term, resp)
}

func (c *ConsensusModule) processVoteResponse(electionTerm int, resp *pb.RequestVoteResponse) {
	c.mu.Lock()
	if c.state != Candidate || electionTerm != c.currentTerm {
		c.mu.Unlock()
		return
	}
	if resp.Term > int32(electionTerm) {
		c.mu.Unlock()
		c.becomeFollower(int(resp.Term))
		return
	}

	if resp.Term == int32(electionTerm) && resp.VoteGranted {
		c.votesReceived += 1
		if c.votesReceived > len(c.peers)/2 {
			c.mu.Unlock()
			go c.becomeLeader()
			return
		}
	}
	c.mu.Unlock()
}

func (c *ConsensusModule) becomeLeader() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logger.Info("Becoming leader",
		slog.String("id", fmt.Sprint(c.id)))
	if c.state == Leader {
		return
	}
	c.state = Leader
	c.heartbeatTicker = time.NewTicker(40 * time.Millisecond)

	go c.sendHeartbeats()
	go c.monitorAEChan()
}

func (c *ConsensusModule) monitorAEChan() {
	for {
		<-c.aeReadyCh
		c.mu.RLock()
		if c.state != Leader {
			c.mu.RUnlock()
			return
		}
		currTerm := c.currentTerm
		c.mu.RUnlock()
		for _, peer := range c.peers {
			go c.sendAppendEntries(peer, currTerm)
		}
	}
}

func (c *ConsensusModule) sendHeartbeats() {
	for {
		c.mu.RLock()
		if c.state != Leader {
			c.mu.RUnlock()
			c.heartbeatTicker.Stop()
			return
		}
		currTerm := c.currentTerm
		c.mu.RUnlock()

		for _, peer := range c.peers {
			go c.sendAppendEntries(peer, currTerm)
		}
		<-c.heartbeatTicker.C
	}
}

func (c *ConsensusModule) sendAppendEntries(peer Peer, savedTerm int) {
	client := getRPCClient(peer)
	c.mu.RLock()
	ni := c.nextIndex[peer.ID]
	prevLogIdx := ni - 1
	prevLogTerm := -1
	if prevLogIdx >= 0 {
		prevLogTerm = int(c.log[prevLogIdx].Term)
	}
	entries := c.log[ni:]
	req := pb.AppendEntriesRequest{
		Term:            int32(savedTerm),
		LeaderId:        int32(c.id),
		Entries:         entries,
		PrevLogTerm:     int32(prevLogTerm),
		PrevLogIdx:      int32(prevLogIdx),
		LeaderCommitIdx: int32(c.commitIndex),
	}
	c.mu.RUnlock()

	resp, err := client.AppendEntries(context.TODO(), &req)
	if err != nil {
		return
	}
	c.processAppendEntriesResponse(&req, resp, peer)
}

func (c *ConsensusModule) processAppendEntriesResponse(req *pb.AppendEntriesRequest, resp *pb.AppendEntriesResponse, peer Peer) {

	if resp.Term > req.Term {
		c.becomeFollower(int(resp.Term))
		return
	}

	c.mu.Lock()
	if c.state == Leader && resp.Term == req.Term {
		if !resp.Success {
			c.nextIndex[peer.ID] = int(req.PrevLogIdx)
			c.mu.Unlock()
			return
		}

		c.nextIndex[peer.ID] += len(req.Entries)
		c.matchIndex[peer.ID] = c.nextIndex[peer.ID] - 1
		c.mu.Unlock()

		c.advanceCommitIndex()
	}
}

func (c *ConsensusModule) HandleVoteRequest(_ context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	//c.logger.Info("Received vote request", slog.String("term", fmt.Sprintf("%v", req.Term)))
	resp := pb.RequestVoteResponse{}
	c.mu.RLock()
	resp.Term = int32(c.currentTerm)
	lastLogIdx, lastLogTerm := c.lastLogIndexAndTerm()
	c.mu.RUnlock()
	resp.VoteGranted = false

	if req.Term > resp.Term {
		c.becomeFollower(int(req.Term))
	}

	// election safety (section 5.4.1)
	if int32(lastLogTerm) > req.LastLogTerm ||
		(int32(lastLogTerm) == req.LastLogTerm && int32(lastLogIdx) > req.LastLogIdx) {
		resp.VoteGranted = false
		return &resp, nil
	}

	c.mu.Lock()
	if req.Term == resp.Term && c.votedFor == -1 || c.votedFor == int(req.CandidateId) {
		c.votedFor = int(req.CandidateId)
		resp.VoteGranted = true
		c.electionReset = time.Now()
		c.logger.Info("Voted for candidate",
			slog.String("candidateId", fmt.Sprint(req.CandidateId)))
	}
	c.mu.Unlock()
	return &resp, nil
}

func (c *ConsensusModule) HandleAppendEntriesRequest(_ context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	resp := pb.AppendEntriesResponse{}

	c.mu.RLock()
	resp.Term = int32(c.currentTerm)
	c.mu.RUnlock()
	if req.Term > resp.Term {
		c.becomeFollower(int(req.Term))
	}

	if req.Term < resp.Term {
		return &resp, nil
	}

	if req.PrevLogIdx >= int32(len(c.log)) || req.PrevLogTerm != c.log[req.PrevLogIdx].Term {
		resp.Success = false
		return &resp, nil
	}

	resp.Success = true

	prevLogIdx := int(req.PrevLogIdx)
	for i, entry := range req.Entries {
		idx := prevLogIdx + i + 1
		if idx >= len(c.log) {
			c.log = append(c.log, entry)
		} else if entry.Term != c.log[idx].Term {
			c.log = c.log[:idx]
			c.log = append(c.log, entry)
		}
	}
	if req.LeaderCommitIdx > int32(c.commitIndex) {
		c.commitIndex = int(min(req.LeaderCommitIdx, int32(len(c.log)-1)))
	}
	c.electionReset = time.Now()
	return &resp, nil
}

func (c *ConsensusModule) logState() {
	t := time.NewTicker(1 * time.Second)
	for {
		<-t.C
		c.mu.RLock()
		c.logger.Info("CM Status",
			slog.String("currentTerm", fmt.Sprintf("%v", c.currentTerm)),
			slog.String("votedFor", fmt.Sprintf("%v", c.votedFor)),
			slog.String("state", fmt.Sprintf("%v", c.state)))
		c.mu.RUnlock()
	}
}

func (c *ConsensusModule) Propose(cmd []byte) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != Leader {
		return -1
	}
	c.realIdx += 1
	c.log = append(c.log, &pb.LogEntry{Term: int32(c.currentTerm), Command: cmd, RealIdx: c.realIdx})
	c.aeReadyCh <- true
	return c.realIdx
}

func (c *ConsensusModule) advanceCommitIndex() {
	c.mu.RLock()
	defer c.mu.RUnlock()
	commitIdx := c.commitIndex
	for i := c.commitIndex + 1; i < len(c.log); i++ {
		count := 1
		for _, peer := range c.peers {
			if c.matchIndex[peer.ID] > i {
				count++
			}
		}

		if count > len(c.peers)/2 {
			c.commitIndex = i
		}
	}
	if commitIdx > c.commitIndex {
		c.applyCommits()
	}
}

func (c *ConsensusModule) applyCommits() {
	var entries []*pb.LogEntry

	c.mu.Lock()
	if c.commitIndex > c.lastApplied {
		entries = c.log[c.lastApplied+1 : c.commitIndex+1]
		c.lastApplied = c.commitIndex
	}
	c.mu.Unlock()

	c.applyCB(entries)
}

func electionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(100)) * time.Millisecond
}
