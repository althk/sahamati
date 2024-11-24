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
	Addr   string // "host:port"
	Client peerClient
}

type peerClient interface {
	RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
}

func getRPCClient(peer Peer) peerClient {
	return peer.Client
}

type ConsensusModule struct {
	id    int
	peers []Peer
	state State

	currentTerm int
	votedFor    int
	log         []pb.LogEntry

	votesReceived   int
	electionTimeout time.Duration
	electionReset   time.Time
	heartbeatTicker *time.Ticker

	mu     sync.RWMutex
	logger *slog.Logger
}

func NewConsensusModule(id int, peers []Peer, logger *slog.Logger) *ConsensusModule {
	return &ConsensusModule{
		id:          id,
		votedFor:    -1,
		log:         make([]pb.LogEntry, 0),
		currentTerm: 0,
		peers:       peers,
		logger:      logger,
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
}

func (c *ConsensusModule) sendHeartbeats() {
	for {
		c.mu.RLock()
		if c.state != Leader {
			c.mu.RUnlock()
			c.heartbeatTicker.Stop()
			return
		}
		c.mu.RUnlock()

		for _, peer := range c.peers {
			go c.sendHeartbeat(peer)
		}
		<-c.heartbeatTicker.C
	}
}

func (c *ConsensusModule) sendHeartbeat(peer Peer) {
	client := getRPCClient(peer)
	c.mu.RLock()
	req := pb.AppendEntriesRequest{
		Term:     int32(c.currentTerm),
		LeaderId: int32(c.id),
		Entries:  make([]*pb.LogEntry, 0),
	}
	c.mu.RUnlock()

	resp, err := client.AppendEntries(context.TODO(), &req)
	if err != nil {
		return
	}
	c.processAppendEntriesResponse(resp)
}

func (c *ConsensusModule) processAppendEntriesResponse(resp *pb.AppendEntriesResponse) {
	c.mu.RLock()

	if resp.Term > int32(c.currentTerm) {
		c.mu.Unlock()
		c.becomeFollower(int(resp.Term))
		return
	}
	if resp.Term == int32(c.currentTerm) && !resp.Success {
		// handle log issue
	}
	c.mu.RUnlock()
}

func (c *ConsensusModule) HandleVoteRequest(_ context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	//c.logger.Info("Received vote request", slog.String("term", fmt.Sprintf("%v", req.Term)))
	resp := pb.RequestVoteResponse{}
	c.mu.RLock()
	resp.Term = int32(c.currentTerm)
	c.mu.RUnlock()
	resp.VoteGranted = false

	if req.Term > resp.Term {
		c.becomeFollower(int(req.Term))
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

	resp.Success = true
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

func electionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(100)) * time.Millisecond
}
