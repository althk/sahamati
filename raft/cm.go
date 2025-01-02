package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/althk/sahamati/network/client"
	pb "github.com/althk/sahamati/proto/v1"
	"github.com/althk/wal"
	"google.golang.org/protobuf/proto"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

type State int
type ConfigChangeType int

const (
	Follower State = iota
	Candidate
	Leader
	Dead
)

const (
	AddMember ConfigChangeType = iota
	RemoveMember
)

var (
	ErrNodeNotLeader              = errors.New("raft node not leader")
	ErrDuplicateNode              = errors.New("duplicate node ID or address")
	ErrNodeNotFound               = errors.New("node not found in cluster")
	ErrCannotRemove               = errors.New("cannot remove node; cluster is at min size (3)")
	ErrMembershipChangeInProgress = errors.New("another membership change in progress, retry later")
)

type configChange struct {
	Type ConfigChangeType
	ID   int
	Addr string
}

type Peer struct {
	ID     int
	Addr   string // "host:port"
	Client peerClient
}

type peerClient interface {
	RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
}

type StateMachine interface {
	ApplyEntries(entries []*pb.LogEntry) error
	CreateSnapshot(snapshotIndex uint64) ([]byte, error)
	RestoreFromSnapshot(data []byte) error
}

type Snapshotter interface {
	Load() (*pb.Snapshot, error)
	Save(*pb.Snapshot) error
	HasData() bool
}

func getRPCClient(peer Peer) peerClient {
	if peer.Client == nil {
		peer.Client = client.NewCMClient(peer.Addr, false)
	}
	return peer.Client
}

type ConsensusModule struct {
	id int
	// TODO: rename to allNodes or something more appropriate
	peers map[int]Peer
	state State

	currentTerm int
	votedFor    int
	log         []*pb.LogEntry
	realIdx     uint64

	votesReceived   int
	electionTimeout time.Duration
	electionReset   time.Time
	heartbeatTicker *time.Ticker

	commitIndex   uint64
	lastApplied   uint64
	snapshotIndex uint64
	snapshotTerm  int
	nextIndex     map[int]uint64
	matchIndex    map[int]uint64

	aeReadyCh   chan bool
	cfgCommitCh chan configChange // used to notify internally for add/remove node rpc
	doneCh      chan bool

	mu             sync.RWMutex
	peerMu         map[int]*sync.Mutex
	once           sync.Once
	logger         *slog.Logger
	sm             StateMachine
	snapper        Snapshotter
	wal            *wal.WAL
	leaderID       int
	joinCluster    bool
	removingMember bool
	addingMember   bool
	maxLogEntries  int
	currentLogSize int
}

func NewConsensusModule(
	id int, peers map[int]Peer,
	sm StateMachine, snapper Snapshotter,
	maxLogEntries int, join bool, w *wal.WAL,
	logger *slog.Logger) *ConsensusModule {
	return &ConsensusModule{
		id:            id,
		votedFor:      -1,
		realIdx:       0,
		log:           make([]*pb.LogEntry, 0),
		currentTerm:   0,
		peers:         peers,
		logger:        logger,
		wal:           w,
		aeReadyCh:     make(chan bool),
		cfgCommitCh:   make(chan configChange),
		doneCh:        make(chan bool),
		nextIndex:     make(map[int]uint64),
		matchIndex:    make(map[int]uint64),
		snapper:       snapper,
		sm:            sm,
		commitIndex:   0,
		lastApplied:   0,
		joinCluster:   join,
		peerMu:        make(map[int]*sync.Mutex),
		maxLogEntries: max(maxLogEntries, 100000),
	}
}

func (c *ConsensusModule) getPeerMutex(id int) *sync.Mutex {
	c.mu.Lock()
	defer c.mu.Unlock()
	if mu, ok := c.peerMu[id]; ok {
		return mu
	}
	c.peerMu[id] = &sync.Mutex{}
	return c.peerMu[id]
}

func (c *ConsensusModule) Init() {
	if c.snapper.HasData() {
		c.loadFromSnapshot()
	}
	c.restoreFromWAL()
	if !c.joinCluster {
		c.becomeFollower(c.currentTerm)
	}
	go c.logState()
}

func (c *ConsensusModule) runElectionTimer() {
	c.electionTimeout = electionTimeout()
	t := time.NewTicker(10 * time.Millisecond)
	c.mu.Lock()
	termStarted := c.currentTerm
	c.mu.Unlock()
	for {
		select {
		case _, ok := <-c.doneCh:
			if !ok {
				t.Stop()
				return
			}
		case <-t.C:
			c.mu.Lock()
			if c.state == Leader {
				c.mu.Unlock()
				t.Stop()
				return
			}
			if c.currentTerm != termStarted {
				c.mu.Unlock()
				t.Stop()
				return
			}
			if elapsed := time.Since(c.electionReset); elapsed >= c.electionTimeout {
				c.mu.Unlock()
				t.Stop()
				c.startElection()
				return
			}
			c.mu.Unlock()
		}
	}
}

func (c *ConsensusModule) becomeFollower(term int) {
	c.mu.Lock()
	c.state = Follower
	err := c.setCurrentTerm(term)
	if err != nil {
		panic(err)
	}
	err = c.setVotedFor(-1)
	if err != nil {
		panic(err)
	}
	c.electionReset = time.Now()
	c.mu.Unlock()
	go c.runElectionTimer()
}

func (c *ConsensusModule) startElection() {
	c.logger.Debug("starting election",
		slog.String("term", fmt.Sprintf("%v", c.currentTerm)))
	c.mu.Lock()
	c.state = Candidate
	err := c.setCurrentTerm(c.currentTerm + 1)
	if err != nil {
		panic(err)
	}
	err = c.setVotedFor(c.id)
	if err != nil {
		panic(err)
	}
	c.votesReceived = 1
	lastLogIdx, lastLogTerm := c.lastLogIndexAndTerm()
	c.mu.Unlock()

	for _, peer := range c.peers {
		if peer.ID == c.id {
			continue
		}
		go c.sendVoteRequest(peer, c.currentTerm, lastLogIdx, lastLogTerm)
	}
	c.electionReset = time.Now()
	go c.runElectionTimer()
}

func (c *ConsensusModule) lastLogIndexAndTerm() (uint64, int) {
	if c.realIdx == 0 {
		return 0, -1
	}
	return c.realIdx, int(c.log[len(c.log)-1].Term)
}

func (c *ConsensusModule) sendVoteRequest(peer Peer, term int, idx uint64, logTerm int) {
	c.getPeerMutex(peer.ID).Lock()
	defer c.getPeerMutex(peer.ID).Unlock()
	cli := getRPCClient(peer)
	c.logger.Debug("sending vote request", slog.String("peer", peer.Addr))
	c.mu.Lock()
	if c.state != Candidate || term != c.currentTerm || c.id != c.votedFor {
		c.mu.Unlock()
		return
	}
	req := pb.RequestVoteRequest{
		Term:        int32(term),
		CandidateId: int32(c.id),
		LastLogIdx:  idx,
		LastLogTerm: int32(logTerm),
	}
	c.mu.Unlock()
	resp, err := cli.RequestVote(context.TODO(), &req)
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
	c.logger.Info("Becoming leader",
		slog.String("ID", fmt.Sprint(c.id)))
	if c.state == Leader {
		c.mu.Unlock()
		return
	}
	c.state = Leader
	c.leaderID = c.id

	for _, peer := range c.peers {
		if peer.ID == c.id {
			continue
		}
		c.nextIndex[peer.ID] = c.realIdx
		c.matchIndex[peer.ID] = 0
	}

	c.aeReadyCh = make(chan bool)
	c.mu.Unlock()
	go c.sendHeartbeatsAndAEs()
}

func (c *ConsensusModule) sendHeartbeatsAndAEs() {
	c.sendAppendEntries(true)
	c.heartbeatTicker = time.NewTicker(50 * time.Millisecond)

	for {
		select {
		case _, ok := <-c.doneCh:
			if !ok {
				return
			}
		case _, ok := <-c.heartbeatTicker.C:
			if !ok {
				return
			}
			c.sendAppendEntries(true)
		case _, ok := <-c.aeReadyCh:
			if !ok {
				return
			}
			c.sendAppendEntries(false)
		}
	}
}

func (c *ConsensusModule) sendAppendEntries(heartbeat bool) {
	c.mu.Lock()
	if c.state != Leader {
		c.mu.Unlock()
		c.heartbeatTicker.Stop()
		return
	}
	currTerm := c.currentTerm
	c.mu.Unlock()
	for _, peer := range c.peers {
		if peer.ID == c.id {
			continue
		}
		go c.sendAppendEntriesToPeer(peer, currTerm, heartbeat)
	}
}

func (c *ConsensusModule) sendAppendEntriesToPeer(peer Peer, savedTerm int, heartbeat bool) {
	if !heartbeat {
		c.getPeerMutex(peer.ID).Lock()
		defer c.getPeerMutex(peer.ID).Unlock()
	}
	cli := getRPCClient(peer)
	c.mu.Lock()
	prevLogIdx := uint64(0)
	prevLogTerm := -1
	entries := make([]*pb.LogEntry, 0)

	if !heartbeat {
		ni := c.nextIndex[peer.ID]
		if ni <= c.snapshotIndex {
			// TODO: if ni <= c.snapshotIndex, issue InstallSnapshot RPC
			c.mu.Unlock()
			return
		} else {
			prevLogIdx = ni - 1
			if prevLogIdx > 0 {
				prevLogTerm = int(c.log[prevLogIdx-c.snapshotIndex+1].Term)
			}
			entries = c.log[ni-c.snapshotIndex+1:]
		}
	}
	req := pb.AppendEntriesRequest{
		Term:            int32(savedTerm),
		LeaderId:        int32(c.id),
		Entries:         entries,
		PrevLogTerm:     int32(prevLogTerm),
		PrevLogIdx:      prevLogIdx,
		LeaderCommitIdx: c.commitIndex,
	}
	c.mu.Unlock()

	resp, err := cli.AppendEntries(context.TODO(), &req)
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
			c.nextIndex[peer.ID] = req.PrevLogIdx
			c.mu.Unlock()
			return
		}

		if len(req.Entries) > 0 {
			c.nextIndex[peer.ID] += uint64(len(req.Entries))
			c.matchIndex[peer.ID] = max(c.nextIndex[peer.ID]-1, 0)
			c.logger.Info("AppendEntries completed, updating index",
				slog.String("peer", peer.Addr),
				slog.Uint64("nextIndex", c.nextIndex[peer.ID]),
				slog.Uint64("matchIndex", c.matchIndex[peer.ID]),
			)
			c.mu.Unlock()
			c.advanceCommitIndex()
			return
		}
	}
	c.mu.Unlock()
}

func (c *ConsensusModule) HandleVoteRequest(_ context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	c.logger.Debug("Received vote request", slog.String("term", fmt.Sprintf("%v", req.Term)))
	resp := pb.RequestVoteResponse{}
	c.mu.Lock()
	resp.Term = int32(c.currentTerm)
	lastLogIdx, lastLogTerm := c.lastLogIndexAndTerm()
	c.mu.Unlock()
	resp.VoteGranted = false
	if _, ok := c.peers[int(req.CandidateId)]; !ok {
		return &resp, fmt.Errorf("candidate (ID: %v) not part of cluster ", req.CandidateId)
	}

	if req.Term > resp.Term {
		c.becomeFollower(int(req.Term))
	}

	// election safety (section 3.6.1)
	if int32(lastLogTerm) > req.LastLogTerm ||
		(int32(lastLogTerm) == req.LastLogTerm && lastLogIdx > req.LastLogIdx) {
		resp.VoteGranted = false
		return &resp, nil
	}

	c.mu.Lock()
	if req.Term == resp.Term && (c.votedFor == -1 || c.votedFor == int(req.CandidateId) ||
		(c.votedFor == c.id && c.id > int(req.CandidateId))) {
		if c.votedFor == c.id {
			c.votesReceived -= 1
		}
		c.votedFor = int(req.CandidateId)
		resp.VoteGranted = true
		c.state = Follower
		c.electionReset = time.Now()
		c.logger.Info("Voted for candidate",
			slog.String("candidateId", fmt.Sprint(req.CandidateId)))
	}
	c.mu.Unlock()
	return &resp, nil
}

func (c *ConsensusModule) HandleAppendEntriesRequest(_ context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	resp := pb.AppendEntriesResponse{}

	c.mu.Lock()
	resp.Term = int32(c.currentTerm)
	c.electionReset = time.Now()

	c.mu.Unlock()
	if req.Term > resp.Term {
		c.becomeFollower(int(req.Term))
	}

	if req.Term < resp.Term {
		return &resp, nil
	}

	c.mu.Lock()
	c.state = Follower
	c.mu.Unlock()

	if req.PrevLogIdx > 0 &&
		(req.PrevLogIdx >= c.realIdx || req.PrevLogTerm != c.log[req.PrevLogIdx-c.snapshotIndex+1].Term) {
		resp.Success = false
		return &resp, nil
	}

	resp.Success = true

	if len(req.Entries) == 0 {
		// heartbeat
		c.mu.Lock()
		c.leaderID = int(req.LeaderId)
		c.mu.Unlock()
		return &resp, nil
	}

	prevLogIdx := req.PrevLogIdx

	c.logger.Info("Appending new entries",
		slog.String("entries", fmt.Sprintf("%v", req.Entries)))

	c.mu.Lock()
	for i, entry := range req.Entries {
		idx := prevLogIdx + uint64(i) + 1
		if idx >= c.realIdx {
			if err := c.appendEntry(entry); err != nil {
				c.mu.Unlock()
				resp.Success = false
				return &resp, err
			}
		} else if entry.Term != c.log[idx-c.snapshotIndex+1].Term {
			c.log = c.log[:idx-c.snapshotIndex+1]
			if err := c.appendEntry(entry); err != nil {
				c.mu.Unlock()
				resp.Success = false
				return &resp, err
			}
		}
		var cfg configChange
		if err := json.Unmarshal(entry.Command, &cfg); err == nil {
			c.applyConfigChange(cfg)
		}
	}
	c.realIdx = c.log[len(c.log)-1].RealIdx
	mustApply := false
	if req.LeaderCommitIdx > c.commitIndex {
		if err := c.setCommitIndex(min(req.LeaderCommitIdx, c.realIdx)); err != nil {
			c.commitIndex = min(req.LeaderCommitIdx, c.realIdx)
		}
		mustApply = true
	}
	c.leaderID = int(req.LeaderId)
	c.mu.Unlock()
	if mustApply {
		c.applyCommits()
	}
	return &resp, nil
}

func (c *ConsensusModule) logState() {
	t := time.NewTicker(8 * time.Second)
	for {
		select {
		case _, ok := <-c.doneCh:
			if !ok {
				t.Stop()
				return
			}
		case <-t.C:
			c.mu.Lock()
			c.logger.Info("CM Status",
				slog.String("currentTerm", fmt.Sprintf("%v", c.currentTerm)),
				slog.String("votedFor", fmt.Sprintf("%v", c.votedFor)),
				slog.String("state", fmt.Sprintf("%v", c.state)),
				slog.String("raft-id", fmt.Sprintf("%v", c.id)),
				slog.String("leader-id", fmt.Sprintf("%v", c.leaderID)),
				slog.String("commit-idx", fmt.Sprintf("%v", c.commitIndex)),
				slog.String("last-applied", fmt.Sprintf("%v", c.lastApplied)),
			)
			c.mu.Unlock()
		}
	}
}

func (c *ConsensusModule) cmState() (id int, currentTerm int, votedFor int,
	commitIndex uint64, lastApplied uint64, state State) {
	return c.id, c.currentTerm, c.votedFor, c.commitIndex, c.lastApplied, c.state
}

func (c *ConsensusModule) Propose(cmd []byte) uint64 {
	c.mu.Lock()
	if c.state != Leader {
		c.mu.Unlock()
		return 0
	}
	c.realIdx++
	entry := &pb.LogEntry{
		Command: cmd,
		Term:    int32(c.currentTerm),
		RealIdx: c.realIdx,
	}
	err := c.appendEntry(entry)
	if err != nil {
		c.realIdx--
		c.mu.Unlock()
		return 0
	}

	c.mu.Unlock()
	c.aeReadyCh <- true
	return c.realIdx
}

func (c *ConsensusModule) advanceCommitIndex() {
	c.mu.Lock()
	commitIdx := c.commitIndex
	c.mu.Unlock()
	for i := commitIdx + 1; i < c.realIdx; i++ {
		if c.log[i-c.snapshotIndex+1].Term == int32(c.currentTerm) {
			count := 1
			for _, peer := range c.peers {
				if peer.ID == c.id {
					continue
				}
				if c.matchIndex[peer.ID] >= i {
					count++
				}
			}
			if count > len(c.peers)/2 {
				c.mu.Lock()
				c.commitIndex = i
				c.mu.Unlock()
			}
		}
	}
	if commitIdx < c.commitIndex {
		c.applyCommits()
		c.aeReadyCh <- true
	}
}

func (c *ConsensusModule) applyCommits() {
	var entries []*pb.LogEntry
	c.logger.Info("applying commits",
		slog.Uint64("commitidx", c.commitIndex),
		slog.Uint64("lastapplied", c.lastApplied))
	c.mu.Lock()
	for c.lastApplied < c.commitIndex {
		c.lastApplied++
		entry := c.log[c.lastApplied-c.snapshotIndex+1]
		var cfg configChange
		if err := json.Unmarshal(entry.Command, &cfg); err == nil {
			if entry.Term == int32(c.currentTerm) && c.state == Leader {
				c.applyConfigChange(cfg)
				c.cfgCommitCh <- cfg
			}
		} else {
			entries = append(entries, entry)
		}
	}
	c.mu.Unlock()

	err := c.sm.ApplyEntries(entries)
	if err != nil {
		c.logger.Error("FAILED: applying commits",
			slog.Uint64("commitidx", c.commitIndex),
			slog.Uint64("lastapplied", c.lastApplied))
	}
}

func (c *ConsensusModule) appendEntry(entry *pb.LogEntry) error {
	b, err := proto.Marshal(entry)
	if err != nil {
		return err
	}
	batch := make(map[string][]byte)
	batch[fmt.Sprintf("log:%d", entry.RealIdx)] = b
	if realIdxBytes, err := json.Marshal(c.realIdx); err == nil {
		batch["realIdx"] = realIdxBytes
	}
	err = c.wal.PutBatch(batch)
	if err != nil {
		return err
	}
	c.log = append(c.log, entry)
	c.currentLogSize++
	if c.currentLogSize >= c.maxLogEntries {
		go c.compactLog()
	}
	return nil
}

func (c *ConsensusModule) setCurrentTerm(term int) error {
	err := c.appendToWAL("term", term)
	if err != nil {
		return err
	}
	c.currentTerm = term
	return nil
}

func (c *ConsensusModule) setVotedFor(votedFor int) error {
	err := c.appendToWAL("votedFor", votedFor)
	if err != nil {
		return err
	}
	c.votedFor = votedFor
	return nil
}

func (c *ConsensusModule) setCommitIndex(commitIndex uint64) error {
	err := c.appendToWAL("commitIndex", commitIndex)
	if err != nil {
		return err
	}
	c.commitIndex = commitIndex
	return nil
}

func (c *ConsensusModule) appendToWAL(key string, val any) error {
	b, err := json.Marshal(val)
	if err != nil {
		return err
	}
	err = c.wal.Put(key, b)
	if err != nil {
		return err
	}
	return nil
}

func (c *ConsensusModule) applyConfigChange(cfg configChange) {
	switch cfg.Type {
	case AddMember:
		c.peers[cfg.ID] = Peer{ID: cfg.ID, Addr: cfg.Addr}
	case RemoveMember:
		delete(c.peers, cfg.ID)
		if cfg.ID == c.id {
			c.Shutdown()
		}
	}
}

func (c *ConsensusModule) Shutdown() {
	c.state = Dead
	c.once.Do(func() {
		close(c.doneCh)
		close(c.aeReadyCh)
	})
}

func (c *ConsensusModule) loadFromSnapshot() {
	if !c.snapper.HasData() {
		return
	}
	s, err := c.snapper.Load()
	if err != nil {
		panic(err)
	}
	err = c.restoreFromSnapshot(s)
	if err != nil {
		panic(err)
	}
}

func (c *ConsensusModule) restoreFromSnapshot(s *pb.Snapshot) error {
	err := c.sm.RestoreFromSnapshot(s.Data)
	if err != nil {
		return err
	}
	c.lastApplied = s.Metadata.Index
	c.snapshotIndex = s.Metadata.Index
	c.snapshotTerm = int(s.Metadata.Term)
	return nil
}

func (c *ConsensusModule) createSnapshot() {
	c.mu.Lock()
	lastApplied := c.lastApplied
	snapshotIndex := c.snapshotIndex
	snapTerm := c.log[c.lastApplied-snapshotIndex+1].Term
	c.mu.Unlock()
	snap := &pb.Snapshot{
		Metadata: &pb.Snapshot_Metadata{
			Term:  snapTerm,
			Index: c.lastApplied,
		},
	}
	var err error
	snap.Data, err = c.sm.CreateSnapshot(lastApplied)
	if err != nil {
		c.logger.Error("FAILED: state machine snapshot creation",
			slog.Uint64("lastApplied", c.lastApplied),
			slog.String("error", err.Error()))
		return
	}
	if err = c.snapper.Save(snap); err != nil {
		c.logger.Error("FAILED: could not persist snapshot",
			slog.String("error", err.Error()))
		return
	}
	var k string
	go func() {
		for k = range c.wal.EntriesBetween(fmt.Sprintf("log:%d", snapshotIndex),
			fmt.Sprintf("log:%d", lastApplied)) {
			// TODO: add batch delete to WAL
			if err := c.wal.Delete(k); err != nil {
				c.logger.Warn("WARN: could not delete key from WAL during compaction",
					slog.String("error", err.Error()),
					slog.String("key", k),
				)
			}
		}
	}()
	c.mu.Lock()
	c.log = c.log[lastApplied-snapshotIndex+1:]
	c.snapshotIndex = lastApplied
	c.snapshotTerm = int(snapTerm)
	c.currentLogSize = len(c.log)
	c.mu.Unlock()
}

func (c *ConsensusModule) AddMember(ctx context.Context, req *pb.AddMemberRequest) (*pb.AddMemberResponse, error) {
	resp := &pb.AddMemberResponse{}

	if c.isMembershipChangeInProgress() {
		resp.Status = false
		return resp, ErrMembershipChangeInProgress
	}
	c.logger.Info(
		"AddMember RPC",
		slog.String("req", fmt.Sprintf("%v", req)),
	)
	c.mu.Lock()

	c.addingMember = true
	defer func() { c.addingMember = false }()

	if c.state != Leader {
		resp.Status = false
		resp.LeaderHint = c.peers[c.leaderID].Addr
		c.mu.Unlock()
		return resp, ErrNodeNotLeader
	}

	for _, peer := range c.peers {
		if peer.Addr == req.Addr || peer.ID == int(req.NodeId) {
			resp.Status = false
			c.mu.Unlock()
			return resp, ErrDuplicateNode
		}
	}

	cfg := configChange{
		Type: AddMember,
		ID:   int(req.NodeId),
		Addr: req.Addr,
	}

	cmd, err := json.Marshal(cfg)
	if err != nil {
		resp.Status = false
		c.mu.Unlock()
		return resp, err
	}

	c.nextIndex[int(req.NodeId)] = 1
	c.matchIndex[int(req.NodeId)] = 0
	c.mu.Unlock()
	idx := c.Propose(cmd)
	c.logger.Info("AddMember RPC proposed log index",
		slog.String("req", fmt.Sprintf("%v", req)),
		slog.Uint64("idx", idx),
	)

	select {
	case <-ctx.Done():
		return resp, ctx.Err()
	case cfg := <-c.cfgCommitCh:
		if cfg.ID == int(req.NodeId) {
			resp.Status = true
		}
	}
	return resp, nil
}

func (c *ConsensusModule) RemoveMember(ctx context.Context, req *pb.RemoveMemberRequest) (*pb.RemoveMemberResponse, error) {
	resp := &pb.RemoveMemberResponse{}

	if c.isMembershipChangeInProgress() {
		resp.Status = false
		return resp, ErrMembershipChangeInProgress
	}

	c.mu.Lock()

	c.removingMember = true
	defer func() { c.removingMember = false }()

	c.logger.Info("RemoveMember RPC",
		slog.String("req", fmt.Sprintf("%v", req)))

	if c.state != Leader {
		resp.Status = false
		resp.LeaderHint = c.peers[c.leaderID].Addr
		c.mu.Unlock()
		return resp, ErrNodeNotLeader
	}

	if len(c.peers) == 3 {
		resp.Status = false
		c.mu.Unlock()
		return resp, ErrCannotRemove
	}

	found := false
	for _, peer := range c.peers {
		if int32(peer.ID) == req.NodeId && peer.Addr == req.Addr {
			found = true
			break
		}
	}
	if !found {
		resp.Status = false
		c.mu.Unlock()
		return resp, ErrNodeNotFound
	}
	cfg := configChange{
		Type: RemoveMember,
		ID:   int(req.NodeId),
		Addr: req.Addr,
	}
	cmd, err := json.Marshal(cfg)
	if err != nil {
		resp.Status = false
		c.mu.Unlock()
		return resp, err
	}
	c.mu.Unlock()
	idx := c.Propose(cmd)

	c.logger.Info("RemoveMember RPC proposed log index",
		slog.String("node-id", fmt.Sprintf("%v", req.NodeId)),
		slog.Uint64("idx", idx),
	)

	select {
	case <-ctx.Done():
		return resp, ctx.Err()
	case cfg := <-c.cfgCommitCh:
		if cfg.ID == int(req.NodeId) {
			delete(c.nextIndex, int(req.NodeId))
			delete(c.matchIndex, int(req.NodeId))
			resp.Status = true
		}
	}
	return resp, nil
}

func (c *ConsensusModule) isMembershipChangeInProgress() bool {
	return c.removingMember || c.addingMember
}

func (c *ConsensusModule) restoreFromWAL() {
	if err := c.loadFromWAL("realIdx", &c.realIdx); err != nil && !errors.Is(err, wal.ErrKeyNotFound) {
		panic(err)
	}
	if err := c.loadFromWAL("commitIndex", &c.commitIndex); err != nil && !errors.Is(err, wal.ErrKeyNotFound) {
		panic(err)
	}
	if err := c.loadFromWAL("term", &c.currentTerm); err != nil && !errors.Is(err, wal.ErrKeyNotFound) {
		panic(err)
	}
	if err := c.loadFromWAL("votedFor", &c.votedFor); err != nil && !errors.Is(err, wal.ErrKeyNotFound) {
		panic(err)
	}
	if c.realIdx < 1 {
		return
	}
	for _, v := range c.wal.EntriesBetween(fmt.Sprintf("log:%d", c.lastApplied),
		fmt.Sprintf("log:%d", c.realIdx+1)) {
		var entry *pb.LogEntry
		err := proto.Unmarshal(v, entry)
		if err != nil {
			panic(err)
		}
		c.log = append(c.log, entry)
	}
}

func (c *ConsensusModule) loadFromWAL(key string, prop any) error {
	b, err := c.wal.Get(key)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, prop); err != nil {
		return err
	}
	return nil
}

func (c *ConsensusModule) compactLog() {
	c.createSnapshot()
}

func electionTimeout() time.Duration {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Duration(150+rng.Intn(151)) * time.Millisecond
}
