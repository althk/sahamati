package main

import (
	"encoding/json"
	"errors"
	pb "github.com/althk/sahamati/proto/v1"
	"log/slog"
	"sync"
)

var (
	ErrStoreNotReady = errors.New("store not ready")
)

type KVStore struct {
	store     map[string]string
	respMap   map[uint64]chan struct{}
	proposeCB func(cmd []byte) (uint64, error)
	ready     bool
	mu        sync.Mutex
	logger    *slog.Logger
}

func NewKVStore(logger *slog.Logger) *KVStore {
	return &KVStore{
		store:   make(map[string]string),
		respMap: make(map[uint64]chan struct{}),
		logger:  logger,
	}
}

func (k *KVStore) Put(key, value string) error {
	if !k.ready {
		return ErrStoreNotReady
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	ch, err := k.propose(key, value)
	if err != nil {
		return err
	}
	k.logger.Info("Waiting for consensus")
	<-ch
	k.store[key] = value
	return nil
}

func (k *KVStore) propose(key, value string) (<-chan struct{}, error) {
	e := make(map[string]string)
	e["key"] = key
	e["value"] = value
	b, _ := json.Marshal(e)
	k.logger.Info("Proposing key", key, value)
	id, err := k.proposeCB(b)
	if err != nil {
		return nil, err
	}
	ch := make(chan struct{})
	k.respMap[id] = ch
	return ch, nil
}

func (k *KVStore) Get(key string) (string, bool, error) {
	if !k.ready {
		return "", false, ErrStoreNotReady
	}
	v, ok := k.store[key]
	return v, ok, nil
}

func (k *KVStore) ApplyEntries(entries []*pb.LogEntry) error {
	for _, e := range entries {
		var entry map[string]string
		err := json.Unmarshal(e.Command, &entry)
		if err != nil {
			k.logger.Warn("error unmarshalling entry:", err)
			continue
		}
		k.store[entry["key"]] = entry["value"]
		if ch, ok := k.respMap[e.RealIdx]; ok {
			close(ch)
		}
	}
	return nil
}

func (k *KVStore) CreateSnapshot(_ uint64) ([]byte, error) {
	b, err := json.Marshal(k.store)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (k *KVStore) RestoreFromSnapshot(data []byte) error {
	k.logger.Info("restoring state from snapshot",
		slog.Int("size-MB", len(data)/(1024*1024)))
	err := json.Unmarshal(data, &k.store)
	if err != nil {
		return err
	}
	return nil
}

func (k *KVStore) Start(proposeCB func(cmd []byte) (uint64, error)) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.logger.Info("ready to accept requests")
	k.proposeCB = proposeCB
	k.ready = true
}
