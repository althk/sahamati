package main

import (
	"encoding/json"
	"errors"
	pb "github.com/althk/sahamati/proto/v1"
	"iter"
	"log/slog"
	"sync"
)

var (
	ErrStoreNotReady = errors.New("store not ready")
)

type KVStore struct {
	store     map[string]string
	respMap   map[int64]chan struct{}
	proposeCB func(cmd []byte) (int64, error)
	ready     bool
	mu        sync.RWMutex
	logger    *slog.Logger
}

func NewKVStore(logger *slog.Logger) *KVStore {
	return &KVStore{
		store:   make(map[string]string),
		respMap: make(map[int64]chan struct{}),
		logger:  logger,
	}
}

func (k *KVStore) Put(key, value string) error {
	if !k.ready {
		return ErrStoreNotReady
	}
	k.mu.Lock()
	ch, err := k.propose(key, value)
	k.mu.Unlock()
	if err != nil {
		return err
	}
	k.logger.Info("Waiting for consensus")
	<-ch
	return nil
}

func (k *KVStore) propose(key, value string) (<-chan struct{}, error) {
	e := make(map[string]string)
	e["key"] = key
	e["value"] = value
	b, _ := json.Marshal(e)
	id, err := k.proposeCB(b)
	k.logger.Info("Proposed key", key, value, "realIdx", id)
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
	k.mu.RLock()
	defer k.mu.RUnlock()
	v, ok := k.store[key]
	return v, ok, nil
}

func (k *KVStore) ApplyEntries(entries []*pb.LogEntry) error {
	k.logger.Info("Applying entries", "count", len(entries))
	for _, e := range entries {
		var entry map[string]string
		err := json.Unmarshal(e.Command, &entry)
		if err != nil {
			k.logger.Warn("error unmarshalling entry:", err)
			continue
		}
		k.logger.Info("Applying entry", entry["key"], entry["value"], "realIdx", e.RealIdx)
		k.mu.Lock()
		k.store[entry["key"]] = entry["value"]
		k.mu.Unlock()
		if ch, ok := k.respMap[e.RealIdx]; ok {
			close(ch)
		}
	}
	return nil
}

func (k *KVStore) CreateSnapshot(_ int64) ([]byte, error) {
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

func (k *KVStore) Start(proposeCB func(cmd []byte) (int64, error)) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.logger.Info("ready to accept requests")
	k.proposeCB = proposeCB
	k.ready = true
}

func (k *KVStore) Count() int {
	return len(k.store)
}

func (k *KVStore) Values() iter.Seq[string] {
	return func(yield func(string) bool) {
		for _, v := range k.store {
			if !yield(v) {
				return
			}
		}
	}
}
