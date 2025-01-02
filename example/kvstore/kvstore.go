package main

import (
	"encoding/json"
	"fmt"
	pb "github.com/althk/sahamati/proto/v1"
)

type KVStore struct {
	m map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{make(map[string]string)}
}

func (k *KVStore) Put(key, value string) {
	k.m[key] = value
}

func (k *KVStore) Get(key string) (string, bool) {
	v, ok := k.m[key]
	return v, ok
}

func (k *KVStore) ApplyEntries(entries []*pb.LogEntry) error {
	for _, e := range entries {
		var entry map[string]string
		err := json.Unmarshal(e.Command, &entry)
		if err != nil {
			fmt.Println("error unmarshalling entry:", err)
			continue
		}
		k.m[entry["key"]] = entry["value"]
	}
	return nil
}

func (k *KVStore) CreateSnapshot(_ uint64) ([]byte, error) {
	b, err := json.Marshal(k.m)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (k *KVStore) RestoreFromSnapshot(data []byte) error {
	err := json.Unmarshal(data, &k.m)
	if err != nil {
		return err
	}
	return nil
}
