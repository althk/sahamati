package main

import (
	"encoding/json"
	"fmt"
	pb "github.com/althk/sahamati/proto/v1"
)

type kvs struct {
	m map[string]string
}

func (k *kvs) ApplyEntries(entries []*pb.LogEntry) error {
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

func (k *kvs) CreateSnapshot(_ uint64) ([]byte, error) {
	b, err := json.Marshal(k.m)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (k *kvs) RestoreFromSnapshot(data []byte) error {
	err := json.Unmarshal(data, &k.m)
	if err != nil {
		return err
	}
	return nil
}
