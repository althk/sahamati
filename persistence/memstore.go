package persistence

import pb "github.com/althk/sahamati/proto/v1"

type MemStore struct {
	state *pb.PersistentState
}

func (m *MemStore) Load() (*pb.PersistentState, error) {
	return m.state, nil
}

func (m *MemStore) Save(state *pb.PersistentState) error {
	m.state = state
	return nil
}

func (m *MemStore) HasData() bool {
	if m.state == nil {
		return false
	}
	return true
}

func NewMemStore() *MemStore {
	return &MemStore{}
}
