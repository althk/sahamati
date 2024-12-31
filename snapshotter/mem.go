package snapshotter

import pb "github.com/althk/sahamati/proto/v1"

type MemSnapshot struct {
	snap *pb.Snapshot
}

func (m MemSnapshot) Load() (*pb.Snapshot, error) {
	return m.snap, nil
}

func (m MemSnapshot) Save(snapshot *pb.Snapshot) error {
	m.snap = snapshot
	return nil
}

func (m MemSnapshot) HasData() bool {
	return m.snap != nil
}

func NewMemSnapshotter() *MemSnapshot {
	return &MemSnapshot{}
}
