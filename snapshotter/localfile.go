package snapshotter

import (
	"bytes"
	pb "github.com/althk/sahamati/proto/v1"
	"google.golang.org/protobuf/proto"
	"os"
	"sync"
)

type LocalFile struct {
	path string
	mu   sync.Mutex
	size int64
}

func (l *LocalFile) Load() (*pb.Snapshot, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fp, err := os.Open(l.path)
	if err != nil {
		return nil, err
	}
	defer func(fp *os.File) {
		_ = fp.Close()
	}(fp)

	buf := bytes.NewBuffer(nil)
	_, err = buf.ReadFrom(fp)
	if err != nil {
		return nil, err
	}
	snap := &pb.Snapshot{}
	err = proto.Unmarshal(buf.Bytes(), snap)
	if err != nil {
		return nil, err
	}
	return snap, nil
}

func (l *LocalFile) Save(snapshot *pb.Snapshot) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	fp, err := os.Create(l.path)
	if err != nil {
		return err
	}
	defer func(fp *os.File) {
		_ = fp.Close()
	}(fp)
	b, err := proto.Marshal(snapshot)
	if err != nil {
		return err
	}
	_, err = fp.Write(b)
	if err != nil {
		return err
	}
	l.size = int64(len(b))
	return nil
}

func (l *LocalFile) HasData() bool {
	return l.size > 0
}

func NewLocalFile(path string) (*LocalFile, error) {
	fs, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	fi, err := fs.Stat()
	if err != nil {
		return nil, err
	}
	_ = fs.Close()
	return &LocalFile{
		path: path,
		size: fi.Size(),
	}, nil
}
