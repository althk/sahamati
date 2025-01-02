package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/althk/sahamati/network/server"
	pb "github.com/althk/sahamati/proto/v1"
	"github.com/althk/sahamati/snapshotter"
	"path"
	"strings"
)

var (
	addr     = flag.String("addr", ":6001", "raft node address")
	allNodes = flag.String("nodes", "", `comma separated list of all nodes in this cluster, 
including the current host, in the form 'host1:port1,host2:port2'`)
	h2c           = flag.Bool("no_tls", false, "whether to use HTTP2 WITHOUT TLS (via h2c)")
	join          = flag.Bool("join", false, "join an already running cluster (skip election)")
	walDir        = flag.String("wal_dir", "/tmp", "directory for writing WAL files")
	snapshotDir   = flag.String("snapshot_dir", "/tmp", "directory for snapshot file(s)")
	maxLogEntries = flag.Int("max_log_entries", 100000, "max number of log entries before triggering log compaction")
)

func main() {
	flag.Parse()

	snapper, err := snapshotter.NewLocalFile(path.Join(*snapshotDir, "snapshot.bin"))
	if err != nil {
		panic(err)
	}
	sm := &kvs{m: make(map[string]string)}
	cfg := &server.ClusterConfig{
		ClusterAddrs:  strings.Split(*allNodes, ","),
		Addr:          *addr,
		WALDir:        *walDir,
		JoinCluster:   *join,
		MaxLogEntries: *maxLogEntries,
		Snapper:       snapper,
		SM:            sm,
		H2c:           *h2c,
	}
	srv, err := server.NewRaftHTTP(cfg)
	if err != nil {
		panic(err)
	}
	err = srv.Serve()
	if err != nil {
		panic(err)
	}
}

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
