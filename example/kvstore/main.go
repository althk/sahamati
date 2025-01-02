package main

import (
	"flag"
	"github.com/althk/sahamati/network/server"
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
