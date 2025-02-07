package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/althk/sahamati/network/server"
	"github.com/althk/sahamati/snapshotter"
	"golang.org/x/sync/errgroup"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path"
	"strings"
)

var (
	raftAddr = flag.String("raft_addr", ":6001", "raft node address")
	kvsAddr  = flag.String("kvstore_addr", ":8000", "KV Store address")
	allNodes = flag.String("nodes", "", `comma separated list of all nodes in this cluster, 
including the current host, in the form 'host1:port1,host2:port2'`)
	h2c           = flag.Bool("no_tls", false, "whether to use HTTP2 WITHOUT TLS (via h2c)")
	join          = flag.Bool("join", false, "join an already running cluster (skip election)")
	walDir        = flag.String("wal_dir", "/tmp/sahamati", "directory for writing WAL files")
	snapshotDir   = flag.String("snapshot_dir", "/tmp/sahamati", "directory for snapshot file(s)")
	logDir        = flag.String("log_dir", "/tmp/sahamati", "directory for snapshot file(s)")
	maxLogEntries = flag.Int("max_log_entries", 100000, "max number of log entries before triggering log compaction")
)

func main() {
	flag.Parse()
	addrName := strings.Replace(*raftAddr, ":", "_", -1)
	logFile, err := os.OpenFile(path.Join(*logDir, fmt.Sprintf("slog_%s.log", addrName)),
		os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		slog.Error("failed to open log file", "file", addrName)
		panic(err)
	}
	defer func() {
		_ = logFile.Close()
	}()

	logger := slog.New(slog.NewJSONHandler(io.MultiWriter(os.Stdout, logFile), nil))
	//logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	snapper, err := snapshotter.NewLocalFile(
		path.Join(*snapshotDir,
			fmt.Sprintf("snapshot_%s.bin", addrName),
		))
	if err != nil {
		panic(err)
	}
	sm := NewKVStore(logger.With(slog.String("svc", "kvstore")))
	cfg := &server.ClusterConfig{
		ClusterAddrs:  strings.Split(*allNodes, ","),
		Addr:          *raftAddr,
		WALDir:        *walDir,
		JoinCluster:   *join,
		MaxLogEntries: *maxLogEntries,
		Snapper:       snapper,
		SM:            sm,
		H2c:           *h2c,
		Logger:        logger,
	}
	srv, err := server.NewRaftHTTP(cfg)
	if err != nil {
		panic(err)
	}

	kvHttp := NewHTTPServer(*kvsAddr, "/kvs", sm,
		logger.With(slog.String("svc", "kvstore_http")))

	ctx, done := signal.NotifyContext(context.Background(), os.Interrupt)
	defer done()

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return srv.Serve(ctx)
	})
	g.Go(func() error {
		return kvHttp.Serve(ctx)
	})
	if err = g.Wait(); err != nil {
		panic(err)
	}
}
