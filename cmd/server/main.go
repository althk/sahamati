package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/althk/sahamati/network"
	pb "github.com/althk/sahamati/proto/v1"
	"github.com/althk/sahamati/raft"
	"github.com/althk/sahamati/snapshotter"
	"github.com/althk/wal"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strings"
)

var (
	addr     = flag.String("addr", ":6001", "raft node address")
	allNodes = flag.String("nodes", "", `comma separated list of all nodes in this cluster, 
including the current host, in the form 'host1:port1,host2:port2'`)
	h2c         = flag.Bool("no_tls", false, "whether to use HTTP2 WITHOUT TLS (via h2c)")
	join        = flag.Bool("join", false, "join an already running cluster (skip election)")
	walDir      = flag.String("wal_dir", "/tmp", "directory for writing WAL files")
	snapshotDir = flag.String("snapshot_dir", "/tmp", "directory for snapshot file(s)")
)

func main() {
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	peers := make(map[int]raft.Peer)
	raftID := -1

	for i, peerAddr := range strings.Split(*allNodes, ",") {
		if peerAddr == *addr {
			raftID = i + 1
		}
		peers[i+1] = raft.Peer{
			ID:     i + 1,
			Addr:   peerAddr,
			Client: network.NewCMClient(peerAddr, false),
		}
	}

	snapper, err := snapshotter.NewLocalFile(path.Join(*snapshotDir, "snapshot.bin"))
	if err != nil {
		logger.Error("error initializing snapshotter", err)
		panic(err)
	}
	walPath := path.Join(*walDir, strings.Replace(*addr, ":", "_", -1))
	w, err := wal.New(walPath)
	logger = logger.With(slog.Int("id", raftID))
	if err != nil {
		logger.Error("error initializing WAL", "err", err)
		panic(err)
	}
	cm := raft.NewConsensusModule(
		raftID, peers, logger, snapper,
		w, &kvs{m: make(map[string]string)}, *join,
	)

	httpServer := network.NewHTTPServer(*addr, cm, *h2c, logger)

	ctx, done := signal.NotifyContext(context.Background(), os.Interrupt)
	defer done()

	go func(ctx context.Context) {
		<-ctx.Done()
		logger.Info("shutting down")
		_ = httpServer.Shutdown(ctx)
	}(ctx)

	err = httpServer.ListenAndServeTLS("", "")
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
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
