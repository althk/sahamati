package main

import (
	"context"
	"errors"
	"flag"
	"github.com/althk/sahamati/network"
	pb "github.com/althk/sahamati/proto/v1"
	"github.com/althk/sahamati/raft"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
)

var (
	addr     = flag.String("addr", ":6001", "raft node address")
	allNodes = flag.String("nodes", "", `comma separated list of all nodes in this cluster, 
including the current host, in the form 'host1:port1,host2:port2'`)
	h2c = flag.Bool("no-tls", false, "whether to use HTTP2 WITHOUT TLS (via h2c)")
)

func main() {
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	peers := make(map[int]raft.Peer)
	raftID := -1

	for i, peerAddr := range strings.Split(*allNodes, ",") {
		if peerAddr == *addr {
			raftID = i + 1
			continue
		}
		peers[i] = raft.Peer{
			ID:     i + 1,
			Addr:   peerAddr,
			Client: network.NewCMClient(peerAddr, false),
		}
	}

	dummyCommitApplier := func(entries []*pb.LogEntry) {}

	cm := raft.NewConsensusModule(
		raftID, peers, logger, dummyCommitApplier,
	)

	httpServer := network.NewHTTPServer(*addr, cm, false, logger)

	ctx, done := signal.NotifyContext(context.Background(), os.Interrupt)
	defer done()

	go func(ctx context.Context) {
		<-ctx.Done()
		logger.Info("shutting down")
		_ = httpServer.Shutdown(ctx)
	}(ctx)

	err := httpServer.ListenAndServeTLS("", "")
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}
