package main

import (
	"context"
	"errors"
	"flag"
	"github.com/althk/sahamati/network"
	"github.com/althk/sahamati/raft"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
)

var (
	addr      = flag.String("addr", ":6001", "raft node address")
	peerAddrs = flag.String("peers", "", "comma separated list of peers 'host1:port1,host2:port2'")
	raftID    = flag.Int("raft-id", -1, "ID of raft node, must be unique in a cluster")
	h2c       = flag.Bool("no-tls", false, "whether to use HTTP2 WITHOUT TLS (via h2c)")
)

func main() {
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	if *raftID == -1 {
		log.Fatal("raft-id is required and must be non-negative")
	}
	var peers []raft.Peer

	for _, addr := range strings.Split(*peerAddrs, ",") {
		peers = append(peers, raft.Peer{
			Addr:   addr,
			Client: network.NewCMClient(addr, false),
		})
	}

	cm := raft.NewConsensusModule(
		*raftID, peers, logger,
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
