package server

import (
	"connectrpc.com/connect"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"github.com/althk/sahamati/network/client"
	"github.com/althk/sahamati/proto/v1"
	"github.com/althk/sahamati/proto/v1/cmv1connect"
	"github.com/althk/sahamati/raft"
	"github.com/althk/wal"
	"github.com/quic-go/quic-go/http3"
	"log/slog"
	"math/big"
	"net/http"
	"path"
	"strings"
	"time"
)

type ClusterConfig struct {
	ClusterAddrs  []string
	Addr          string
	Snapper       raft.Snapshotter
	WALDir        string
	JoinCluster   bool
	MaxLogEntries int
	SM            raft.StateMachine
	H2c           bool
	Logger        *slog.Logger
}

type RaftHTTP struct {
	httpServer *http3.Server
	cm         consensusModule
	cfg        *ClusterConfig
	nodeID     int
	logger     *slog.Logger
}

type consensusModule interface {
	Init()
	Shutdown()
	HandleVoteRequest(context.Context, *cmv1.RequestVoteRequest) (*cmv1.RequestVoteResponse, error)
	HandleAppendEntriesRequest(context.Context, *cmv1.AppendEntriesRequest) (*cmv1.AppendEntriesResponse, error)
	AddMember(context.Context, *cmv1.AddMemberRequest) (*cmv1.AddMemberResponse, error)
	RemoveMember(context.Context, *cmv1.RemoveMemberRequest) (*cmv1.RemoveMemberResponse, error)
}

func NewRaftHTTP(cfg *ClusterConfig) (*RaftHTTP, error) {
	srv := &RaftHTTP{
		cfg: cfg,
	}
	peers := make(map[int]raft.Peer)

	for i, peerAddr := range cfg.ClusterAddrs {
		if peerAddr == cfg.Addr {
			srv.nodeID = i + 1
		}
		peers[i+1] = raft.Peer{
			ID:     i + 1,
			Addr:   peerAddr,
			Client: client.NewCMClient(peerAddr, false),
		}
	}
	walPath := path.Join(cfg.WALDir, strings.Replace(cfg.Addr, ":", "_", -1))
	w, err := wal.New(walPath)
	srv.logger = cfg.Logger.With(slog.Int("id", srv.nodeID)).With(
		slog.String("svc", "raft-http"))
	if err != nil {
		srv.logger.Error("error initializing WAL", "err", err)
		return nil, err
	}
	srv.cm = raft.NewConsensusModule(
		srv.nodeID, peers, cfg.SM, cfg.Snapper,
		cfg.MaxLogEntries, cfg.JoinCluster,
		w, srv.logger.With(slog.String("svc", "raft-cm")),
	)
	srv.httpServer = newHTTPServer(srv.cfg.Addr, srv.cm, cfg.H2c, srv.logger)
	return srv, nil
}

func localhostTLSConfig() *tls.Config {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
	}
}

func newCMServer(cm consensusModule) *CMServer {
	cm.Init()
	return &CMServer{
		cm: cm,
	}
}

func newHTTPServer(addr string, cm consensusModule, h2c bool, logger *slog.Logger) *http3.Server {
	var cmServer cmv1connect.CMServiceHandler = newCMServer(cm)
	mux := http.NewServeMux()
	p, handler := cmv1connect.NewCMServiceHandler(
		cmServer,
		connect.WithInterceptors(
			LoggingInterceptor(logger),
		),
	)
	mux.Handle(p, handler)

	httpServer := &http3.Server{
		Addr:      addr,
		Handler:   mux,
		TLSConfig: localhostTLSConfig(),
	}

	return httpServer
}

func (srv *RaftHTTP) Serve(ctx context.Context) error {
	shutdownCh := make(chan struct{})
	go func(ctx context.Context) {
		<-ctx.Done()
		srv.logger.Info("shutting down")
		srv.cm.Shutdown()
		tCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()
		if err := srv.httpServer.Shutdown(tCtx); err != nil {
			srv.logger.Error("could not perform graceful shut down, closing active connections", "err", err)
			_ = srv.httpServer.Close()
		}
		close(shutdownCh)
	}(ctx)

	err := srv.httpServer.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	<-shutdownCh
	srv.logger.Info("raft-http shut down complete")
	return nil
}
