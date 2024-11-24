package network

import (
	connect "connectrpc.com/connect"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"github.com/althk/sahamati/proto/v1"
	"github.com/althk/sahamati/proto/v1/cmv1connect"
	"log/slog"
	"math/big"
	"net/http"
)

type CMServer struct {
	cm consensusModule
}

type consensusModule interface {
	Init()
	HandleVoteRequest(context.Context, *cmv1.RequestVoteRequest) (*cmv1.RequestVoteResponse, error)
	HandleAppendEntriesRequest(context.Context, *cmv1.AppendEntriesRequest) (*cmv1.AppendEntriesResponse, error)
}

func NewCMServer(cm consensusModule) *CMServer {
	cm.Init()
	return &CMServer{
		cm: cm,
	}
}

func (c *CMServer) RequestVote(ctx context.Context, r *connect.Request[cmv1.RequestVoteRequest]) (*connect.Response[cmv1.RequestVoteResponse], error) {
	resp, err := c.cm.HandleVoteRequest(ctx, r.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse[cmv1.RequestVoteResponse](resp), nil
}

func (c *CMServer) AppendEntries(ctx context.Context, r *connect.Request[cmv1.AppendEntriesRequest]) (*connect.Response[cmv1.AppendEntriesResponse], error) {
	resp, err := c.cm.HandleAppendEntriesRequest(ctx, r.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse[cmv1.AppendEntriesResponse](resp), nil
}

func LocalhostTLSConfig() *tls.Config {
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

func NewHTTPServer(addr string, cm consensusModule, h2c bool, logger *slog.Logger) *http.Server {
	var cmServer cmv1connect.CMServiceHandler = NewCMServer(cm)
	mux := http.NewServeMux()
	path, handler := cmv1connect.NewCMServiceHandler(
		cmServer,
		connect.WithInterceptors(
			LoggingInterceptor(logger),
		),
	)
	mux.Handle(path, handler)

	httpServer := &http.Server{
		Addr:      addr,
		Handler:   mux,
		TLSConfig: LocalhostTLSConfig(),
	}
	return httpServer
}
