package network

import (
	"connectrpc.com/connect"
	"context"
	"crypto/tls"
	"github.com/althk/sahamati/proto/v1"
	"github.com/althk/sahamati/proto/v1/cmv1connect"
	"net/http"
	"strings"
)

type CMClient struct {
	Addr   string
	Client cmv1connect.CMServiceClient
}

func (c *CMClient) RequestVote(ctx context.Context, req *cmv1.RequestVoteRequest) (*cmv1.RequestVoteResponse, error) {
	resp, err := c.Client.RequestVote(
		ctx,
		connect.NewRequest(req),
	)
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (c *CMClient) AppendEntries(ctx context.Context, req *cmv1.AppendEntriesRequest) (*cmv1.AppendEntriesResponse, error) {
	resp, err := c.Client.AppendEntries(
		ctx,
		connect.NewRequest(req),
	)
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func NewCMClient(addr string, httpOnly bool) *CMClient {
	baseURL := "https://" + addr
	var httpClient *http.Client

	if httpOnly {
		baseURL = "http://" + addr
		httpClient = http.DefaultClient
	} else {
		skipVerify := strings.HasPrefix(addr, "localhost")
		httpClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: skipVerify,
				},
			},
		}

	}
	return &CMClient{
		Addr: addr,
		Client: cmv1connect.NewCMServiceClient(
			httpClient,
			baseURL,
		),
	}
}
