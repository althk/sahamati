package server

import (
	"connectrpc.com/connect"
	"context"
	cmv1 "github.com/althk/sahamati/proto/v1"
)

type CMServer struct {
	cm consensusModule
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

func (c *CMServer) AddMember(ctx context.Context, c2 *connect.Request[cmv1.AddMemberRequest]) (*connect.Response[cmv1.AddMemberResponse], error) {
	resp, err := c.cm.AddMember(ctx, c2.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse[cmv1.AddMemberResponse](resp), nil
}

func (c *CMServer) RemoveMember(ctx context.Context, c2 *connect.Request[cmv1.RemoveMemberRequest]) (*connect.Response[cmv1.RemoveMemberResponse], error) {
	resp, err := c.cm.RemoveMember(ctx, c2.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse[cmv1.RemoveMemberResponse](resp), nil
}
