// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.35.2
// 	protoc        (unknown)
// source: proto/v1/cm.proto

package cmv1

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type LogEntry struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term    int32  `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
	Command []byte `protobuf:"bytes,2,opt,name=command,proto3" json:"command,omitempty"`
	RealIdx int64  `protobuf:"varint,3,opt,name=real_idx,json=realIdx,proto3" json:"real_idx,omitempty"`
}

func (x *LogEntry) Reset() {
	*x = LogEntry{}
	mi := &file_proto_v1_cm_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LogEntry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LogEntry) ProtoMessage() {}

func (x *LogEntry) ProtoReflect() protoreflect.Message {
	mi := &file_proto_v1_cm_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LogEntry.ProtoReflect.Descriptor instead.
func (*LogEntry) Descriptor() ([]byte, []int) {
	return file_proto_v1_cm_proto_rawDescGZIP(), []int{0}
}

func (x *LogEntry) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *LogEntry) GetCommand() []byte {
	if x != nil {
		return x.Command
	}
	return nil
}

func (x *LogEntry) GetRealIdx() int64 {
	if x != nil {
		return x.RealIdx
	}
	return 0
}

type PersistentState struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term     int32       `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
	VotedFor int32       `protobuf:"varint,2,opt,name=voted_for,json=votedFor,proto3" json:"voted_for,omitempty"`
	Log      []*LogEntry `protobuf:"bytes,3,rep,name=log,proto3" json:"log,omitempty"`
}

func (x *PersistentState) Reset() {
	*x = PersistentState{}
	mi := &file_proto_v1_cm_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PersistentState) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PersistentState) ProtoMessage() {}

func (x *PersistentState) ProtoReflect() protoreflect.Message {
	mi := &file_proto_v1_cm_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PersistentState.ProtoReflect.Descriptor instead.
func (*PersistentState) Descriptor() ([]byte, []int) {
	return file_proto_v1_cm_proto_rawDescGZIP(), []int{1}
}

func (x *PersistentState) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *PersistentState) GetVotedFor() int32 {
	if x != nil {
		return x.VotedFor
	}
	return 0
}

func (x *PersistentState) GetLog() []*LogEntry {
	if x != nil {
		return x.Log
	}
	return nil
}

type RequestVoteRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term        int32 `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
	CandidateId int32 `protobuf:"varint,2,opt,name=candidate_id,json=candidateId,proto3" json:"candidate_id,omitempty"`
	LastLogIdx  int32 `protobuf:"varint,3,opt,name=last_log_idx,json=lastLogIdx,proto3" json:"last_log_idx,omitempty"`
	LastLogTerm int32 `protobuf:"varint,4,opt,name=last_log_term,json=lastLogTerm,proto3" json:"last_log_term,omitempty"`
}

func (x *RequestVoteRequest) Reset() {
	*x = RequestVoteRequest{}
	mi := &file_proto_v1_cm_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RequestVoteRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RequestVoteRequest) ProtoMessage() {}

func (x *RequestVoteRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_v1_cm_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RequestVoteRequest.ProtoReflect.Descriptor instead.
func (*RequestVoteRequest) Descriptor() ([]byte, []int) {
	return file_proto_v1_cm_proto_rawDescGZIP(), []int{2}
}

func (x *RequestVoteRequest) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *RequestVoteRequest) GetCandidateId() int32 {
	if x != nil {
		return x.CandidateId
	}
	return 0
}

func (x *RequestVoteRequest) GetLastLogIdx() int32 {
	if x != nil {
		return x.LastLogIdx
	}
	return 0
}

func (x *RequestVoteRequest) GetLastLogTerm() int32 {
	if x != nil {
		return x.LastLogTerm
	}
	return 0
}

type RequestVoteResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term        int32 `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
	VoteGranted bool  `protobuf:"varint,2,opt,name=vote_granted,json=voteGranted,proto3" json:"vote_granted,omitempty"`
}

func (x *RequestVoteResponse) Reset() {
	*x = RequestVoteResponse{}
	mi := &file_proto_v1_cm_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RequestVoteResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RequestVoteResponse) ProtoMessage() {}

func (x *RequestVoteResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_v1_cm_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RequestVoteResponse.ProtoReflect.Descriptor instead.
func (*RequestVoteResponse) Descriptor() ([]byte, []int) {
	return file_proto_v1_cm_proto_rawDescGZIP(), []int{3}
}

func (x *RequestVoteResponse) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *RequestVoteResponse) GetVoteGranted() bool {
	if x != nil {
		return x.VoteGranted
	}
	return false
}

type AppendEntriesRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term            int32       `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
	LeaderId        int32       `protobuf:"varint,2,opt,name=leader_id,json=leaderId,proto3" json:"leader_id,omitempty"`
	PrevLogIdx      int32       `protobuf:"varint,3,opt,name=prev_log_idx,json=prevLogIdx,proto3" json:"prev_log_idx,omitempty"`
	PrevLogTerm     int32       `protobuf:"varint,4,opt,name=prev_log_term,json=prevLogTerm,proto3" json:"prev_log_term,omitempty"`
	LeaderCommitIdx int32       `protobuf:"varint,5,opt,name=leader_commit_idx,json=leaderCommitIdx,proto3" json:"leader_commit_idx,omitempty"`
	Entries         []*LogEntry `protobuf:"bytes,6,rep,name=entries,proto3" json:"entries,omitempty"`
}

func (x *AppendEntriesRequest) Reset() {
	*x = AppendEntriesRequest{}
	mi := &file_proto_v1_cm_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *AppendEntriesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AppendEntriesRequest) ProtoMessage() {}

func (x *AppendEntriesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_v1_cm_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AppendEntriesRequest.ProtoReflect.Descriptor instead.
func (*AppendEntriesRequest) Descriptor() ([]byte, []int) {
	return file_proto_v1_cm_proto_rawDescGZIP(), []int{4}
}

func (x *AppendEntriesRequest) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *AppendEntriesRequest) GetLeaderId() int32 {
	if x != nil {
		return x.LeaderId
	}
	return 0
}

func (x *AppendEntriesRequest) GetPrevLogIdx() int32 {
	if x != nil {
		return x.PrevLogIdx
	}
	return 0
}

func (x *AppendEntriesRequest) GetPrevLogTerm() int32 {
	if x != nil {
		return x.PrevLogTerm
	}
	return 0
}

func (x *AppendEntriesRequest) GetLeaderCommitIdx() int32 {
	if x != nil {
		return x.LeaderCommitIdx
	}
	return 0
}

func (x *AppendEntriesRequest) GetEntries() []*LogEntry {
	if x != nil {
		return x.Entries
	}
	return nil
}

type AppendEntriesResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term    int32 `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
	Success bool  `protobuf:"varint,2,opt,name=success,proto3" json:"success,omitempty"`
}

func (x *AppendEntriesResponse) Reset() {
	*x = AppendEntriesResponse{}
	mi := &file_proto_v1_cm_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *AppendEntriesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AppendEntriesResponse) ProtoMessage() {}

func (x *AppendEntriesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_v1_cm_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AppendEntriesResponse.ProtoReflect.Descriptor instead.
func (*AppendEntriesResponse) Descriptor() ([]byte, []int) {
	return file_proto_v1_cm_proto_rawDescGZIP(), []int{5}
}

func (x *AppendEntriesResponse) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *AppendEntriesResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

var File_proto_v1_cm_proto protoreflect.FileDescriptor

var file_proto_v1_cm_proto_rawDesc = []byte{
	0x0a, 0x11, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x76, 0x31, 0x2f, 0x63, 0x6d, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x08, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x76, 0x31, 0x22, 0x53, 0x0a,
	0x08, 0x4c, 0x6f, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x65, 0x72,
	0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x12, 0x18, 0x0a,
	0x07, 0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x07,
	0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x12, 0x19, 0x0a, 0x08, 0x72, 0x65, 0x61, 0x6c, 0x5f,
	0x69, 0x64, 0x78, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x07, 0x72, 0x65, 0x61, 0x6c, 0x49,
	0x64, 0x78, 0x22, 0x68, 0x0a, 0x0f, 0x50, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x74,
	0x53, 0x74, 0x61, 0x74, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x05, 0x52, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x12, 0x1b, 0x0a, 0x09, 0x76, 0x6f, 0x74,
	0x65, 0x64, 0x5f, 0x66, 0x6f, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x08, 0x76, 0x6f,
	0x74, 0x65, 0x64, 0x46, 0x6f, 0x72, 0x12, 0x24, 0x0a, 0x03, 0x6c, 0x6f, 0x67, 0x18, 0x03, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x76, 0x31, 0x2e, 0x4c,
	0x6f, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x03, 0x6c, 0x6f, 0x67, 0x22, 0x91, 0x01, 0x0a,
	0x12, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x05, 0x52, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x12, 0x21, 0x0a, 0x0c, 0x63, 0x61, 0x6e, 0x64, 0x69,
	0x64, 0x61, 0x74, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0b, 0x63,
	0x61, 0x6e, 0x64, 0x69, 0x64, 0x61, 0x74, 0x65, 0x49, 0x64, 0x12, 0x20, 0x0a, 0x0c, 0x6c, 0x61,
	0x73, 0x74, 0x5f, 0x6c, 0x6f, 0x67, 0x5f, 0x69, 0x64, 0x78, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05,
	0x52, 0x0a, 0x6c, 0x61, 0x73, 0x74, 0x4c, 0x6f, 0x67, 0x49, 0x64, 0x78, 0x12, 0x22, 0x0a, 0x0d,
	0x6c, 0x61, 0x73, 0x74, 0x5f, 0x6c, 0x6f, 0x67, 0x5f, 0x74, 0x65, 0x72, 0x6d, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x05, 0x52, 0x0b, 0x6c, 0x61, 0x73, 0x74, 0x4c, 0x6f, 0x67, 0x54, 0x65, 0x72, 0x6d,
	0x22, 0x4c, 0x0a, 0x13, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x12, 0x21, 0x0a, 0x0c, 0x76,
	0x6f, 0x74, 0x65, 0x5f, 0x67, 0x72, 0x61, 0x6e, 0x74, 0x65, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x08, 0x52, 0x0b, 0x76, 0x6f, 0x74, 0x65, 0x47, 0x72, 0x61, 0x6e, 0x74, 0x65, 0x64, 0x22, 0xe7,
	0x01, 0x0a, 0x14, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x12, 0x1b, 0x0a, 0x09, 0x6c,
	0x65, 0x61, 0x64, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x08,
	0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x49, 0x64, 0x12, 0x20, 0x0a, 0x0c, 0x70, 0x72, 0x65, 0x76,
	0x5f, 0x6c, 0x6f, 0x67, 0x5f, 0x69, 0x64, 0x78, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0a,
	0x70, 0x72, 0x65, 0x76, 0x4c, 0x6f, 0x67, 0x49, 0x64, 0x78, 0x12, 0x22, 0x0a, 0x0d, 0x70, 0x72,
	0x65, 0x76, 0x5f, 0x6c, 0x6f, 0x67, 0x5f, 0x74, 0x65, 0x72, 0x6d, 0x18, 0x04, 0x20, 0x01, 0x28,
	0x05, 0x52, 0x0b, 0x70, 0x72, 0x65, 0x76, 0x4c, 0x6f, 0x67, 0x54, 0x65, 0x72, 0x6d, 0x12, 0x2a,
	0x0a, 0x11, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x5f,
	0x69, 0x64, 0x78, 0x18, 0x05, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0f, 0x6c, 0x65, 0x61, 0x64, 0x65,
	0x72, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x49, 0x64, 0x78, 0x12, 0x2c, 0x0a, 0x07, 0x65, 0x6e,
	0x74, 0x72, 0x69, 0x65, 0x73, 0x18, 0x06, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x2e, 0x76, 0x31, 0x2e, 0x4c, 0x6f, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52,
	0x07, 0x65, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x22, 0x45, 0x0a, 0x15, 0x41, 0x70, 0x70, 0x65,
	0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52,
	0x04, 0x74, 0x65, 0x72, 0x6d, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x32,
	0xad, 0x01, 0x0a, 0x09, 0x43, 0x4d, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x4c, 0x0a,
	0x0b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65, 0x12, 0x1c, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x76, 0x31, 0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56,
	0x6f, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1d, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x2e, 0x76, 0x31, 0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74,
	0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x52, 0x0a, 0x0d, 0x41,
	0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x12, 0x1e, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x76, 0x31, 0x2e, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e,
	0x74, 0x72, 0x69, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1f, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x76, 0x31, 0x2e, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e,
	0x74, 0x72, 0x69, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42,
	0x29, 0x5a, 0x27, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x61, 0x6c,
	0x74, 0x68, 0x6b, 0x2f, 0x73, 0x61, 0x68, 0x61, 0x6d, 0x61, 0x74, 0x69, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x2f, 0x76, 0x31, 0x3b, 0x63, 0x6d, 0x76, 0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var (
	file_proto_v1_cm_proto_rawDescOnce sync.Once
	file_proto_v1_cm_proto_rawDescData = file_proto_v1_cm_proto_rawDesc
)

func file_proto_v1_cm_proto_rawDescGZIP() []byte {
	file_proto_v1_cm_proto_rawDescOnce.Do(func() {
		file_proto_v1_cm_proto_rawDescData = protoimpl.X.CompressGZIP(file_proto_v1_cm_proto_rawDescData)
	})
	return file_proto_v1_cm_proto_rawDescData
}

var file_proto_v1_cm_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_proto_v1_cm_proto_goTypes = []any{
	(*LogEntry)(nil),              // 0: proto.v1.LogEntry
	(*PersistentState)(nil),       // 1: proto.v1.PersistentState
	(*RequestVoteRequest)(nil),    // 2: proto.v1.RequestVoteRequest
	(*RequestVoteResponse)(nil),   // 3: proto.v1.RequestVoteResponse
	(*AppendEntriesRequest)(nil),  // 4: proto.v1.AppendEntriesRequest
	(*AppendEntriesResponse)(nil), // 5: proto.v1.AppendEntriesResponse
}
var file_proto_v1_cm_proto_depIdxs = []int32{
	0, // 0: proto.v1.PersistentState.log:type_name -> proto.v1.LogEntry
	0, // 1: proto.v1.AppendEntriesRequest.entries:type_name -> proto.v1.LogEntry
	2, // 2: proto.v1.CMService.RequestVote:input_type -> proto.v1.RequestVoteRequest
	4, // 3: proto.v1.CMService.AppendEntries:input_type -> proto.v1.AppendEntriesRequest
	3, // 4: proto.v1.CMService.RequestVote:output_type -> proto.v1.RequestVoteResponse
	5, // 5: proto.v1.CMService.AppendEntries:output_type -> proto.v1.AppendEntriesResponse
	4, // [4:6] is the sub-list for method output_type
	2, // [2:4] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_proto_v1_cm_proto_init() }
func file_proto_v1_cm_proto_init() {
	if File_proto_v1_cm_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_proto_v1_cm_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_proto_v1_cm_proto_goTypes,
		DependencyIndexes: file_proto_v1_cm_proto_depIdxs,
		MessageInfos:      file_proto_v1_cm_proto_msgTypes,
	}.Build()
	File_proto_v1_cm_proto = out.File
	file_proto_v1_cm_proto_rawDesc = nil
	file_proto_v1_cm_proto_goTypes = nil
	file_proto_v1_cm_proto_depIdxs = nil
}
