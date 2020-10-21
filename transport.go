package eraft

import (
	"go.etcd.io/etcd/raft/raftpb"
)

type ID uint64

type Transporter interface {
	Start() error
	Send(cluster ID, messages []raftpb.Message)
	Stop()
}
