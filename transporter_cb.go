package eraft

import (
	"context"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

type transporterCB struct {
	node *Node
}

func (t *transporterCB) Process(ctx context.Context, m raftpb.Message) error {
	return t.node.process(ctx,m)
}

func (t *transporterCB) IsIDRemoved(id uint64) bool {
	return t.node.isIDRemoved(id)
}

func (t *transporterCB) ReportUnreachable(id uint64) {
	t.node.reportUnreachable(id)
}

func (t *transporterCB) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	t.node.reportSnapshot(id,status)
}

