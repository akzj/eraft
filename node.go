package eraft

import (
	"encoding/json"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/akzj/block-queue"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"
)

type Node struct {
	Options
	confState      raftpb.ConfState
	node           raft.Node
	wal            *wal.WAL
	snapshotter    *snap.Snapshotter
	raftStorage    *raft.MemoryStorage
	queue          *blockqueue.QueueWithContext
	transport      Transporter
	isLeader       bool
	readStatus     unsafe.Pointer //*raft.ReadState
	committedIndex uint64
}

type ApplyDone func()

func (node *Node) Loop() error {
	var rd raft.Ready
	tick := time.NewTicker(node.TickInterval)
	defer func() {
		tick.Stop()
	}()
	for {
		select {
		case rd = <-node.node.Ready():
			if err := node.handleReady(&rd); err != nil {
				return err
			}
			node.node.Advance()
		case <-tick.C:
			node.node.Tick()
			continue
		}
	}
}

func (node *Node) applySnapshot(snapshot raftpb.Snapshot) error {
	if err := node.queue.Push(snapshot); err != nil {
		return errors.WithStack(err)
	}
	node.setCommittedIndex(snapshot.Metadata.Index)
	return nil
}

func createApplyDone() (ApplyDone, <-chan interface{}) {
	ch := make(chan interface{})
	return func() {
		close(ch)
	}, ch
}

func (node *Node) applyEntries(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	for i := range entries {
		entry := &entries[i]
		switch entry.Type {
		case raftpb.EntryConfChange:
			done, ch := createApplyDone()
			if err := node.queue.Push(done); err != nil {
				return err
			}
			select {
			case <-ch:
			}
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(entry.Data); err != nil {
				return errors.WithStack(err)
			}
			if err := node.handleConfChange(cc); err != nil {
				return err
			}
		case raftpb.EntryNormal:
			if err := node.queue.Push(entry); err != nil {
				return errors.WithStack(err)
			}
			node.setCommittedIndex(entry.Index)

		}
	}
	return nil
}

func (node *Node) CreateSnapshot(index uint64, sc raftpb.ConfState, data []byte) error {
	snapshot, err := node.raftStorage.CreateSnapshot(index, &sc, data)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := node.saveSnapshot(snapshot);err !=nil{
		return err
	}

	if err := node.releaseToSnapshot(snapshot);err != nil{
		return errors.WithStack(err)
	}

	compactIndex := uint64(1)
	if index > node.SnapshotCatchUpEntries {
		compactIndex = index - node.SnapshotCatchUpEntries
	}

	if err := node.raftStorage.Compact(compactIndex);err != nil{
		return errors.WithStack(err)
	}

	return nil
}

func (node *Node) handleReady(rd *raft.Ready) error {
	if raft.IsEmptySnap(rd.Snapshot) == false {
		if err := node.saveSnapshot(rd.Snapshot); err != nil {
			return err
		}
		if err := node.raftStorage.ApplySnapshot(rd.Snapshot); err != nil {
			return errors.WithStack(err)
		}
		if err := node.applySnapshot(rd.Snapshot); err != nil {
			return err
		}
		if err := node.releaseToSnapshot(rd.Snapshot);err != nil{
			return err
		}
	}
	if err := node.applyEntries(rd.CommittedEntries); err != nil {
		return err
	}
	if node.IsLeader() {
		if err := node.sendMessage(rd.Messages); err != nil {
			return err
		}
	}
	if err := node.saveEntryWithState(rd.Entries, rd.HardState); err != nil {
		return err
	}
	if node.IsLeader() == false {
		node.filterMessage(rd.Messages)
		if err := node.sendMessage(rd.Messages); err != nil {
			return err
		}
	}
	return nil
}


func (node *Node) filterMessage(ms []raftpb.Message) {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		if node.isIDRemoved(ms[i].To) {
			ms[i].To = 0
		}
		if ms[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}
	}
}

func (node *Node) handleConfChange(cc raftpb.ConfChange) error {
	node.confState = *node.node.ApplyConfChange(cc)
	switch cc.Type {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		var confChangeContext ConfChangeContext
		if err := json.Unmarshal(cc.Context, &confChangeContext); err != nil {
			return errors.WithStack(err)
		}
		//todo
	}
	return nil
}

func (node *Node) sendMessage(messages []raftpb.Message) error {
	if len(messages) == 0 {
		return nil
	}
	node.transport.Send(node.ClusterID, messages)
	return nil
}

func (node *Node) saveEntryWithState(entries []raftpb.Entry, state raftpb.HardState) error {
	if err := node.wal.Save(state, entries); err != nil {
		return errors.WithStack(err)
	}
	if err := node.raftStorage.Append(entries);err != nil{
		return errors.WithStack(err)
	}
	return nil
}

func (node *Node) IsLeader() bool {
	return node.isLeader
}

func (node *Node) saveSnapshot(snapshot raftpb.Snapshot) error {
	if err := node.snapshotter.SaveSnap(snapshot); err != nil {
		return err
	}
	if err := node.wal.SaveSnapshot(walpb.Snapshot{
		Index: snapshot.Metadata.Index, Term: snapshot.Metadata.Term}); err != nil {
		return err
	}
	return nil
}

func (node *Node) releaseToSnapshot(snapshot raftpb.Snapshot) error {
	if err := node.wal.ReleaseLockTo(snapshot.Metadata.Index); err != nil {
		return errors.WithStack(err)
	}
	if err := node.snapshotter.ReleaseSnapDBs(snapshot);err != nil{
		return errors.WithStack(err)
	}
	return nil
}

func (node *Node) setCommittedIndex(index uint64) {
	atomic.StoreUint64(&node.committedIndex, index)
}

func (node *Node) CommittedIndex() uint64 {
	return atomic.LoadUint64(&node.committedIndex)
}

func (node *Node) isIDRemoved(to uint64) bool {
	return false
}
