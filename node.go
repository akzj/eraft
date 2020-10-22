package eraft

import (
	"context"
	"encoding/json"
	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/akzj/block-queue"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/etcdserver/api/snap"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"
)

type Node struct {
	Options
	membership      *Membership
	confState       raftpb.ConfState
	node            raft.Node
	wal             *wal.WAL
	snapshotter     *snap.Snapshotter
	raftStorage     *raft.MemoryStorage
	queue           *blockqueue.QueueWithContext
	transport       *rafthttp.Transport
	isLeader        bool
	readStatus      unsafe.Pointer //*raft.ReadState
	committedIndex  uint64
	termIndex       uint64
	appliedIndex    uint64
	snapIndex       uint64
	pendingSnapshot bool
}

type NodeMeta struct {
	NodeID    uint64 `json:"node_id"`
	ClusterID uint64 `json:"cluster_id"`
}

func readWAL(lg *zap.Logger, waldir string, snap walpb.Snapshot) (
	w *wal.WAL, nodeMeta NodeMeta, st raftpb.HardState, entries []raftpb.Entry) {
	var (
		err      error
		metadata []byte
	)
	repaired := false
	for {
		if w, err = wal.Open(lg, waldir, snap); err != nil {
			lg.Fatal("failed to open WAL", zap.Error(err))
		}
		if metadata, st, entries, err = w.ReadAll(); err != nil {
			w.Close()
			// we can only repair ErrUnexpectedEOF and we never repair twice.
			if repaired || err != io.ErrUnexpectedEOF {
				lg.Fatal("failed to read WAL, cannot be repaired", zap.Error(err))
			}
			if !wal.Repair(lg, waldir) {
				lg.Fatal("failed to repair WAL", zap.Error(err))
			} else {
				lg.Info("repaired WAL", zap.Error(err))
				repaired = true
			}
			continue
		}
		break
	}
	var meta NodeMeta
	if err := json.Unmarshal(metadata, &meta); err != nil {
		panic(err)
	}
	return w, nodeMeta, st, entries
}

func StartNode(options Options) *Node {
	var restart = true
	if wal.Exist(options.WalDir) == false {
		restart = false
		if err := fileutil.CreateDirAll(options.WalDir); err != nil {
			options.Logger.Fatal(
				"failed to create snapshot directory",
				zap.String("path", options.SnapDir),
				zap.Error(err),
			)
		}
	}
	membership := openMembership(options.Logger, options.MemberDir, options.ClusterName, options.MembersUrlMap)
	raftStorage := raft.NewMemoryStorage()
	snapshotter := snap.New(options.Logger, options.SnapDir)
	var raftWal *wal.WAL
	var raftNode raft.Node
	if restart {
		var walsnap walpb.Snapshot
		walSnaps, err := wal.ValidSnapshotEntries(options.Logger, options.WalDir)
		if err != nil {
			options.Logger.Fatal(err.Error())
		}
		snapshot, err := snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil && err != snap.ErrNoSnapshot {
			options.Logger.Fatal(err.Error())
		}
		if snapshot != nil {
			var wg sync.WaitGroup
			wg.Add(1)
			options.RecoveryFromSnapshot(ApplySnapshot{
				Data:  snapshot.Data,
				Index: snapshot.Metadata.Index,
				Done: func(err error) {
					if err != nil {
						options.Logger.Fatal(err.Error())
					}
					wg.Done()
				},
			})
			wg.Wait()
		}

		if snapshot != nil {
			walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
			if err := raftStorage.ApplySnapshot(*snapshot); err != nil {
				options.Logger.Fatal("applySnapshot failed", zap.Error(err))
			}
		}
		w, nodeMeta, st, entries := readWAL(options.Logger, options.WalDir, walsnap)
		if err := raftStorage.Append(entries); err != nil {
			options.Logger.Fatal("raftStorage append entries failed", zap.Error(err))
		}
		if err := raftStorage.SetHardState(st); err != nil {
			options.Logger.Fatal("raftStorage setHardState failed", zap.Error(err))
		}
		raftWal = w
		raftNode = raft.RestartNode(&raft.Config{
			ID:              nodeMeta.NodeID,
			ElectionTick:    options.ElectionTick,
			HeartbeatTick:   options.HeartbeatTick,
			Storage:         raftStorage,
			MaxSizePerMsg:   options.MaxSizePerMsg,
			MaxInflightMsgs: options.MaxInflightMsgs,
			CheckQuorum:     true,
			PreVote:         options.PreVote,
		})
	} else {
		var err error
		metadata, _ := json.Marshal(
			&NodeMeta{
				NodeID:    options.NodeID,
				ClusterID: options.ClusterID,
			},
		)
		if raftWal, err = wal.Create(options.Logger, options.WalDir, metadata); err != nil {
			options.Logger.Panic("failed to create WAL", zap.Error(err))
		}
		var peers []raft.Peer
		for _, member := range membership.GetMembers() {
			ctx, _ := json.Marshal(member)
			peers = append(peers, raft.Peer{ID: member.ID, Context: ctx})
		}
		member := membership.LocalMember()
		options.Logger.Info(
			"starting local member",
			zap.String("local-member-id", member.Name),
			zap.String("cluster-id", membership.GetClusterName()),
		)
		s := raft.NewMemoryStorage()
		c := &raft.Config{
			ID:              member.ID,
			ElectionTick:    options.ElectionTick,
			HeartbeatTick:   1,
			Storage:         s,
			MaxSizePerMsg:   options.MaxSizePerMsg,
			MaxInflightMsgs: options.MaxInflightMsgs,
			PreVote:         options.PreVote,
			CheckQuorum:     true,
		}
		if len(peers) == 0 {
			raftNode = raft.RestartNode(c)
		} else {
			raftNode = raft.StartNode(c, peers)
		}
	}

	cb := new(transporterCB)
	node := &Node{
		Options:         options,
		confState:       raftpb.ConfState{},
		node:            raftNode,
		wal:             raftWal,
		snapshotter:     snapshotter,
		membership:      membership,
		queue:           blockqueue.NewQueueWithContext(context.Background(), options.QueueCap),
		pendingSnapshot: false,
	}

	node.transport = &rafthttp.Transport{
		Logger:      zap.NewExample(),
		ID:          types.ID(options.NodeID),
		ClusterID:   types.ID(options.ClusterID),
		Raft:        cb,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.FormatUint(options.NodeID, 10)),
		ErrorC:      make(chan error),
	}

	if err := node.transport.Start(); err != nil {
		options.Logger.Fatal(err.Error())
	}
	for _, member := range membership.GetMembers() {
		if member.ID == options.NodeID {
			continue
		}
		node.transport.AddPeer(types.ID(member.ID), member.URLs)
	}

	return node
}

type ApplyEntry struct {
	Index uint64
	Data  []byte
}

type ApplyDone func()

type CreateSnapshot struct {
	Create func(Data []byte) error
}

type ApplySnapshot struct {
	Data  []byte
	Index uint64
	Done  func(err error)
}

type SendSnapshot struct {
}
type HijackSnapshot struct {
	Index uint64
	Data  []byte
}

func (node *Node) Queue() *blockqueue.QueueWithContext {
	return node.queue
}

func (node *Node) ErrorC() chan error {
	return node.transport.ErrorC
}

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
	if err := node.queue.Push(ApplySnapshot{
		Data:  snapshot.Data,
		Index: snapshot.Metadata.Index,
		Done: func(e error) {
			if e != nil {
				return
			}
			node.confState = snapshot.Metadata.ConfState
			node.termIndex = snapshot.Metadata.Term
			node.committedIndex = snapshot.Metadata.Index
		},
	}); err != nil {
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
	applyEntries := make([]ApplyEntry, 0, len(entries))
	pushEntries := func() error {
		if len(applyEntries) != 0 {
			if err := node.queue.Push(applyEntries); err != nil {
				return errors.WithStack(err)
			}
			applyEntries = make([]ApplyEntry, 0, len(entries))
		}
		return nil
	}
	for i := range entries {
		entry := &entries[i]
		switch entry.Type {
		case raftpb.EntryConfChange:
			if err := pushEntries(); err != nil {
				return err
			}
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
			applyEntries = append(applyEntries, ApplyEntry{
				Index: entry.Index,
				Data:  entry.Data,
			})
			node.setCommittedIndex(entry.Index)
		}
	}
	if err := pushEntries(); err != nil {
		return err
	}
	if node.checkTriggerSnapshot() {
		if err := node.triggerSnapshot(node.CommittedIndex()); err != nil {
			return err
		}
	}
	return nil
}

func (node *Node) createSnapshot(index uint64, sc raftpb.ConfState, data []byte) error {
	snapshot, err := node.raftStorage.CreateSnapshot(index, &sc, data)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := node.saveSnapshot(snapshot); err != nil {
		return err
	}

	if err := node.releaseToSnapshot(snapshot); err != nil {
		return errors.WithStack(err)
	}

	compactIndex := uint64(1)
	if index > node.SnapshotCatchUpEntries {
		compactIndex = index - node.SnapshotCatchUpEntries
	}

	if err := node.raftStorage.Compact(compactIndex); err != nil {
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
		if err := node.releaseToSnapshot(rd.Snapshot); err != nil {
			return err
		}
	}
	if len(rd.CommittedEntries) != 0 {
		if err := node.applyEntries(rd.CommittedEntries); err != nil {
			return err
		}
	}

	node.filterMessage(rd.Messages)
	if node.IsLeader() {
		if err := node.sendMessage(rd.Messages); err != nil {
			return err
		}
	}
	if err := node.saveEntryWithState(rd.Entries, rd.HardState); err != nil {
		return err
	}
	if node.IsLeader() == false {
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
	node.transport.Send(messages)
	return nil
}

func (node *Node) saveEntryWithState(entries []raftpb.Entry, state raftpb.HardState) error {
	if err := node.wal.Save(state, entries); err != nil {
		return errors.WithStack(err)
	}
	if err := node.raftStorage.Append(entries); err != nil {
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
	if err := node.snapshotter.ReleaseSnapDBs(snapshot); err != nil {
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

func (node *Node) triggerSnapshot(index uint64) error {
	cs := node.confState
	if err := node.queue.Push(CreateSnapshot{func(Data []byte) error {
		if err := node.createSnapshot(index, cs, Data); err != nil {
			return err
		}
		node.snapIndex = index
		node.pendingSnapshot = false
		return nil
	}}); err != nil {
		return err
	}
	return nil
}

func (node *Node) checkTriggerSnapshot() bool {
	if node.CommittedIndex()-node.snapIndex <= node.SnapshotCount || node.pendingSnapshot {
		return false
	}
	return true
}

func (node *Node) Propose(ctx context.Context, data []byte) error {
	return node.node.Propose(ctx, data)
}

func (node *Node) process(ctx context.Context, m raftpb.Message) error {
	return node.node.Step(ctx, m)
}

func (node *Node) reportUnreachable(id uint64) {
	node.node.ReportUnreachable(id)
}

func (node *Node) reportSnapshot(id uint64, status raft.SnapshotStatus) {
	node.node.ReportSnapshot(id, status)
}
