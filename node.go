package eraft

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"github.com/azkj/eraft/transport"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
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
	membership   *Membership
	confState    raftpb.ConfState
	node         raft.Node
	wal          *wal.WAL
	snapshotter  *snap.Snapshotter
	raftStorage  *raft.MemoryStorage
	queue        *blockqueue.QueueWithContext
	proposeQueue *blockqueue.QueueWithContext

	//transport       *rafthttp.Transport
	transport       *transport.Transport
	serv            *http.Server
	isLeader        bool
	readStatus      unsafe.Pointer //*raft.ReadState
	committedIndex  uint64
	termIndex       uint64
	appliedIndex    uint64
	snapIndex       uint64
	pendingSnapshot bool
	stopC           chan interface{}
	isStop          int32
	lead            uint64
}

type NodeMeta struct {
	NodeID    uint64 `json:"node_id"`
	ClusterID uint64 `json:"cluster_id"`
}

func (n NodeMeta) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddUint64("node_id", n.NodeID)
	encoder.AddUint64("cluster_id", n.ClusterID)
	return nil
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
	if err := json.Unmarshal(metadata, &nodeMeta); err != nil {
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
	membership := openMembership(options.Logger, options.MemberDir,
		options.ClusterName, options.NodeName, options.MembersUrlMap)
	raftStorage := raft.NewMemoryStorage()
	snapshotter := snap.New(options.Logger, options.SnapDir)
	var raftWal *wal.WAL
	var raftNode raft.Node
	if restart {
		options.Logger.Info("wal exist")
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
		if nodeMeta.ClusterID == 0 || nodeMeta.NodeID == 0 {
			options.Logger.Fatal("nodeMeta error", zap.Object("nodeMeta", nodeMeta))
		}
		options.Logger.Info("wal entries", zap.Int("size", len(entries)))
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
				NodeID:    membership.LocalID,
				ClusterID: membership.ClusterID,
			},
		)
		if err := fileutil.CreateDirAll(options.SnapDir); err != nil {
			options.Logger.Fatal("create dir failed",
				zap.String("dir", options.SnapDir), zap.Error(err))
		}
		if raftWal, err = wal.Create(options.Logger, options.WalDir, metadata); err != nil {
			options.Logger.Panic("failed to create WAL", zap.Error(err))
		}
		var peers []raft.Peer
		for _, member := range membership.GetMembers() {
			ctx, _ := json.Marshal(member)
			peers = append(peers, raft.Peer{ID: member.ID, Context: ctx})
			options.Logger.Info("peer info", zap.Uint64("ID", member.ID))
		}
		member := membership.LocalMember()
		options.Logger.Info(
			"starting local member",
			zap.String("local-member-id", member.Name),
			zap.String("cluster-id", membership.GetClusterName()),
		)
		c := &raft.Config{
			ID:              member.ID,
			ElectionTick:    options.ElectionTick,
			HeartbeatTick:   1,
			Storage:         raftStorage,
			MaxSizePerMsg:   options.MaxSizePerMsg,
			MaxInflightMsgs: options.MaxInflightMsgs,
			PreVote:         options.PreVote,
			CheckQuorum:     true,
		}
		raftNode = raft.StartNode(c, peers)
		//raftNode.Tick()
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
		stopC:           make(chan interface{}),
		raftStorage:     raftStorage,
		proposeQueue:    blockqueue.NewQueueWithContext(context.Background(), 64),
	}
	cb.node = node

	encoderCfg := zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		NameKey:        "logger",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
	}
	core := zapcore.NewCore(zapcore.NewJSONEncoder(encoderCfg), os.Stdout, zap.DebugLevel)
	logger := zap.New(core).WithOptions()

	/*node.transport = &rafthttp.Transport{
		Logger:      logger,
		ID:          types.ID(membership.LocalID),
		ClusterID:   types.ID(membership.ClusterID),
		Raft:        cb,
		ServerStats: stats.NewServerStats(membership.ClusterName, membership.LocalMember().Name),
		LeaderStats: stats.NewLeaderStats(strconv.FormatUint(membership.LocalID, 10)),
		ErrorC:      make(chan error),
	}*/

	transportOptions := transport.DefaultOptions()
	transportOptions.Logger = logger
	transportOptions.ClusterID = membership.ClusterID
	transportOptions.Raft = cb

	_, port, err := net.SplitHostPort(membership.LocalMember().Addrs[0])
	if err != nil {
		options.Logger.Fatal("parse url failed",
			zap.String("addr", membership.LocalMember().Addrs[0]), zap.Error(err))
	}
	transportOptions.Port = port

	node.transport = transport.NewTransport(transportOptions)

	go func() {
		if err := node.transport.Run(); err != nil {
			options.Logger.Fatal(err.Error())
		}
	}()
	for _, member := range membership.GetMembers() {
		if member.ID == membership.LocalID {
			continue
		}
		node.transport.AddPeer(member.ID, member.Addrs)
	}

	/*serv := &http.Server{
		Handler: node.transport.Handler(),
	}
	node.serv = serv
	url, err := url.Parse(membership.LocalMember().URLs[0])
	if err != nil {
		options.Logger.Fatal("parse url failed",
			zap.String("url", membership.LocalMember().URLs[0]), zap.Error(err))
	}
	l, err := net.Listen("tcp", net.JoinHostPort(options.Host, url.Port()))
	if err != nil {
		options.Logger.Fatal("listen failed",
			zap.Strings("url", membership.LocalMember().URLs), zap.Error(err))
	}
	options.Logger.Info("-----------node listen success----------",
		zap.String("addr", net.JoinHostPort(options.Host, url.Port())),
		zap.String("name", membership.NodeName))
	go func() {
		if err := serv.Serve(l); err != nil {
			if err != http.ErrServerClosed {
				options.Logger.Fatal("http serve failed", zap.Error(err))
			}
		}
	}()
	*/
	go func() {
		if err := node.loop(); err != nil {
			node.Logger.Error("node error stop", zap.Error(err))
			_ = node.Close()
		}
	}()
	go func() {
		for i := 0; i < 1; i++ {
			node.proposeLoop()
		}
	}()
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
func (node *Node) proposeLoop() {
	for {
		items, _ := node.proposeQueue.PopAll(nil)
		var buffer = bytes.NewBuffer(make([]byte, 0, 64*1024))
		var count int
		for _, it := range items {
			count += 1
			data := it.([]byte)
			_ = binary.Write(buffer, binary.BigEndian, uint32(len(data)))
			buffer.Write(data)
		}
		//node.Logger.Info("propose", zap.Int("count", count))
		if err := node.node.Propose(context.Background(), buffer.Bytes()); err != nil {
			node.Logger.Error("propose failed", zap.Error(err))
		}
		//time.Sleep(time.Millisecond * 10)
	}
}

func (node *Node) loop() error {
	tick := time.NewTicker(node.Options.TickMs)
	defer func() {
		tick.Stop()
	}()
	for {
		select {
		case <-tick.C:
			node.node.Tick()
		case rd := <-node.node.Ready():
			if err := node.handleReady(&rd); err != nil {
				return err
			}
			node.node.Advance()
		case <-node.stopC:
			node.Logger.Warn("node quit",
				zap.String("nodeID", node.membership.LocalMember().Name))
			return nil
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
	//node.Logger.Info("applyEntries", zap.Int("size", len(entries)))
	applyEntries := make([]ApplyEntry, 0, len(entries))
	pushEntries := func() error {
		if len(applyEntries) != 0 {
			//node.Logger.Info("pushEntries", zap.Int("size", len(applyEntries)))
			if err := node.queue.Push(applyEntries); err != nil {
				return errors.WithStack(err)
			}
			applyEntries = make([]ApplyEntry, 0, len(entries))
		}
		return nil
	}
	for i := range entries {
		entry := &entries[i]
		//node.Logger.Info("entryType",
		//zap.String("type", entry.Type.String()), zap.String("data", string(entry.Data)))
		switch entry.Type {
		case raftpb.EntryConfChangeV2:
			data, _ := json.Marshal(entry)
			var buf bytes.Buffer
			json.Indent(&buf, data, "", "    ")
			node.Logger.Fatal("handle error", zap.String("entry", buf.String()))
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
			if len(entry.Data) == 0 {
				continue
			}
			var reader = bytes.NewReader(entry.Data)
			for {
				var size uint32
				if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
					if err == io.EOF {
						break
					}
				}
				data := make([]byte, size)
				_, err := io.ReadFull(reader, data)
				if err != nil {
					node.Logger.Panic("encode entry failed", zap.Error(err))
				}
				applyEntries = append(applyEntries, ApplyEntry{
					Index: entry.Index,
					Data:  data,
				})
			}
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
	//node.Logger.Info("handle Ready")
	if rd.SoftState != nil && rd.SoftState.Lead != 0 {
		if node.lead != rd.SoftState.Lead {
			node.lead = rd.SoftState.Lead
			if node.membership.LocalID == node.lead {
				node.isLeader = true
			} else {
				node.isLeader = false
			}
		}
	}
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
	node.Logger.Info("handleConfChange", zap.String("type", cc.Type.String()), zap.String("context", string(cc.Context)))
	node.confState = *node.node.ApplyConfChange(cc)
	switch cc.Type {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		if len(cc.Context) > 0 {
			var member Member
			if err := json.Unmarshal(cc.Context, &member); err != nil {
				node.Logger.Fatal(err.Error())
			}
			//node.transport.AddPeer(types.ID(cc.ID), member.URLs)
			node.Logger.Info("member info", zap.Object("member", member))
		}
	}
	return nil
}

func (node *Node) sendMessage(messages []raftpb.Message) error {
	if len(messages) == 0 {
		return nil
	}
	/*for _, message := range messages {
		node.Logger.Info("sendMessage",
			zap.String("node", node.membership.GetMember(message.To).Name),
			zap.Uint64("nodeID", message.To))
	}*/
	//node.Logger.Info("sendmessage", zap.Int("size", len(messages)))
	node.transport.SendMessages(messages)
	return nil
}

func (node *Node) saveEntryWithState(entries []raftpb.Entry, state raftpb.HardState) error {
	if len(entries) == 0 && raft.IsEmptyHardState(state) {
		return nil
	}
	//node.Logger.Info("saveEntry", zap.Int("size", len(entries)))
	/*if err := node.wal.Save(state, entries); err != nil {
		return errors.WithStack(err)
	}*/
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
	//node.Logger.Info("setCommittedIndex", zap.Uint64("index", index))
	atomic.StoreUint64(&node.committedIndex, index)
}

func (node *Node) CommittedIndex() uint64 {
	return atomic.LoadUint64(&node.committedIndex)
}

func (node *Node) isIDRemoved(to uint64) bool {
	return false
}

func (node *Node) triggerSnapshot(index uint64) error {
	node.Logger.Info("triggerSnapshot", zap.Uint64("index", index))
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
	return node.proposeQueue.Push(data)
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

func (node *Node) Close() error {
	if atomic.CompareAndSwapInt32(&node.isStop, 0, 1) == false {
		return nil
	}
	node.node.Stop()
	close(node.stopC)
	return node.serv.Close()
}
