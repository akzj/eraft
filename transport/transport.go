package transport

import (
	"context"
	"github.com/akzj/block-queue"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
	"io"
	"net"
	"sync"
	"time"
)

type Transport struct {
	Options
	ctxCancel   context.CancelFunc
	peersLocker sync.RWMutex
	peers       map[uint64]*Peer
	remoteNodes map[uint64]*Peer
}

type Options struct {
	Raft             rafthttp.Raft
	Port             string
	Host             string
	MessageGroupSize int
	CTX              context.Context
	CompactionType   CompactionType
	WriteTimeout     time.Duration
	ReadTimeout      time.Duration
	Logger           *zap.Logger
	DailTimeout      time.Duration
	ClusterID        uint64
	SendQueueCap     int
	RecvQueueCap     int
}

func DefaultOptions() Options {
	return Options{
		Port:             "5000",
		Host:             "0.0.0.0",
		MessageGroupSize: 1024 * 1024 * 4,
		CTX:              context.Background(),
		CompactionType:   0,
		//WriteTimeout:     time.Second * 5,
		//ReadTimeout:      time.Second * 5,
		DailTimeout:  time.Second * 5,
		Logger:       zap.NewExample(),
		ClusterID:    0,
		SendQueueCap: 10240,
		RecvQueueCap: 10240,
	}
}

func NewTransport(options Options) *Transport {
	ctx, cancel := context.WithCancel(options.CTX)
	options.CTX = ctx
	return &Transport{
		Options:     options,
		ctxCancel:   cancel,
		peersLocker: sync.RWMutex{},
		peers:       map[uint64]*Peer{},
		remoteNodes: map[uint64]*Peer{},
	}
}

func (transport *Transport) createNewPeer(id uint64, addreses []string) *Peer {
	ctx, cancel := context.WithCancel(transport.CTX)
	peer := &Peer{
		ctx:              ctx,
		ctxCancel:        cancel,
		sendQueue:        blockqueue.NewQueueWithContext(ctx, transport.SendQueueCap),
		recvQueue:        blockqueue.NewQueueWithContext(ctx, transport.RecvQueueCap),
		conn:             nil,
		messageGroupSize: transport.MessageGroupSize,
		compactionType:   transport.CompactionType,
		writeTimeout:     transport.WriteTimeout,
		readTimeout:      transport.ReadTimeout,
		logger:           transport.Logger,
		ID:               id,
		addrs:            addreses,
		addrsLocker:      sync.Mutex{},
		dailTimeout:      transport.DailTimeout,
		isClose:          false,
		readBuffer:       make([]byte, 1024*64),
		helloMessage: helloMessage{
			ClusterID: transport.ClusterID,
			NodeID:    id,
		},
	}
	transport.peersLocker.Lock()
	defer transport.peersLocker.Unlock()
	if old, ok := transport.peers[id]; ok == false {
		transport.peers[id] = peer
		go peer.run()
		go transport.pullMessageLoop(peer)
		return peer
	} else {
		return old
	}
}

func (transport *Transport) AddPeer(ID uint64, addrs []string) {
	transport.Logger.Info("addPeer", zap.Uint64("ID", ID), zap.Strings("addrs", addrs))
	peer := transport.findPeers(ID)
	if peer == nil {
		peer = transport.createNewPeer(ID, addrs)
	} else {
		peer.appendAddrs(addrs...)
	}
}

func (transport *Transport) SendMessages(messages []raftpb.Message) {
	for _, message := range messages {
		if peer := transport.findPeers(message.To); peer != nil {
			peer.pushMessage(message)
		}
	}
}

func (transport *Transport) findPeers(to uint64) *Peer {
	transport.peersLocker.RLock()
	peer, _ := transport.remoteNodes[to]
	if peer != nil {
		transport.peersLocker.RUnlock()
		return peer
	}
	peer, _ = transport.peers[to]
	transport.peersLocker.RUnlock()
	return peer
}

func (transport *Transport) pullMessageLoop(peer *Peer) {
	transport.Logger.Info("start pull Message", zap.Uint64("node", peer.ID))
	for {
		items, err := peer.popMessages()
		if err != nil {
			transport.Logger.Warn("peer popMessage failed", zap.Uint64("id", peer.ID), zap.Error(err))
			return
		}
		for _, it := range items {
			switch msg := it.(type) {
			case raftpb.Message:
				//transport.Logger.Info("receive message")
				if err := transport.Raft.Process(transport.CTX, msg); err != nil {
					transport.Logger.Warn("raft process failed",
						zap.String("message type", msg.Type.String()))
				}
			}
		}
	}
}
func (transport *Transport) handleConn(conn net.Conn) {
	addr := conn.RemoteAddr()
	transport.Logger.Debug("handle remote connection", zap.String("addr", addr.String()))
	var message helloMessage
	var buffer = make([]byte, 16)
	_, err := io.ReadFull(conn, buffer)
	if err != nil {
		transport.Logger.Debug("read helloMessage failed")
		_ = conn.Close()
		return
	}
	message.decode(buffer)
	if transport.ClusterID != message.ClusterID {
		transport.Logger.Warn("helloMessage clusterID no equal local clusterID",
			zap.Uint64("message.ClusterID", message.ClusterID),
			zap.Uint64("transport.ClusterID", transport.ClusterID))
		_ = conn.Close()
		return
	}
	transport.Logger.Debug("receive HelloMessage done", zap.Uint64("NodeID", message.NodeID))
	if transport.WriteTimeout != 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(transport.WriteTimeout)); err != nil {
			_ = conn.Close()
			return
		}
	}
	if _, err := conn.Write(buffer); err != nil {
		transport.Logger.Debug("send data failed", zap.String("addr", addr.String()), zap.Error(err))
		_ = conn.Close()
		return
	}

	ctx, cancel := context.WithCancel(transport.CTX)
	peer := &Peer{
		conn:             conn,
		ctx:              ctx,
		ctxCancel:        cancel,
		sendQueue:        blockqueue.NewQueueWithContext(ctx, transport.SendQueueCap),
		recvQueue:        blockqueue.NewQueueWithContext(ctx, transport.RecvQueueCap),
		messageGroupSize: transport.MessageGroupSize,
		compactionType:   transport.CompactionType,
		writeTimeout:     transport.WriteTimeout,
		readTimeout:      transport.ReadTimeout,
		logger:           transport.Logger,
		dailTimeout:      transport.DailTimeout,
		ID:               message.NodeID,
		addrs:            []string{addr.String()},
		addrsLocker:      sync.Mutex{},
		isClose:          false,
		readBuffer:       make([]byte, 1024*64),
		helloMessage:     message,
	}
	transport.peersLocker.Lock()
	defer transport.peersLocker.Unlock()
	if _, ok := transport.remoteNodes[message.NodeID]; ok {
		//old.Close()
		transport.Logger.Warn("message.NodeID remote node exist", zap.Uint64("NodeID", message.NodeID))
	}
	transport.remoteNodes[message.NodeID] = peer
	go peer.run()
	go transport.pullMessageLoop(peer)
}

func (transport *Transport) Run() error {
	bind := net.JoinHostPort(transport.Host, transport.Port)
	transport.Logger.Info("bind ", zap.String("addr", bind))
	listener, err := net.Listen("tcp", bind)
	if err != nil {
		return errors.WithStack(err)
	}
	for {
		conn, err := listener.Accept()
		var tempDelay time.Duration // how long to sleep on accept failure
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				transport.Logger.Error("accept failed", zap.Error(err))
				time.Sleep(tempDelay)
				continue
			}
			return err
		}
		go transport.handleConn(conn)
	}
}
