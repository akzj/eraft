package transport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/akzj/block-queue"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
	"io"
	"net"
	"reflect"
	"sync"
	"time"
)

type Peer struct {
	ctx              context.Context
	ctxCancel        context.CancelFunc
	sendQueue        *blockqueue.QueueWithContext
	recvQueue        *blockqueue.QueueWithContext
	conn             net.Conn
	messageGroupSize int
	compactionType   CompactionType
	writeTimeout     time.Duration
	readTimeout      time.Duration
	logger           *zap.Logger
	ID               uint64
	addrs            []string
	addrsLocker      sync.Mutex
	dailTimeout      time.Duration
	isClose          bool
	readBuffer       []byte
	helloMessage     helloMessage
}

type helloMessage struct {
	ClusterID uint64
	NodeID    uint64
}

func (m *helloMessage) decode(buffer []byte) {
	m.ClusterID = binary.BigEndian.Uint64(buffer)
	buffer = buffer[8:]
	m.NodeID = binary.BigEndian.Uint64(buffer)
}

func (m *helloMessage) encode() []byte {
	var buffer = make([]byte, 16)
	binary.BigEndian.PutUint64(buffer, m.ClusterID)
	binary.BigEndian.PutUint64(buffer[8:], m.NodeID)
	return buffer
}

func (peer *Peer) toMessageGroup(items []interface{}) [][]raftpb.Message {
	var messagesGroup [][]raftpb.Message
	var messages []raftpb.Message
	var size int
	for _, it := range items {
		switch msg := it.(type) {
		case raftpb.Message:
			msgSize := msg.Size() + 4
			if msgSize+size > peer.messageGroupSize && messages != nil {
				messagesGroup = append(messagesGroup, messages)
				messages = nil
				size = 0
				continue
			}
			messages = append(messages, msg)
			size += msgSize
		default:
			panic("unknown object type" + reflect.ValueOf(msg).String())
		}
	}
	if messages != nil {
		messagesGroup = append(messagesGroup, messages)
	}
	return messagesGroup
}

func (peer *Peer) sendLoop() error {
	peer.logger.Debug("start sendLoop")
	for peer.isClose == false {
		items, err := peer.sendQueue.PopAll(nil)
		if err != nil {
			peer.logger.Error("sendQueue popAll error", zap.Error(err))
			return err
		}
		for _, messageGroup := range peer.toMessageGroup(items) {
			data := peer.encoderMessages(messageGroup)
			if peer.writeTimeout != 0 {
				if err := peer.conn.SetWriteDeadline(time.Now().Add(peer.writeTimeout)); err != nil {
					return errors.WithStack(err)
				}
			}
			if _, err := peer.conn.Write(data); err != nil {
				peer.logger.Error("conn write failed", zap.Error(err))
				return err
			}
			//peer.logger.Debug("send messages success", zap.Int("groupCount", len(messageGroup)))
		}
	}
	return nil
}

func (peer *Peer) encoderMessages(group []raftpb.Message) []byte {
	var header = Header{
		MessageCount: uint32(len(group)),
		Compaction:   peer.compactionType,
	}
	var size int
	for _, message := range group {
		size += message.Size() + 4
	}
	var buffer = bytes.NewBuffer(make([]byte, 0, size+8))
	_ = binary.Write(buffer, binary.BigEndian, header.MessageCount)
	_ = binary.Write(buffer, binary.BigEndian, uint32(header.Compaction))
	for _, it := range group {
		n := it.Size()
		_ = binary.Write(buffer, binary.BigEndian, uint32(n))
		data, err := it.Marshal()
		if err != nil {
			panic(err.Error())
		}
		buffer.Write(data)
	}
	return buffer.Bytes()
}

type ReaderWithTimeout struct {
	conn        net.Conn
	readTimeout time.Duration
}

func (r *ReaderWithTimeout) Read(p []byte) (n int, err error) {
	if r.readTimeout != 0 {
		if err := r.conn.SetReadDeadline(time.Now().Add(r.readTimeout)); err != nil {
			return 0, errors.WithStack(err)
		}
	}
	return r.conn.Read(p)
}

func (peer *Peer) receiveLoop() error {
	peer.logger.Debug("start receiveLoop")
	var header Header
	var reader = bufio.NewReaderSize(&ReaderWithTimeout{
		conn: peer.conn, readTimeout: peer.readTimeout}, 64*1024)

	for peer.isClose == false {
		if err := binary.Read(reader, binary.BigEndian, &header.MessageCount); err != nil {
			return err
		}
		if err := binary.Read(reader, binary.BigEndian, (*uint32)(&header.Compaction)); err != nil {
			return err
		}
		var size uint32
		var buffer []byte
		for i := uint32(0); i < header.MessageCount; i++ {
			if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
				return err
			}
			if size < 64*1024 {
				buffer = peer.readBuffer[:size]
			} else {
				buffer = make([]byte, size)
			}
			if _, err := io.ReadFull(reader, buffer); err != nil {
				return errors.WithStack(err)
			}
			var message raftpb.Message
			if err := message.Unmarshal(buffer); err != nil {
				return err
			}
			if err := peer.recvQueue.Push(message); err != nil {
				return err
			}
		}
	}
	return nil
}

func (peer *Peer) pushMessage(message raftpb.Message) {
	remain, _ := peer.sendQueue.PushManyWithoutBlock([]interface{}{message})
	if len(remain) != 0 {
		peer.logger.Warn("peer sendQueue full", zap.Uint64("ID", peer.ID))
	}
}

func (peer *Peer) popMessages() ([]interface{}, error) {
	items, err := peer.recvQueue.PopAll(nil)
	return items, err
}

func (peer *Peer) GetAddrs() []string {
	peer.addrsLocker.Lock()
	defer peer.addrsLocker.Unlock()
	addrs := append([]string{}, peer.addrs...)
	return addrs
}

func (peer *Peer) dail() {
	if peer.conn != nil {
		return
	}
	defer func() {
		peer.logger.Info(" dail to success", zap.Strings("addr", peer.addrs))
	}()
	for peer.isClose == false {
		addrs := peer.GetAddrs()
		for _, addr := range addrs {
			peer.logger.Debug("- - - - - - - - start dail to - - - - - - - -", zap.String("addr", addr))
			conn, err := net.DialTimeout("tcp", addr, peer.dailTimeout)
			if err != nil {
				peer.logger.Debug("dail failed ", zap.String("addr", addr), zap.Error(err))
				continue
			}
			peer.conn = conn
			if err := peer.checkHelloMessage(); err != nil {
				peer.logger.Error(err.Error())
				_ = peer.conn.Close()
				peer.conn = nil
				continue
			}
			return
		}
		time.Sleep(time.Second * 3)
	}
}

func (peer *Peer) run() {
	peer.logger.Info("------- start peer --------", zap.Uint64("nodeID", peer.ID))
	for peer.isClose == false {
		peer.dail()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := peer.sendLoop(); err != nil {
				peer.logger.Warn("sendLoop return error", zap.Uint64("ID", peer.ID), zap.Error(err))
			}
			_ = peer.conn.Close()
		}()
		go func() {
			defer wg.Done()
			if err := peer.receiveLoop(); err != nil {
				peer.logger.Warn("recvLoop return error", zap.Uint64("ID", peer.ID), zap.Error(err))
			}
			_ = peer.conn.Close()
		}()
		wg.Wait()
		//dail again
		peer.conn = nil
	}
}

func (peer *Peer) appendAddrs(addrs ...string) {
	peer.addrsLocker.Lock()
	defer peer.addrsLocker.Unlock()
	for _, addr := range addrs {
		for _, it := range peer.addrs {
			if it == addr {
				continue
			}
		}
		peer.addrs = append(peer.addrs, addr)
	}
}

func (peer *Peer) Close() {
	peer.logger.Info("close peer", zap.Uint64("nodeID", peer.ID))
	if peer.isClose {
		return
	}
	peer.isClose = true
	_ = peer.conn.Close()
	peer.ctxCancel()
}

func (peer *Peer) checkHelloMessage() error {
	if peer.writeTimeout != 0 {
		if err := peer.conn.SetReadDeadline(time.Now().Add(peer.writeTimeout)); err != nil {
			return errors.WithStack(err)
		}
	}
	data := peer.helloMessage.encode()
	if _, err := peer.conn.Write(data); err != nil {
		return err
	}
	peer.logger.Debug("send hello message success", zap.Uint64("node", peer.ID))
	var message helloMessage
	var buffer = make([]byte, 16)
	if peer.readTimeout != 0 {
		if err := peer.conn.SetWriteDeadline(time.Now().Add(peer.readTimeout)); err != nil {
			return err
		}
	}
	if _, err := io.ReadFull(peer.conn, buffer); err != nil {
		peer.logger.Debug("read helloMessage failed", zap.Error(err))
		return err
	}
	message.decode(buffer)
	if peer.helloMessage.ClusterID != message.ClusterID {
		peer.logger.Warn("helloMessage clusterID no equal local clusterID",
			zap.Uint64("message.ClusterID", message.ClusterID),
			zap.Uint64("peer.ClusterID", peer.helloMessage.ClusterID))
		return fmt.Errorf("clusterID no match error")
	}
	return nil
}
