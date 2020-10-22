package kvstore

import (
	"encoding/json"
	"fmt"
	"github.com/azkj/eraft"
	"log"
	"sync"
)

type SetReq struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Store struct {
	sync.RWMutex
	DataSet    map[string]string `json:"data_set"`
	ApplyIndex uint64
}

func (s *Store) Set(key string, value string, index uint64) {
	s.Lock()
	defer s.Unlock()
	s.DataSet[key] = value
	s.ApplyIndex = index
}

func (s *Store) makeSnapshot() []byte {
	s.RLock()
	defer s.RUnlock()
	data, _ := json.Marshal(s)
	return data
}

type Serv struct {
	node  *eraft.Node
	store *Store
}

func (s *Serv) startRaftNode() {
	go func() {
		queue := s.node.Queue()
		for {
			items, err := queue.PopAll(nil)
			if err != nil {
				panic(err)
			}
			for i := range items {
				item := items[i]
				switch obj := item.(type) {
				case []eraft.ApplyEntry:
					s.applyEntries(obj)
				case eraft.ApplyDone:
					obj()
				case eraft.ApplySnapshot:
					s.applySnapshot(obj)
				case eraft.SendSnapshot:
					s.applySendSnapshot(obj)
				case eraft.CreateSnapshot:
					s.createSnapshot(obj)
				}
			}
		}
	}()
}

func (s *Serv) applyEntries(entries []eraft.ApplyEntry) {
	for _, entry := range entries {
		var req SetReq
		if err := json.Unmarshal(entry.Data, &req); err != nil {
			log.Panic(err)
		}
		s.store.Set(req.Key, req.Value, entry.Index)
	}
}

func (s *Serv) applySnapshot(snapshot eraft.ApplySnapshot) {
	var store Store
	if err := json.Unmarshal(snapshot.Data, &store); err != nil {
		snapshot.Done(err)
		fmt.Println(err.Error())
	} else {
		snapshot.Done(nil)
	}
	s.store = &store
}

func (s *Serv) applySendSnapshot(send eraft.SendSnapshot) {
	log.Panic("no reject")
}

func (s *Serv) createSnapshot(snap eraft.CreateSnapshot) {
	data := s.store.makeSnapshot()
	if err := snap.Create(data); err != nil {
		panic(err)
	}
}
