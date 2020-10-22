package filestore

import (
	"encoding/json"
	"fmt"
	"github.com/azkj/eraft"
	"io/ioutil"
	"log"
	"path/filepath"
	"sync"
)

type SyncFileRequest struct {
	Filename string `json:"filename"`
	Data     string `json:"value"`
}

type FileIndexStore struct {
	sync.RWMutex
	IndexSet   map[string]string `json:"data_set"`
	ApplyIndex uint64
}

func (s *FileIndexStore) Set(key string, value string, index uint64) {
	s.Lock()
	defer s.Unlock()
	s.IndexSet[key] = value
	s.ApplyIndex = index
}

func (s *FileIndexStore) makeSnapshot() []byte {
	s.RLock()
	defer s.RUnlock()
	data, _ := json.Marshal(s)
	return data
}

func (s *FileIndexStore) getApplyIndex() uint64 {
	s.RLock()
	defer s.RUnlock()
	return s.ApplyIndex
}

type Serv struct {
	Dir        string
	node       *eraft.Node
	indexStore *FileIndexStore
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

func (s *Serv) downloadFile(request SyncFileRequest) error {
	filename := filepath.Join(s.Dir, request.Filename)
	if err := ioutil.WriteFile(filename, []byte(request.Data), 0666); err != nil {
		panic(err.Error())
	}
	return nil
}

func (s *Serv) applyEntries(entries []eraft.ApplyEntry) {
	for _, entry := range entries {
		if entry.Index <= s.applyIndex() {
			continue
		}
		var req SyncFileRequest
		if err := json.Unmarshal(entry.Data, &req); err != nil {
			log.Panic(err)
		}
		if err := s.downloadFile(req); err != nil {
			panic(err)
		}
		s.indexStore.Set(req.Filename, req.Data, entry.Index)
	}
}

func (s *Serv) applySnapshot(snapshot eraft.ApplySnapshot) {
	var store FileIndexStore
	if err := json.Unmarshal(snapshot.Data, &store); err != nil {
		snapshot.Done(err)
		fmt.Println(err.Error())
	} else {
		snapshot.Done(nil)
	}
	s.indexStore = &store
}

func (s *Serv) applySendSnapshot(send eraft.SendSnapshot) {
	log.Panic("no reject")
}

func (s *Serv) createSnapshot(snap eraft.CreateSnapshot) {
	data := s.indexStore.makeSnapshot()
	if err := snap.Create(data); err != nil {
		panic(err)
	}
}

func (s *Serv) applyIndex() uint64 {
	return s.indexStore.getApplyIndex()
}
