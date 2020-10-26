package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/azkj/eraft"
	"github.com/gin-gonic/gin"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type SetReq struct {
	ID    uint64 `json:"id"`
	Key   string `json:"key" uri:"key" binding:"required"`
	Value string `json:"value" uri:"value" binding:"required"`
}

type Store struct {
	sync.RWMutex
	DataSet    map[string]string `json:"data_set"`
	ApplyIndex uint64
}

func NewStore() *Store {
	return &Store{
		RWMutex:    sync.RWMutex{},
		DataSet:    map[string]string{},
		ApplyIndex: 0,
	}
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

func (s *Store) Get(key string) (string, error) {
	s.RLock()
	defer s.RUnlock()
	val, ok := s.DataSet[key]
	if ok == false {
		return "", fmt.Errorf("no find")
	}
	return val, nil
}

type Config struct {
	Path          string              `json:"path"`
	Port          int                 `json:"port"`
	ClusterName   string              `json:"cluster_name"`
	NodeName      string              `json:"node_name"`
	MemberUrlsMap map[string][]string `json:"member_urls_map"`
}

type Serv struct {
	engine *gin.Engine
	node   *eraft.Node
	store  *Store
	reqID  uint64
	config Config
}

func NewServ(config Config) *Serv {
	serv := &Serv{
		config: config,
		store:  NewStore(),
		engine: gin.New(),
		reqID:  uint64(int(time.Now().Unix()) << 32),
	}
	serv.node = eraft.StartNode(eraft.DefaultOptionWithPath(filepath.Join(config.Path, config.NodeName)).
		WithNodeName(config.NodeName).
		//WithTickMs(time.Second * 5).
		//WithTickMs(time.Second * 5).
		WithClusterName(config.ClusterName).
		WithMembersUrlMap(config.MemberUrlsMap).
		WithRecoveryFromSnapshot(func(snapshot eraft.ApplySnapshot) {
			serv.applySnapshot(snapshot)
		}))

	return serv
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
		//fmt.Println(string(entry.Data))
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

func (s *Serv) Serve() {
	s.startRaftNode()
	s.engine.GET("/get/:key", func(c *gin.Context) {
		val, err := s.store.Get(c.Param("key"))
		if err != nil {
			c.String(404, err.Error())
			return
		}
		c.String(200, val)
	})

	s.engine.GET("/set/:key/:value", func(c *gin.Context) {
		var req SetReq
		req.Key = c.Param("key")
		req.Value = c.Param("value")
		req.ID = s.getID()
		data, _ := json.Marshal(req)
		if err := s.node.Propose(c.Request.Context(), data); err != nil {
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
	})
	if err := s.engine.Run("127.0.0.1:" + strconv.Itoa(s.config.Port)); err != nil {
		log.Panic(err)
	}
}

func (s *Serv) getID() uint64 {
	return atomic.AddUint64(&s.reqID, 1)
}

func main() {
	var conf string
	var dumpConf string
	flag.StringVar(&conf, "c", "", "-c config.json")
	flag.StringVar(&dumpConf, "d", "", "-d `to dump default config`")

	flag.Parse()

	if dumpConf != "" {
		data, _ := json.Marshal(Config{
			Path:        "data",
			Port:        5000,
			ClusterName: "cluster1",
			NodeName:    "node1",
			MemberUrlsMap: map[string][]string{
				"node1": {"http://127.0.0.1:5001"},
				"node2": {"http://127.0.0.1:6001"},
				"node3": {"http://127.0.0.1:7001"},
			},
		})
		var buffer bytes.Buffer
		_ = json.Indent(&buffer, data, "", "    ")
		if err := ioutil.WriteFile(dumpConf, buffer.Bytes(), 0666); err != nil {
			panic(err)
		}
		return
	}

	var config Config
	if data, err := ioutil.ReadFile(conf); err != nil {
		panic(err)
	} else {
		if err := json.Unmarshal(data, &config); err != nil {
			panic(err)
		}
		if config.Port == 0 {

		}
	}

	NewServ(config).Serve()
}
