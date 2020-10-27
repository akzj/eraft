package eraft

import (
	"encoding/json"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"hash/crc64"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type Member struct {
	ID    uint64   `json:"id"`
	Name  string   `json:"name"`
	Addrs []string `json:"addrs"`
}

func (m Member) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddUint64("id", m.ID)
	encoder.AddString("name", m.Name)
	encoder.AddString("urls", "["+strings.Join(m.Addrs, "|")+"]")
	return nil
}

type ConfChangeContext struct {
	IsPromote bool   `json:"is_promote"`
	Member    Member `json:"member"`
}

type Membership struct {
	locker      sync.Mutex
	LocalID     uint64    `json:"local_id"`
	ClusterID   uint64    `json:"cluster_id"`
	ClusterName string    `json:"cluster_name"`
	NodeName    string    `json:"node_name"`
	Members     []*Member `json:"members"`
}

const memberFilename = "members.json"

func openMembership(logger *zap.Logger, dir string, clusterName string, nodeName string, membersUrlsMap map[string][]string) *Membership {
	if fileutil.Exist(dir) == false {
		if err := fileutil.CreateDirAll(dir); err != nil {
			logger.Fatal("create members dir failed", zap.String("dir", dir), zap.Error(err))
		}
	}
	var membership = Membership{
		locker:      sync.Mutex{},
		ClusterName: clusterName,
		NodeName:    nodeName,
	}
	filename := filepath.Join(dir, memberFilename)
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) == false {
			logger.Fatal("reader file failed",
				zap.String("filepath", filepath.Join(dir, filename)), zap.Error(err))
		}
	} else {
		if err := json.Unmarshal(data, &membership); err != nil {
			logger.Fatal("unmarshal failed", zap.String("data", string(data)), zap.Error(err))
		}
		return &membership
	}

	for key, addrs := range membersUrlsMap {
		member := &Member{
			ID:    0,
			Name:  key,
			Addrs: addrs,
		}
		data, _ := json.Marshal(member)
		member.ID = crc64.Checksum(data, crc64.MakeTable(crc64.ISO))
		membership.Members = append(membership.Members, member)
		if member.Name == nodeName {
			if membership.LocalID != 0 {
				logger.Fatal("repeated local nodeID",
					zap.Uint64("nodeID", membership.LocalID),
					zap.Uint64("repeated", member.ID))
			}
			membership.LocalID = member.ID
		}
	}
	sort.Slice(membership.Members, func(i, j int) bool {
		return membership.Members[i].Name < membership.Members[j].Name
	})
	if membership.LocalID == 0 {
		panic("localId no find")
	}
	membership.ClusterName = clusterName
	data, _ = json.Marshal(membership.Members)
	membership.ClusterID = crc64.Checksum(data, crc64.MakeTable(crc64.ISO))
	if membership.ClusterID == 0 {
		logger.Fatal("generate clusterID error")
	}
	data, _ = json.Marshal(&membership)
	if err := ioutil.WriteFile(filename, data, 0666); err != nil {
		logger.Fatal("writeFile failed", zap.Error(err))
	}
	return &membership
}

func (m *Membership) IsIDExist(id uint64) bool {
	return m.GetMember(id) != nil
}

func (m *Membership) GetMembers() []Member {
	m.locker.Lock()
	defer m.locker.Unlock()
	var members []Member
	for _, member := range m.Members {
		members = append(members, Member{
			ID:    member.ID,
			Addrs: append([]string{}, member.Addrs...),
		})
	}
	return members
}

func (m *Membership) LocalMember() Member {
	return *m.GetMember(m.LocalID)
}

func (m *Membership) GetMember(id uint64) *Member {
	m.locker.Lock()
	defer m.locker.Unlock()
	for _, member := range m.Members {
		if member.ID == id {
			return member
		}
	}
	return nil
}

func (m *Membership) GetClusterID() uint64 {
	return m.ClusterID
}

func (m *Membership) GetClusterName() string {
	return m.ClusterName
}
