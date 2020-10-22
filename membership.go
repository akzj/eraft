package eraft

import (
	"encoding/json"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.uber.org/zap"
	"hash/crc64"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

type Member struct {
	ID   uint64   `json:"id"`
	Name string   `json:"name"`
	URLs []string `json:"urls"`
}

type ConfChangeContext struct {
	IsPromote bool
}

type Membership struct {
	locker      sync.Mutex
	LocalID     uint64             `json:"local_id"`
	ClusterID   uint64             `json:"cluster_id"`
	ClusterName string             `json:"cluster_name"`
	Members     map[uint64]*Member `json:"members"`
}

const memberFilename = "members.json"

func openMembership(logger *zap.Logger, dir string, clusterName string, membersUrlsMap map[string][]string) *Membership {
	if err := fileutil.CreateDirAll(dir); err != nil {
		logger.Fatal("create members dir failed", zap.String("dir", dir), zap.Error(err))
	}
	var membership Membership
	data, err := ioutil.ReadFile(filepath.Join(dir, memberFilename))
	if err != nil {
		if os.IsNotExist(err) == false {
			logger.Fatal("reader file failed",
				zap.String("filepath", filepath.Join(dir, memberFilename)), zap.Error(err))
		}
	} else {
		if err := json.Unmarshal(data, &membership); err != nil {
			logger.Fatal("unmarshal failed", zap.String("data", string(data)), zap.Error(err))
		}
		return &membership
	}

	for nodeName, urls := range membersUrlsMap {
		member := &Member{
			ID:   0,
			Name: nodeName,
			URLs: urls,
		}
		data, _ := json.Marshal(member)
		member.ID = crc64.Checksum(data, crc64.MakeTable(crc64.ISO))
	}
	membership.ClusterName = clusterName
	data, _ = json.Marshal(&membership)
	membership.ClusterID = crc64.Checksum(data, crc64.MakeTable(crc64.ISO))
	return &membership
}

func (m *Membership) IsIDExist(id uint64) bool {
	m.locker.Lock()
	defer m.locker.Unlock()
	_, ok := m.Members[id]
	return ok == false
}

func (m *Membership) GetMembers() []Member {
	m.locker.Lock()
	defer m.locker.Unlock()
	var members []Member
	for _, member := range m.Members {
		members = append(members, Member{
			ID:   member.ID,
			URLs: append([]string{}, member.URLs...),
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
	member, ok := m.Members[id]
	if ok == false {
		return nil
	}
	return member
}

func (m *Membership) GetClusterID() uint64 {
	return m.ClusterID
}

func (m *Membership) GetClusterName() string {
	return m.ClusterName
}
