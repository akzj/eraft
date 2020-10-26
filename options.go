package eraft

import (
	"go.uber.org/zap"
	"path/filepath"
	"time"
)

type Options struct {
	Host                   string              `json:"host"`
	NodeName               string              `json:"node_name"`
	ClusterName            string              `json:"cluster_name"`
	WalDir                 string              `json:"wal_dir"`
	SnapDir                string              `json:"snap_dir"`
	MemberDir              string              `json:"member_dir"`
	SnapshotCount          uint64              `json:"snapshot_count"`
	SnapshotCatchUpEntries uint64              `json:"snapshot_catch_up_entries"`
	QueueCap               int                 `json:"queue_cap"`
	ElectionTick           int                 `json:"election_tick"`
	HeartbeatTick          int                 `json:"heartbeat_tick"`
	PreVote                bool                `json:"pre_vote"`
	MaxInflightMsgs        int                 `json:"max_inflight_msgs"`
	MaxSizePerMsg          uint64              `json:"max_size_per_msg"`
	MembersUrlMap          map[string][]string `json:"members_url_map"`
	TickMs                 time.Duration       `json:"tick_ms"`
	Logger                 *zap.Logger         `json:"-"`
	HijackReceiveSnapshot  func() error        `json:"-"`
	HijackSendSnapshot     func() error        `json:"-"`
	RecoveryFromSnapshot   func(ApplySnapshot) `json:"-"`
}

func DefaultOptionWithPath(path string) Options {
	return Options{
		WalDir:                 filepath.Join(path, "wal"),
		SnapDir:                filepath.Join(path, "snap"),
		MemberDir:              filepath.Join(path, "members"),
		Host:                   "127.0.0.1",
		SnapshotCount:          500000,
		SnapshotCatchUpEntries: 10000,
		QueueCap:               1024,
		ElectionTick:           10,
		HeartbeatTick:          1,
		PreVote:                false,
		MaxInflightMsgs:        10000,
		MaxSizePerMsg:          4096 * 10240,
		ClusterName:            "cluster1",
		MembersUrlMap:          nil,
		Logger:                 zap.NewExample(zap.AddCaller(), zap.Development()),
		TickMs:                 time.Millisecond * 100,
	}
}

func (options Options) WithNodeName(val string) Options {
	options.NodeName = val
	return options
}
func (options Options) WithClusterName(val string) Options {
	options.ClusterName = val
	return options
}
func (options Options) WithWalDir(val string) Options {
	options.WalDir = val
	return options
}
func (options Options) WithSnapDir(val string) Options {
	options.SnapDir = val
	return options
}
func (options Options) WithMemberDir(val string) Options {
	options.MemberDir = val
	return options
}
func (options Options) WithSnapshotCount(val uint64) Options {
	options.SnapshotCount = val
	return options
}
func (options Options) WithSnapshotCatchUpEntries(val uint64) Options {
	options.SnapshotCatchUpEntries = val
	return options
}
func (options Options) WithQueueCap(val int) Options {
	options.QueueCap = val
	return options
}
func (options Options) WithElectionTick(val int) Options {
	options.ElectionTick = val
	return options
}
func (options Options) WithHeartbeatTick(val int) Options {
	options.HeartbeatTick = val
	return options
}
func (options Options) WithPreVote(val bool) Options {
	options.PreVote = val
	return options
}
func (options Options) WithMaxInflightMsgs(val int) Options {
	options.MaxInflightMsgs = val
	return options
}
func (options Options) WithMaxSizePerMsg(val uint64) Options {
	options.MaxSizePerMsg = val
	return options
}
func (options Options) WithMembersUrlMap(val map[string][]string) Options {
	options.MembersUrlMap = val
	return options
}
func (options Options) WithLogger(val *zap.Logger) Options {
	options.Logger = val
	return options
}
func (options Options) WithHijackReceiveSnapshot(val func() error) Options {
	options.HijackReceiveSnapshot = val
	return options
}

func (options Options) WithHijackSendSnapshot(val func() error) Options {
	options.HijackSendSnapshot = val
	return options
}
func (options Options) WithRecoveryFromSnapshot(val func(ApplySnapshot)) Options {
	options.RecoveryFromSnapshot = val
	return options
}

func (options Options) WithTickMs(val time.Duration) Options {
	options.TickMs = val
	return options
}
