package eraft

import (
	"go.uber.org/zap"
	"time"
)

type Options struct {
	ClusterID              uint64              `json:"cluster_id"`
	NodeID                 uint64              `json:"node_id"`
	WalDir                 string              `json:"wal_dir"`
	SnapDir                string              `json:"snap_dir"`
	MemberDir              string              `json:"member_dir"`
	TickInterval           time.Duration       `json:"tick_interval"`
	SnapshotCount          uint64              `json:"snapshot-count"`
	SnapshotCatchUpEntries uint64              `json:"snapshot_catch_up_entries"`
	QueueCap               int                 `json:"queue_cap"`
	ElectionTick           int                 `json:"election_tick"`
	HeartbeatTick          int                 `json:"heartbeat_tick"`
	PreVote                bool                `json:"pre_vote"`
	MaxInflightMsgs        int                 `json:"max_inflight_msgs"`
	MaxSizePerMsg          uint64              `json:"max_size_per_msg"`
	ClusterName            string              `json:"cluster_name"`
	MembersUrlMap          map[string][]string `json:"members_url_map"`
	Logger                 *zap.Logger         `json:"-"`
	HijackReceiveSnapshot  func() error        `json:"-"`
	HijackSendSnapshot     func() error        `json:"-"`
	RecoveryFromSnapshot   func(ApplySnapshot) `json:"-"`
}
