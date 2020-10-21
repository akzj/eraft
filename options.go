package eraft

import "time"

type Options struct {
	ClusterID              ID            `json:"cluster_id"`
	TickInterval           time.Duration `json:"tick_interval"`
	SnapshotCount          uint64        `json:"snapshot-count"`
	SnapshotCatchUpEntries uint64        `json:"snapshot_catch_up_entries"`
}
