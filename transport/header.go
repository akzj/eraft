package transport

type CompactionType uint32

const NoCompaction = 0

type Header struct {
	MessageCount uint32
	Compaction   CompactionType
}

