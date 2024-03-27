package logtailreplay

import (
	"sds_use/bloomfilter/pb/timestamp"
	"sync/atomic"
)

// a partition corresponds to a dn
type Partition struct {
	lock  chan struct{}
	state atomic.Pointer[PartitionState]
	TS    timestamp.Timestamp // last updated timestamp

	// assuming checkpoints will be consumed once
	checkpointConsumed atomic.Bool
}

func (p *Partition) Snapshot() *PartitionState {
	panic("")
}
