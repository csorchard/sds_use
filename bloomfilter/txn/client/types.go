package client

import "sds_use/bloomfilter/pb/timestamp"

type TxnOperator interface {
	SnapshotTS() timestamp.Timestamp
}
