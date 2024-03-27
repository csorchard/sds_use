package client

import "sds_use/bloomfilter/z_pb/timestamp"

type TxnOperator interface {
	SnapshotTS() timestamp.Timestamp
}
