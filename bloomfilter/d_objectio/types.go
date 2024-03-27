package objectio

import "sds_use/bloomfilter/c_tae/index"

type ObjectMeta interface {
}

type ZoneMap = index.ZM

type ColumnMetaFetcher interface {
	MustGetColumn(seqnum uint16) ColumnMeta
}
