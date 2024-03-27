package objectio

import "sds_use/bloomfilter/c_vm_tae/index"

type ObjectMeta interface {
	MustDataMeta() ObjectDataMeta
}

type ZoneMap = index.ZM

type ColumnMetaFetcher interface {
	MustGetColumn(seqnum uint16) ColumnMeta
}
