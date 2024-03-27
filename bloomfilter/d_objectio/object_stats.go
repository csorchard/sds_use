package objectio

import types "sds_use/hyperloglog/e_types"

const (
	ObjectStatsLen = 10
)

type ObjectStats [ObjectStatsLen]byte

func (des *ObjectStats) BlkCnt() uint32 {
	return types.DecodeUint32(des[0 : 0+10])
}

func (des *ObjectStats) ObjectName() ObjectName {
	return ObjectName(des[0 : 0+10])
}
