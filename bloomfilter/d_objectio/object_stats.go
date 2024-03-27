package objectio

import (
	types "sds_use/bloomfilter/z_containers/types"
)

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

func (des *ObjectStats) Extent() Extent {
	panic("any")
}

func (des *ObjectStats) Rows() int {
	return 0
}

func (des *ObjectStats) ZMIsEmpty() bool {
	//return bytes.Equal(des[zoneMapOffset:zoneMapOffset+zoneMapLen],
	//	ZeroObjectStats[zoneMapOffset:zoneMapOffset+zoneMapLen])
	return true
}

func (des *ObjectStats) SortKeyZoneMap() ZoneMap {
	return ZoneMap(des[0 : 0+10])
}

func BuildObjectBlockid(name ObjectName, u uint16) *types.Blockid {
	panic("")
}
