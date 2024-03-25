package objectio

import "sds_use/hyperloglog/types"

const (
	ndvOff = 0
	ndvLen = 4

	nullCntOff = ndvOff + ndvLen
	nullCntLen = 4

	colMetaLen = 0
)

func BuildObjectColumnMeta() ColumnMeta {
	var buf [colMetaLen]byte
	return buf[:]
}

type ColumnMeta []byte

func (cm ColumnMeta) SetNdv(cnt uint32) {
	copy(cm[ndvOff:ndvOff+ndvLen], types.EncodeUint32(&cnt))
}

func (cm ColumnMeta) SetNullCnt(cnt uint32) {
	copy(cm[nullCntOff:nullCntOff+nullCntLen], types.EncodeUint32(&cnt))
}

func (cm ColumnMeta) NullCnt() uint32 {
	return types.DecodeUint32(cm[nullCntOff : nullCntOff+nullCntLen])
}
