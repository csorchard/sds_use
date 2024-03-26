package objectio

import (
	"sds_use/bloomfilter/containers/types"
	"unsafe"
)

type BlockInfoSlice []byte

func (s *BlockInfoSlice) AppendBlockInfo(info BlockInfo) {
	*s = append(*s, EncodeBlockInfo(info)...)
}

func (s *BlockInfoSlice) Len() int {
	return 0
}

//--------------------------------------------------

var (
	EmptyBlockInfo      = BlockInfo{}
	EmptyBlockInfoBytes = EncodeBlockInfo(EmptyBlockInfo)
)

const (
	BlockInfoSize = int(unsafe.Sizeof(EmptyBlockInfo))
)

func EncodeBlockInfo(info BlockInfo) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&info)), BlockInfoSize)
}

type ObjectLocation [10]byte

type BlockInfo struct {
	BlockID      types.Blockid
	EntryState   bool
	Sorted       bool
	MetaLoc      ObjectLocation
	DeltaLoc     ObjectLocation
	CommitTs     types.TS
	CanRemote    bool
	PartitionNum int
}