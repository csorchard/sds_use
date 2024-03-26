package index

import (
	"sds_use/hyperloglog/d_containers"
)

// StaticFilter is a filter that can be used to check if a key may be in a set.
// It is methods of BloomFilter and CuckooFilter.
type StaticFilter interface {
	MayContainsKey(key []byte) (bool, error)
	Marshal() ([]byte, error)
	Unmarshal(buf []byte) error
	String() string
}

func NewBinaryFuseFilter(data containers.Vector) (StaticFilter, error) {
	panic("implement me")
}
