package index

import (
	"sds_use/hyperloglog/d_containers"
)

type StaticFilter interface {
	MayContainsKey(key []byte) (bool, error)
	Marshal() ([]byte, error)
	Unmarshal(buf []byte) error
	String() string
}

func NewBinaryFuseFilter(data containers.Vector) (StaticFilter, error) {
	panic("implement me")
}
