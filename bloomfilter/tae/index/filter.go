package index

import (
	"sds_use/bloomfilter/containers/vector"
)

type StaticFilter interface {
	MayContainsKey(key []byte) (bool, error)
	MayContainsAny(keys *vector.Vector) bool
	Marshal() ([]byte, error)
	Unmarshal(buf []byte) error
	String() string
}

func NewEmptyBinaryFuseFilter() StaticFilter {
	return nil
}

func DecodeBloomFilter(sf StaticFilter, data []byte) error {
	if err := sf.Unmarshal(data); err != nil {
		return err
	}
	return nil
}
