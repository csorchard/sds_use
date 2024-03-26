package index

import "sds_use/bloomfilter/containers/vector"

type ZM []byte

func (zm ZM) AnyGEByValue(k []byte) bool {
	return true
}

func (zm ZM) AnyLEByValue(k []byte) bool {
	return true
}

func (zm ZM) AnyIn(vec *vector.Vector) bool {
	return true
}

func (zm ZM) ContainsKey(k []byte) bool {
	return true
}
