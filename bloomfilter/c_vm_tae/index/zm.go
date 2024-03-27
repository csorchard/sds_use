package index

import "sds_use/bloomfilter/z_containers/vector"

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

func (zm ZM) PrefixEq(s []byte) bool {
	//zmin := zm.GetMinBuf()
	//zmax := zm.GetMaxBuf()
	//
	//return types.PrefixCompare(zmin, s) <= 0 && types.PrefixCompare(s, zmax) <= 0
	return true
}

func (zm ZM) PrefixIn(vec *vector.Vector) bool {
	return true
}
