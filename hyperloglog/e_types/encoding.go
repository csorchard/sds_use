package types

import "unsafe"

func EncodeUint32(v *uint32) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), 4)
}

func DecodeUint32(v []byte) uint32 {
	return *(*uint32)(unsafe.Pointer(&v[0]))
}
