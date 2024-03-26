package vector

import "sds_use/bloomfilter/containers/types"

// Vector represent a column
type Vector struct {
	// type represent the type of column
	typ types.Type
}

func NewVec(typ types.Type) *Vector {
	vec := &Vector{
		typ: typ,
	}

	return vec
}

func (v *Vector) UnmarshalBinary(data []byte) error {
	return nil
}
