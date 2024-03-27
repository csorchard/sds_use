package types

type T uint8

const (
	T_any T = 0
)

type Type struct {
	Oid T
}

func (t T) ToType() Type {
	var typ Type
	return typ
}

const (
	TxnTsSize   = 12
	BlockidSize = 20
)

type TS [TxnTsSize]byte
type Blockid [BlockidSize]byte
