package engine

import (
	"context"
	"sds_use/bloomfilter/z_pb/plan"
)

type Ranges interface {
	GetBytes(i int) []byte

	Len() int

	Append([]byte)

	Size() int

	SetBytes([]byte)

	GetAllBytes() []byte

	Slice(i, j int) []byte
}

type Node struct {
	Mcpu             int
	Id               string   `json:"id"`
	Addr             string   `json:"address"`
	Data             []byte   `json:"payload"`
	Rel              Relation // local relation
	NeedExpandRanges bool
}

type Relation interface {
	Ranges(context.Context, []*plan.Expr) (Ranges, error)
}
