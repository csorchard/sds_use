package plan

type TableDef struct {
	Pkey *PrimaryKeyDef
}

type PrimaryKeyDef struct {
	CompPkeyCol *ColDef
}

type ColDef struct {
	Seqnum uint32
}

type Expr interface {
	IsExpr_Expr() any
	MarshalTo([]byte) (int, error)
	ProtoSize() int
}

type Expr_F struct {
	F *Function
}

type Expr_Col struct {
	Col *Column
}

type Column struct {
	Name string
}

type Function struct {
	Func ObjectRef
}

type ObjectRef struct {
	ObjName string
}

type Node struct {
	BlockFilterList []*Expr
}
