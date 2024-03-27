package plan

type TableDef struct {
	Pkey *PrimaryKeyDef
}

type PrimaryKeyDef struct {
	CompPkeyCol *ColDef
}

type ColDef struct {
}

type Expr struct {
}
