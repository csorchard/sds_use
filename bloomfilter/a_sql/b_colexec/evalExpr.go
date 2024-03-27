package colexec

import (
	"sds_use/bloomfilter/d_objectio"
	"sds_use/bloomfilter/z_containers/vector"
	"sds_use/bloomfilter/z_pb/plan"
	"sds_use/bloomfilter/z_vm/process"
)

func EvaluateFilterByZoneMap(
	proc *process.Process,
	expr *plan.Expr,
	meta objectio.ColumnMetaFetcher,
	columnMap map[int]int,
	zms []objectio.ZoneMap,
	vecs []*vector.Vector) (selected bool) {
	return false
}
