package colexec

import (
	"sds_use/bloomfilter/containers/vector"
	"sds_use/bloomfilter/objectio"
	"sds_use/bloomfilter/pb/plan"
	"sds_use/bloomfilter/process"
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
