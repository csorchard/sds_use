package disttae

import (
	"sds_use/bloomfilter/b_disttae/logtailreplay"
	"sds_use/bloomfilter/d_objectio"
	"sds_use/bloomfilter/z_pb/plan"
	"sds_use/bloomfilter/z_pb/timestamp"
)

func extractPKValueFromEqualExprs(
	def *plan.TableDef,
	exprs []*plan.Expr,
	pkIdx int,
) (val []byte, isVec bool) {
	return
}

func ForeachSnapshotObjects(
	ts timestamp.Timestamp,
	onObject func(obj logtailreplay.ObjectInfo, isCommitted bool) error,
	tableSnapshot *logtailreplay.PartitionState,
	uncommitted ...objectio.ObjectStats,
) (err error) {
	// process all uncommitted objects first
	for _, obj := range uncommitted {
		info := logtailreplay.ObjectInfo{
			ObjectStats: obj,
		}
		if err = onObject(info, false); err != nil {
			return
		}
	}

	// process all committed objects
	if tableSnapshot == nil {
		return
	}

	iter, err := tableSnapshot.NewObjectsIter(types.TimestampToTS(ts))
	if err != nil {
		return
	}
	defer iter.Close()
	for iter.Next() {
		obj := iter.Entry()
		if err = onObject(obj.ObjectInfo, true); err != nil {
			return
		}
	}
	return
}

// ForeachBlkInObjStatsList receives an object info list,
// and visits each blk of these object info by OnBlock,
// until the onBlock returns false or all blks have been enumerated.
// when onBlock returns a false,
// the next argument decides whether continue onBlock on the next stats or exit foreach completely.
func ForeachBlkInObjStatsList(
	next bool,
	dataMeta objectio.ObjectDataMeta,
	onBlock func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool,
	objects ...objectio.ObjectStats,
) {
	stop := false
	objCnt := len(objects)

	for idx := 0; idx < objCnt && !stop; idx++ {
		iter := NewStatsBlkIter(&objects[idx], dataMeta)
		pos := uint32(0)
		for iter.Next() {
			blk := iter.Entry()
			var meta objectio.BlockObject
			if !dataMeta.IsEmpty() {
				meta = dataMeta.GetBlockMeta(pos)
			}
			pos++
			if !onBlock(blk, meta) {
				stop = true
				break
			}
		}

		if stop && next {
			stop = false
		}
	}
}
