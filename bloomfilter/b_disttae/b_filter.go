package disttae

import (
	"context"
	"sds_use/bloomfilter/b_disttae/logtailreplay"
	"sds_use/bloomfilter/c_vm_tae/index"
	objectio "sds_use/bloomfilter/d_objectio"
	"sds_use/bloomfilter/e_fileservice"
	"sds_use/bloomfilter/z_containers/types"
	"sds_use/bloomfilter/z_containers/vector"
	"sds_use/bloomfilter/z_pb/plan"
	"sds_use/bloomfilter/z_pb/timestamp"
	"sds_use/bloomfilter/z_vm/process"
	"sort"
)

type FastFilterOp func(objectio.ObjectStats) (bool, error)
type LoadOp = func(
	context.Context, objectio.ObjectStats, objectio.ObjectMeta, objectio.BloomFilter,
) (objectio.ObjectMeta, objectio.BloomFilter, error)
type ObjectFilterOp func(objectio.ObjectMeta, objectio.BloomFilter) (bool, error)
type SeekFirstBlockOp func(objectio.ObjectDataMeta) int
type BlockFilterOp func(int, objectio.BlockObject, objectio.BloomFilter) (bool, bool, error)
type LoadOpFactory func(fileservice.FileService) LoadOp

var loadMetadataAndBFOpFactory LoadOpFactory
var loadMetadataOnlyOpFactory LoadOpFactory

func CompileFilterExpr(
	expr *plan.Expr,
	proc *process.Process,
	tableDef *plan.TableDef,
	fs fileservice.FileService,
) (
	fastFilterOp FastFilterOp,
	loadOp LoadOp,
	objectFilterOp ObjectFilterOp,
	blockFilterOp BlockFilterOp,
	seekOp SeekFirstBlockOp,
	canCompile bool,
) {
	canCompile = true
	if expr == nil {
		return
	}

	var exprImpl *plan.Expr_F // todo: populate this correctly
	switch exprImpl.F.Func.ObjName {
	case "prefix_eq":
		colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
		if !ok {
			canCompile = false
			return
		}
		colDef := getColDefByName(colExpr.Col.Name, tableDef)
		_, isSorted := isSortedKey(colDef)
		if isSorted {
			fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
				if obj.ZMIsEmpty() {
					return true, nil
				}
				return obj.SortKeyZoneMap().PrefixEq(vals[0]), nil
			}
		}
		loadOp = loadMetadataOnlyOpFactory(fs)
		seqNum := colDef.Seqnum
		objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
			if isSorted {
				return true, nil
			}
			dataMeta := meta.MustDataMeta()
			return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixEq(vals[0]), nil
		}
		blockFilterOp = func(
			_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
		) (bool, bool, error) {
			// TODO: define canQuickBreak
			return false, blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixEq(vals[0]), nil
		}
		// TODO: define seekOp
	case "=":
		colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
		if !ok {
			canCompile = false
			return
		}
		colDef := getColDefByName(colExpr.Col.Name, tableDef)
		isPK, isSorted := isSortedKey(colDef)
		if isSorted {
			fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
				if obj.ZMIsEmpty() {
					return true, nil
				}
				return obj.SortKeyZoneMap().ContainsKey(vals[0]), nil
			}
		}
		if isPK {
			loadOp = loadMetadataAndBFOpFactory(fs)
		} else {
			loadOp = loadMetadataOnlyOpFactory(fs)
		}

		seqNum := colDef.Seqnum
		objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
			if isSorted {
				return true, nil
			}
			dataMeta := meta.MustDataMeta()
			return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().ContainsKey(vals[0]), nil
		}
		blockFilterOp = func(
			blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
		) (bool, bool, error) {
			var (
				can, ok bool
			)
			zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
			if isSorted {
				can = !zm.AnyLEByValue(vals[0])
				if can {
					ok = false
				} else {
					ok = zm.ContainsKey(vals[0])
				}
			} else {
				can = false
				ok = zm.ContainsKey(vals[0])
			}
			if !ok {
				return can, ok, nil
			}
			if isPK {
				blkBf := bf.GetBloomFilter(uint32(blkIdx))
				blkBfIdx := index.NewEmptyBinaryFuseFilter()
				if err := index.DecodeBloomFilter(blkBfIdx, blkBf); err != nil {
					return false, false, err
				}
				exist, err := blkBfIdx.MayContainsKey(vals[0])
				if err != nil || !exist {
					return false, false, err
				}
			}
			return false, true, nil
		}
		if isSorted {
			seekOp = func(meta objectio.ObjectDataMeta) int {
				blockCnt := int(meta.BlockCount())
				blkIdx := sort.Search(blockCnt, func(j int) bool {
					return meta.GetBlockMeta(uint32(j)).MustGetColumn(uint16(seqNum)).ZoneMap().AnyGEByValue(vals[0])
				})
				return blkIdx
			}
		}
	case "prefix_in":
		colExpr, val, ok := mustColVecValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
		if !ok {
			canCompile = false
			return
		}
		vec := vector.NewVec(types.T_any.ToType())
		_ = vec.UnmarshalBinary(val)
		colDef := getColDefByName(colExpr.Col.Name, tableDef)
		_, isSorted := isSortedKey(colDef)
		if isSorted {
			fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
				if obj.ZMIsEmpty() {
					return true, nil
				}
				return obj.SortKeyZoneMap().PrefixIn(vec), nil
			}
		}
		loadOp = loadMetadataOnlyOpFactory(fs)

		seqNum := colDef.Seqnum
		objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
			if isSorted {
				return true, nil
			}
			dataMeta := meta.MustDataMeta()
			return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixIn(vec), nil
		}
		blockFilterOp = func(
			_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
		) (bool, bool, error) {
			// TODO: define canQuickBreak
			if !blkMeta.IsEmpty() && !blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixIn(vec) {
				return false, false, nil
			}
			return false, true, nil
		}
	case "in":
		colExpr, val, ok := mustColVecValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
		if !ok {
			canCompile = false
			return
		}
		vec := vector.NewVec(types.T_any.ToType())
		_ = vec.UnmarshalBinary(val)
		colDef := getColDefByName(colExpr.Col.Name, tableDef)
		isPK, isSorted := isSortedKey(colDef)
		if isSorted {
			fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
				if obj.ZMIsEmpty() {
					return true, nil
				}
				return obj.SortKeyZoneMap().AnyIn(vec), nil
			}
		}
		if isPK {
			loadOp = loadMetadataAndBFOpFactory(fs)
		} else {
			loadOp = loadMetadataOnlyOpFactory(fs)
		}

		seqNum := colDef.Seqnum
		objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
			if isSorted {
				return true, nil
			}
			dataMeta := meta.MustDataMeta()
			return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyIn(vec), nil
		}
		blockFilterOp = func(
			blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
		) (bool, bool, error) {
			// TODO: define canQuickBreak
			if !blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyIn(vec) {
				return false, false, nil
			}
			if isPK {
				blkBf := bf.GetBloomFilter(uint32(blkIdx))
				blkBfIdx := index.NewEmptyBinaryFuseFilter()
				if err := index.DecodeBloomFilter(blkBfIdx, blkBf); err != nil {
					return false, false, err
				}
				if exist := blkBfIdx.MayContainsAny(vec); !exist {
					return false, false, nil
				}
			}
			return false, true, nil
		}
	default:
		canCompile = false
	}

	return
}

func CompileFilterExprs(
	exprs []*plan.Expr,
	proc *process.Process,
	tableDef *plan.TableDef,
	fs fileservice.FileService,
) (
	fastFilterOp FastFilterOp,
	loadOp LoadOp,
	objectFilterOp ObjectFilterOp,
	blockFilterOp BlockFilterOp,
	seekOp SeekFirstBlockOp,
	canCompile bool,
) {
	canCompile = true
	if len(exprs) == 0 {
		return
	}
	if len(exprs) == 1 {
		return CompileFilterExpr(exprs[0], proc, tableDef, fs)
	}
	ops1 := make([]FastFilterOp, 0, len(exprs))
	ops2 := make([]LoadOp, 0, len(exprs))
	ops3 := make([]ObjectFilterOp, 0, len(exprs))
	ops4 := make([]BlockFilterOp, 0, len(exprs))
	ops5 := make([]SeekFirstBlockOp, 0, len(exprs))

	for _, expr := range exprs {
		expr_op1, expr_op2, expr_op3, expr_op4, expr_op5, can := CompileFilterExpr(expr, proc, tableDef, fs)
		if !can {
			return nil, nil, nil, nil, nil, false
		}
		if expr_op1 != nil {
			ops1 = append(ops1, expr_op1)
		}
		if expr_op2 != nil {
			ops2 = append(ops2, expr_op2)
		}
		if expr_op3 != nil {
			ops3 = append(ops3, expr_op3)
		}
		if expr_op4 != nil {
			ops4 = append(ops4, expr_op4)
		}
		if expr_op5 != nil {
			ops5 = append(ops5, expr_op5)
		}
	}
	fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
		for _, op := range ops1 {
			ok, err := op(obj)
			if err != nil || !ok {
				return ok, err
			}
		}
		return true, nil
	}
	loadOp = func(
		ctx context.Context,
		obj objectio.ObjectStats,
		inMeta objectio.ObjectMeta,
		inBF objectio.BloomFilter,
	) (meta objectio.ObjectMeta, bf objectio.BloomFilter, err error) {
		for _, op := range ops2 {
			if meta != nil && bf != nil {
				continue
			}
			if meta, bf, err = op(ctx, obj, meta, bf); err != nil {
				return
			}
		}
		return
	}
	objectFilterOp = func(meta objectio.ObjectMeta, bf objectio.BloomFilter) (bool, error) {
		for _, op := range ops3 {
			ok, err := op(meta, bf)
			if !ok || err != nil {
				return ok, err
			}
		}
		return true, nil
	}
	blockFilterOp = func(
		blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
	) (bool, bool, error) {
		ok := true
		for _, op := range ops4 {
			thisCan, thisOK, err := op(blkIdx, blkMeta, bf)
			if err != nil {
				return false, false, err
			}
			if thisCan {
				return true, false, nil
			}
			ok = ok && thisOK
		}
		return false, ok, nil
	}

	seekOp = func(obj objectio.ObjectDataMeta) int {
		var pos int
		for _, op := range ops5 {
			pos2 := op(obj)
			if pos2 > pos {
				pos = pos2
			}
		}
		return pos
	}
	return
}

func TryFastFilterBlocks(
	snapshotTS timestamp.Timestamp,
	tableDef *plan.TableDef,
	exprs []*plan.Expr,
	snapshot *logtailreplay.PartitionState,
	uncommittedObjects []objectio.ObjectStats,
	dirtyBlocks map[types.Blockid]struct{},
	outBlocks *objectio.BlockInfoSlice,
	fs fileservice.FileService,
	proc *process.Process,
) (ok bool, err error) {
	fastFilterOp, loadOp, objectFilterOp, blockFilterOp, seekOp, ok := CompileFilterExprs(exprs, proc, tableDef, fs)
	if !ok {
		return false, nil
	}
	err = ExecuteBlockFilter(
		snapshotTS,
		fastFilterOp,
		loadOp,
		objectFilterOp,
		blockFilterOp,
		seekOp,
		snapshot,
		uncommittedObjects,
		dirtyBlocks,
		outBlocks,
		fs,
		proc,
	)
	return true, err
}

func ExecuteBlockFilter(
	snapshotTS timestamp.Timestamp,
	fastFilterOp FastFilterOp,
	loadOp LoadOp,
	objectFilterOp ObjectFilterOp,
	blockFilterOp BlockFilterOp,
	seekOp SeekFirstBlockOp,
	snapshot *logtailreplay.PartitionState,
	uncommittedObjects []objectio.ObjectStats,
	dirtyBlocks map[types.Blockid]struct{},
	outBlocks *objectio.BlockInfoSlice,
	fs fileservice.FileService,
	proc *process.Process,
) (err error) {

	hasDeletes := len(dirtyBlocks) > 0
	err = ForeachSnapshotObjects(
		snapshotTS,
		func(obj logtailreplay.ObjectInfo, isCommitted bool) (err2 error) {
			var ok bool
			objStats := obj.ObjectStats
			if fastFilterOp != nil {
				if ok, err2 = fastFilterOp(objStats); err2 != nil || !ok {
					return
				}
			}
			var (
				meta objectio.ObjectMeta
				bf   objectio.BloomFilter
			)
			if loadOp != nil {
				if meta, bf, err2 = loadOp(
					proc.Ctx, objStats, meta, bf,
				); err2 != nil {
					return
				}
			}
			if objectFilterOp != nil {
				if ok, err2 = objectFilterOp(meta, bf); err2 != nil || !ok {
					return
				}
			}
			var dataMeta objectio.ObjectDataMeta
			if meta != nil {
				dataMeta = meta.MustDataMeta()
			}
			var blockCnt int
			if dataMeta != nil {
				blockCnt = int(dataMeta.BlockCount())
			} else {
				blockCnt = int(objStats.BlkCnt())
			}

			name := objStats.ObjectName()
			extent := objStats.Extent()

			var pos int
			if seekOp != nil {
				pos = seekOp(dataMeta)
			}

			for ; pos < blockCnt; pos++ {
				var blkMeta objectio.BlockObject
				if dataMeta != nil && blockFilterOp != nil {
					var (
						quickBreak, ok2 bool
					)
					blkMeta = dataMeta.GetBlockMeta(uint32(pos))
					if quickBreak, ok2, err2 = blockFilterOp(pos, blkMeta, bf); err2 != nil {
						return

					}
					// skip the following block checks
					if quickBreak {
						break
					}
					// skip this block
					if !ok2 {
						continue
					}
				}
				var rows = blkMeta.GetRows()
				loc := objectio.BuildLocation(name, extent, rows, uint16(pos))
				blk := objectio.BlockInfo{
					BlockID: *objectio.BuildObjectBlockid(name, uint16(pos)),
					MetaLoc: objectio.ObjectLocation(loc),
				}

				blk.Sorted = obj.Sorted
				blk.EntryState = obj.EntryState
				blk.CommitTs = obj.CommitTS

				if hasDeletes {
					if _, ok := dirtyBlocks[blk.BlockID]; !ok {
						blk.CanRemote = true
					}
					blk.PartitionNum = -1
					outBlocks.AppendBlockInfo(blk)
					continue
				}
				// store the block in ranges
				blk.CanRemote = true
				blk.PartitionNum = -1
				outBlocks.AppendBlockInfo(blk)
			}

			return
		},
		snapshot,
		uncommittedObjects...,
	)
	return
}

//-----------------------------------------

func mustColConstValueFromBinaryFuncExpr(
	expr *plan.Expr_F, tableDef *plan.TableDef, proc *process.Process,
) (*plan.Expr_Col, [][]byte, bool) {
	panic("")
}

func getColDefByName(name string, tableDef *plan.TableDef) *plan.ColDef {
	panic("")
	//idx := strings.Index(name, ".")
	//var pos int32
	//if idx >= 0 {
	//	subName := name[idx+1:]
	//	pos = tableDef.Name2ColIndex[subName]
	//} else {
	//	pos = tableDef.Name2ColIndex[name]
	//}
	//return tableDef.Cols[pos]
}

func isSortedKey(colDef *plan.ColDef) (isPK, isSorted bool) {
	//if colDef.Name == catalog.FakePrimaryKeyColName {
	//	return false, false
	//}
	//isPK, isCluster := colDef.Primary, colDef.ClusterBy
	//isSorted = isPK || isCluster
	return
}

func mustColVecValueFromBinaryFuncExpr(
	expr *plan.Expr_F, tableDef *plan.TableDef, proc *process.Process,
) (*plan.Expr_Col, []byte, bool) {
	panic("")
}
