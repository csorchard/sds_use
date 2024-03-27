package disttae

import (
	"context"
	"sds_use/bloomfilter/containers/types"
	"sds_use/bloomfilter/containers/vector"
	"sds_use/bloomfilter/disttae/logtailreplay"
	"sds_use/bloomfilter/fileservice"
	"sds_use/bloomfilter/pb/plan"
	"sds_use/bloomfilter/pb/timestamp"
	"sds_use/bloomfilter/process"
	"sds_use/bloomfilter/tae/index"
	objectio "sds_use/hyperloglog/b_objectio"
	"sort"
)

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
	switch exprImpl := expr.Expr.(type) {
	// case *plan.Expr_Lit:
	// case *plan.Expr_Col:
	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "or":
			leftFastOp, leftLoadOp, leftObjectOp, leftBlockOp, leftSeekOp, leftCan := CompileFilterExpr(
				exprImpl.F.Args[0], proc, tableDef, fs,
			)
			if !leftCan {
				return nil, nil, nil, nil, nil, false
			}
			rightFastOp, rightLoadOp, rightObjectOp, rightBlockOp, rightSeekOp, rightCan := CompileFilterExpr(
				exprImpl.F.Args[1], proc, tableDef, fs,
			)
			if !rightCan {
				return nil, nil, nil, nil, nil, false
			}
			if leftFastOp != nil || rightFastOp != nil {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if leftFastOp != nil {
						if ok, err := leftFastOp(obj); ok || err != nil {
							return ok, err
						}
					}
					if rightFastOp != nil {
						return rightFastOp(obj)
					}
					return true, nil
				}
			}
			if leftLoadOp != nil || rightLoadOp != nil {
				loadOp = func(
					ctx context.Context,
					obj objectio.ObjectStats,
					inMeta objectio.ObjectMeta,
					inBF objectio.BloomFilter,
				) (meta objectio.ObjectMeta, bf objectio.BloomFilter, err error) {
					if leftLoadOp != nil {
						if meta, bf, err = leftLoadOp(ctx, obj, inMeta, inBF); err != nil {
							return
						}
						inMeta = meta
						inBF = bf
					}
					if rightLoadOp != nil {
						meta, bf, err = rightLoadOp(ctx, obj, inMeta, inBF)
					}
					return
				}
			}
			if leftObjectOp != nil || rightLoadOp != nil {
				objectFilterOp = func(meta objectio.ObjectMeta, bf objectio.BloomFilter) (bool, error) {
					if leftObjectOp != nil {
						if ok, err := leftObjectOp(meta, bf); ok || err != nil {
							return ok, err
						}
					}
					if rightObjectOp != nil {
						return rightObjectOp(meta, bf)
					}
					return true, nil
				}

			}
			if leftBlockOp != nil || rightBlockOp != nil {
				blockFilterOp = func(
					blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
				) (bool, bool, error) {
					can := true
					ok := false
					if leftBlockOp != nil {
						if thisCan, thisOK, err := leftBlockOp(blkIdx, blkMeta, bf); err != nil {
							return false, false, err
						} else {
							ok = ok || thisOK
							can = can && thisCan
						}
					}
					if rightBlockOp != nil {
						if thisCan, thisOK, err := rightBlockOp(blkIdx, blkMeta, bf); err != nil {
							return false, false, err
						} else {
							ok = ok || thisOK
							can = can && thisCan
						}
					}
					return can, ok, nil
				}
			}
			if leftSeekOp != nil || rightBlockOp != nil {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					var pos int
					if leftSeekOp != nil {
						pos = leftSeekOp(meta)
					}
					if rightSeekOp != nil {
						pos2 := rightSeekOp(meta)
						if pos2 < pos {
							pos = pos2
						}
					}
					return pos
				}
			}
		case "and":
			leftFastOp, leftLoadOp, leftObjectOp, leftBlockOp, leftSeekOp, leftCan := CompileFilterExpr(
				exprImpl.F.Args[0], proc, tableDef, fs,
			)
			if !leftCan {
				return nil, nil, nil, nil, nil, false
			}
			rightFastOp, rightLoadOp, rightObjectOp, rightBlockOp, rightSeekOp, rightCan := CompileFilterExpr(
				exprImpl.F.Args[1], proc, tableDef, fs,
			)
			if !rightCan {
				return nil, nil, nil, nil, nil, false
			}
			if leftFastOp != nil || rightFastOp != nil {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if leftFastOp != nil {
						if ok, err := leftFastOp(obj); !ok || err != nil {
							return ok, err
						}
					}
					if rightFastOp != nil {
						return rightFastOp(obj)
					}
					return true, nil
				}
			}
			if leftLoadOp != nil || rightLoadOp != nil {
				loadOp = func(
					ctx context.Context,
					obj objectio.ObjectStats,
					inMeta objectio.ObjectMeta,
					inBF objectio.BloomFilter,
				) (meta objectio.ObjectMeta, bf objectio.BloomFilter, err error) {
					if leftLoadOp != nil {
						if meta, bf, err = leftLoadOp(ctx, obj, inMeta, inBF); err != nil {
							return
						}
						inMeta = meta
						inBF = bf
					}
					if rightLoadOp != nil {
						meta, bf, err = rightLoadOp(ctx, obj, inMeta, inBF)
					}
					return
				}
			}
			if leftObjectOp != nil || rightLoadOp != nil {
				objectFilterOp = func(meta objectio.ObjectMeta, bf objectio.BloomFilter) (bool, error) {
					if leftObjectOp != nil {
						if ok, err := leftObjectOp(meta, bf); !ok || err != nil {
							return ok, err
						}
					}
					if rightObjectOp != nil {
						return rightObjectOp(meta, bf)
					}
					return true, nil
				}

			}
			if leftBlockOp != nil || rightBlockOp != nil {
				blockFilterOp = func(
					blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
				) (bool, bool, error) {
					ok := true
					if leftBlockOp != nil {
						if thisCan, thisOK, err := leftBlockOp(blkIdx, blkMeta, bf); err != nil {
							return false, false, err
						} else {
							if thisCan {
								return true, false, nil
							}
							ok = ok && thisOK
						}
					}
					if rightBlockOp != nil {
						if thisCan, thisOK, err := rightBlockOp(blkIdx, blkMeta, bf); err != nil {
							return false, false, err
						} else {
							if thisCan {
								return true, false, nil
							}
							ok = ok && thisOK
						}
					}
					return false, ok, nil
				}
			}
			if leftSeekOp != nil || rightSeekOp != nil {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					var pos int
					if leftSeekOp != nil {
						pos = leftSeekOp(meta)
					}
					if rightSeekOp != nil {
						pos2 := rightSeekOp(meta)
						if pos2 < pos {
							pos = pos2
						}
					}
					return pos
				}
			}
		case "<=":
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
					return obj.SortKeyZoneMap().AnyLEByValue(vals[0]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLEByValue(vals[0]), nil
			}
			blockFilterOp = func(
				blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				ok := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLEByValue(vals[0])
				if isSorted {
					return !ok, ok, nil
				}
				return false, ok, nil
			}
		case ">=":
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
					return obj.SortKeyZoneMap().AnyGEByValue(vals[0]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGEByValue(vals[0]), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				return false, blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGEByValue(vals[0]), nil
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
		case ">":
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
					return obj.SortKeyZoneMap().AnyGTByValue(vals[0]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGTByValue(vals[0]), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				return false, blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGTByValue(vals[0]), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					blockCnt := int(meta.BlockCount())
					blkIdx := sort.Search(blockCnt, func(j int) bool {
						return meta.GetBlockMeta(uint32(j)).MustGetColumn(uint16(seqNum)).ZoneMap().AnyGTByValue(vals[0])
					})
					return blkIdx
				}
			}
		case "<":
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
					return obj.SortKeyZoneMap().AnyLTByValue(vals[0]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLTByValue(vals[0]), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				ok := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLTByValue(vals[0])
				if isSorted {
					return !ok, ok, nil
				}
				return false, ok, nil
			}
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
		case "prefix_between":
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
					return obj.SortKeyZoneMap().PrefixBetween(vals[0], vals[1]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixBetween(vals[0], vals[1]), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				// TODO: define canQuickBreak
				return false, blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixBetween(vals[0], vals[1]), nil
			}
			// TODO: define seekOp
			// ok
		case "between":
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
					return obj.SortKeyZoneMap().Between(vals[0], vals[1]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().Between(vals[0], vals[1]), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				// TODO: define canQuickBreak
				return false, blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().Between(vals[0], vals[1]), nil
			}
			// TODO: define seekOp
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
			// TODO: define seekOp
			// ok
		case "isnull", "is_null":
			colExpr, _, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(colExpr.Col.Name, tableDef)
			fastFilterOp = nil
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).NullCnt() != 0, nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				return false, blkMeta.MustGetColumn(uint16(seqNum)).NullCnt() != 0, nil
			}

			// ok
		case "isnotnull", "is_not_null":
			colExpr, _, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(colExpr.Col.Name, tableDef)
			fastFilterOp = nil
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).NullCnt() < dataMeta.BlockHeader().Rows(), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				return false, blkMeta.MustGetColumn(uint16(seqNum)).NullCnt() < blkMeta.GetRows(), nil
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
		default:
			canCompile = false
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

			if objStats.Rows() == 0 {
				logutil.Fatalf("object stats has zero rows, detail: %s", obj.String())
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
				var rows uint32
				if objRows := objStats.Rows(); objRows != 0 {
					if pos < blockCnt-1 {
						rows = options.DefaultBlockMaxRows
					} else {
						rows = objRows - options.DefaultBlockMaxRows*uint32(pos)
					}
				} else {
					if blkMeta == nil {
						blkMeta = dataMeta.GetBlockMeta(uint32(pos))
					}
					rows = blkMeta.GetRows()
				}
				loc := objectio.BuildLocation(name, extent, rows, uint16(pos))
				blk := objectio.BlockInfo{
					BlockID:   *objectio.BuildObjectBlockid(name, uint16(pos)),
					SegmentID: name.SegmentId(),
					MetaLoc:   objectio.ObjectLocation(loc),
				}

				blk.Sorted = obj.Sorted
				blk.EntryState = obj.EntryState
				blk.CommitTs = obj.CommitTS
				if obj.HasDeltaLoc {
					deltaLoc, commitTs, ok := snapshot.GetBockDeltaLoc(blk.BlockID)
					if ok {
						blk.DeltaLoc = deltaLoc
						blk.CommitTs = commitTs
					}
				}

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
