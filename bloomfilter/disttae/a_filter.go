package disttae

import (
	"context"
	"sds_use/bloomfilter/containers/types"
	"sds_use/bloomfilter/containers/vector"
	"sds_use/bloomfilter/fileservice"
	"sds_use/bloomfilter/pb/plan"
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
