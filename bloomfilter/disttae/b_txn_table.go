package disttae

import (
	"context"
	"sds_use/bloomfilter/containers/types"
	"sds_use/bloomfilter/containers/vector"
	"sds_use/bloomfilter/disttae/logtailreplay"
	"sds_use/bloomfilter/fileservice"
	"sds_use/bloomfilter/objectio"
	"sds_use/bloomfilter/pb/plan"
	"sds_use/bloomfilter/tae/blockio"
	"sds_use/bloomfilter/tae/index"
	"sort"
	"time"
)

type txnTable struct {
	primaryIdx int
	tableDef   *plan.TableDef
	db         *txnDatabase
}

// txn can read :
//  1. snapshot data:
//      1>. committed block data resides in S3.
//      2>. partition state data resides in memory. read by partitionReader.

//      deletes(rowids) for committed block exist in the following four places:
//      1. in delta location formed by TN writing S3. read by blockReader.
//      2. in CN's partition state, read by partitionReader.
//  	3. in txn's workspace(txn.writes) being deleted by txn, read by partitionReader.
//  	4. in delta location being deleted through CN writing S3, read by blockMergeReader.

//  2. data in txn's workspace:
//     1>.Raw batch data resides in t     xn.writes,read by partitionReader.
//     2>.CN blocks resides in S3, read by blockReader.

// return all unmodified blocks
func (tbl *txnTable) Ranges(ctx context.Context, exprs []*plan.Expr) (ranges engine.Ranges, err error) {
	start := time.Now()

	seq := tbl.db.txn.op.NextSequence()
	trace.GetService().AddTxnDurationAction(
		tbl.db.txn.op,
		client.RangesEvent,
		seq,
		tbl.tableId,
		0,
		nil)

	defer func() {
		cost := time.Since(start)

		trace.GetService().AddTxnAction(
			tbl.db.txn.op,
			client.RangesEvent,
			seq,
			tbl.tableId,
			int64(ranges.Len()),
			"blocks",
			err)

		trace.GetService().AddTxnDurationAction(
			tbl.db.txn.op,
			client.RangesEvent,
			seq,
			tbl.tableId,
			cost,
			err)

		v2.TxnTableRangeDurationHistogram.Observe(cost.Seconds())
	}()

	// make sure we have the block infos snapshot
	if err = tbl.UpdateObjectInfos(ctx); err != nil {
		return
	}

	// get the table's snapshot
	var part *logtailreplay.PartitionState
	if part, err = tbl.getPartitionState(ctx); err != nil {
		return
	}

	var blocks objectio.BlockInfoSlice
	blocks.AppendBlockInfo(objectio.EmptyBlockInfo)

	if err = tbl.rangesOnePart(
		ctx,
		part,
		tbl.GetTableDef(ctx),
		exprs,
		&blocks,
		tbl.proc.Load(),
	); err != nil {
		return
	}
	ranges = &blocks
	return
}

// rangesOnePart collect blocks which are visible to this txn,
// include committed blocks and uncommitted blocks by CN writing S3.
// notice that only clean blocks can be distributed into remote CNs.
func (tbl *txnTable) rangesOnePart(
	ctx context.Context,
	state *logtailreplay.PartitionState, // snapshot state of this transaction
	tableDef *plan.TableDef, // table definition (schema)
	exprs []*plan.Expr, // filter expression
	outBlocks *objectio.BlockInfoSlice, // output marshaled block list after filtering
	proc *process.Process, // process of this transaction
) (err error) {

	uncommittedObjects := tbl.collectUnCommittedObjects()
	dirtyBlks := tbl.collectDirtyBlocks(state, uncommittedObjects)

	done, err := tbl.tryFastFilterBlocks(
		exprs, state, uncommittedObjects, dirtyBlks, outBlocks, tbl.db.txn.engine.fs)
	if err != nil {
		return err
	} else if done {
		return nil
	}

	if done, err = tbl.tryFastRanges(
		exprs, state, uncommittedObjects, dirtyBlks, outBlocks, tbl.db.txn.engine.fs,
	); err != nil {
		return err
	} else if done {
		return nil
	}

	// for dynamic parameter, substitute param ref and const fold cast expression here to improve performance
	newExprs, err := plan.ConstandFoldList(exprs, tbl.proc.Load(), true)
	if err == nil {
		exprs = newExprs
	}

	var (
		objMeta  objectio.ObjectMeta
		zms      []objectio.ZoneMap
		vecs     []*vector.Vector
		skipObj  bool
		auxIdCnt int32
		s3BlkCnt uint32
	)

	defer func() {
		for i := range vecs {
			if vecs[i] != nil {
				vecs[i].Free(proc.Mp())
			}
		}
	}()

	// check if expr is monotonic, if not, we can skip evaluating expr for each block
	for _, expr := range exprs {
		auxIdCnt += plan.AssignAuxIdForExpr(expr, auxIdCnt)
	}

	columnMap := make(map[int]int)
	if auxIdCnt > 0 {
		zms = make([]objectio.ZoneMap, auxIdCnt)
		vecs = make([]*vector.Vector, auxIdCnt)
		plan.GetColumnMapByExprs(exprs, tableDef, columnMap)
	}

	errCtx := errutil.ContextWithNoReport(ctx, true)

	hasDeletes := len(dirtyBlks) > 0

	if err = ForeachSnapshotObjects(
		tbl.db.txn.op.SnapshotTS(),
		func(obj logtailreplay.ObjectInfo, isCommitted bool) (err2 error) {
			var meta objectio.ObjectDataMeta
			skipObj = false

			s3BlkCnt += obj.BlkCnt()
			if auxIdCnt > 0 {
				v2.TxnRangesLoadedObjectMetaTotalCounter.Inc()
				location := obj.ObjectLocation()
				if objMeta, err2 = objectio.FastLoadObjectMeta(
					errCtx, &location, false, tbl.db.txn.engine.fs,
				); err2 != nil {
					return
				}

				meta = objMeta.MustDataMeta()
				// here we only eval expr on the object meta if it has more than one blocks
				if meta.BlockCount() > 2 {
					for _, expr := range exprs {
						if !colexec.EvaluateFilterByZoneMap(
							errCtx, proc, expr, meta, columnMap, zms, vecs,
						) {
							skipObj = true
							break
						}
					}
				}
			}
			if skipObj {
				return
			}

			if obj.Rows() == 0 && meta.IsEmpty() {
				location := obj.ObjectLocation()
				if objMeta, err2 = objectio.FastLoadObjectMeta(
					errCtx, &location, false, tbl.db.txn.engine.fs,
				); err2 != nil {
					return
				}
				meta = objMeta.MustDataMeta()
			}

			ForeachBlkInObjStatsList(true, meta, func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
				skipBlk := false

				if auxIdCnt > 0 {
					// eval filter expr on the block
					for _, expr := range exprs {
						if !colexec.EvaluateFilterByZoneMap(errCtx, proc, expr, blkMeta, columnMap, zms, vecs) {
							skipBlk = true
							break
						}
					}

					// if the block is not needed, skip it
					if skipBlk {
						return true
					}
				}

				blk.Sorted = obj.Sorted
				blk.EntryState = obj.EntryState
				blk.CommitTs = obj.CommitTS
				if obj.HasDeltaLoc {
					deltaLoc, commitTs, ok := state.GetBockDeltaLoc(blk.BlockID)
					if ok {
						blk.DeltaLoc = deltaLoc
						blk.CommitTs = commitTs
					}
				}

				if hasDeletes {
					if _, ok := dirtyBlks[blk.BlockID]; !ok {
						blk.CanRemote = true
					}
					blk.PartitionNum = -1
					outBlocks.AppendBlockInfo(blk)
					return true
				}
				// store the block in ranges
				blk.CanRemote = true
				blk.PartitionNum = -1
				outBlocks.AppendBlockInfo(blk)

				return true

			},
				obj.ObjectStats,
			)
			return
		},
		state,
		uncommittedObjects...,
	); err != nil {
		return
	}

	bhit, btotal := outBlocks.Len()-1, int(s3BlkCnt)
	v2.TaskSelBlockTotal.Add(float64(btotal))
	v2.TaskSelBlockHit.Add(float64(btotal - bhit))
	blockio.RecordBlockSelectivity(bhit, btotal)
	if btotal > 0 {
		v2.TxnRangeSizeHistogram.Observe(float64(bhit))
		v2.TxnRangesBlockSelectivityHistogram.Observe(float64(bhit) / float64(btotal))
	}
	return
}

// tryFastRanges only handle equal expression filter on zonemap and bloomfilter in tp scenario;
// it filters out only a small number of blocks which should not be distributed to remote CNs.
func (tbl *txnTable) tryFastRanges(
	exprs []*plan.Expr,
	snapshot *logtailreplay.PartitionState,
	uncommittedObjects []objectio.ObjectStats,
	dirtyBlocks map[types.Blockid]struct{},
	outBlocks *objectio.BlockInfoSlice,
	fs fileservice.FileService,
) (done bool, err error) {

	if tbl.primaryIdx == -1 || len(exprs) == 0 {
		done = false
		return
	}

	val, isVec := extractPKValueFromEqualExprs(
		tbl.tableDef,
		exprs,
		tbl.primaryIdx,
	)

	var (
		meta     objectio.ObjectDataMeta
		bf       objectio.BloomFilter
		blockCnt uint32
		zmTotal  float64
	)

	var vec *vector.Vector
	if isVec {
		vec = vector.NewVec(types.T_any.ToType())
		_ = vec.UnmarshalBinary(val)
	}

	hasDeletes := len(dirtyBlocks) > 0

	if err = ForeachSnapshotObjects(
		tbl.db.txn.op.SnapshotTS(),
		func(obj logtailreplay.ObjectInfo, isCommitted bool) (err2 error) {
			zmTotal++
			blockCnt += obj.BlkCnt()
			location := obj.Location()

			bf = nil
			if bf, err2 = objectio.LoadBFWithMeta(
				context.Background(), meta, location, fs,
			); err2 != nil {
				return
			}

			var blkIdx int
			blockCnt := int(meta.BlockCount())
			if !isVec {
				blkIdx = sort.Search(blockCnt, func(j int) bool {
					return meta.GetBlockMeta(uint32(j)).
						MustGetColumn(uint16(tbl.primaryIdx)).
						ZoneMap().
						AnyGEByValue(val)
				})
			}
			if blkIdx >= blockCnt {
				return
			}
			for ; blkIdx < blockCnt; blkIdx++ {
				blkMeta := meta.GetBlockMeta(uint32(blkIdx))
				zm := blkMeta.MustGetColumn(uint16(tbl.primaryIdx)).ZoneMap()
				if !isVec && !zm.AnyLEByValue(val) {
					break
				}
				if isVec {
					if !zm.AnyIn(vec) {
						continue
					}
				} else {
					if !zm.ContainsKey(val) {
						continue
					}
				}

				blkBf := bf.GetBloomFilter(uint32(blkIdx))
				blkBfIdx := index.NewEmptyBinaryFuseFilter()
				if err2 = index.DecodeBloomFilter(blkBfIdx, blkBf); err2 != nil {
					return
				}

				var exist bool
				if isVec {
					if exist = blkBfIdx.MayContainsAny(vec); !exist {
						continue
					}
				} else {
					if exist, err2 = blkBfIdx.MayContainsKey(val); err2 != nil {
						return
					} else if !exist {
						continue
					}
				}

				name := obj.ObjectName()
				loc := objectio.BuildLocation(name, obj.Extent(), blkMeta.GetRows(), uint16(blkIdx))
				blk := objectio.BlockInfo{
					//BlockID:   *objectio.BuildObjectBlockid(name, uint16(blkIdx)),
					//SegmentID: name.SegmentId(),
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
					return
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
	); err != nil {
		return
	}

	done = true
	bhit, btotal := outBlocks.Len()-1, int(blockCnt)

	blockio.RecordBlockSelectivity(bhit, btotal)

	return
}
