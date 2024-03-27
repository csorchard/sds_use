package disttae

import (
	"context"
	"sds_use/bloomfilter/a_sql/colexec"
	"sds_use/bloomfilter/b_disttae/logtailreplay"
	"sds_use/bloomfilter/c_tae/blockio"
	"sds_use/bloomfilter/c_tae/index"
	"sds_use/bloomfilter/d_objectio"
	"sds_use/bloomfilter/e_fileservice"
	"sds_use/bloomfilter/z_containers/types"
	"sds_use/bloomfilter/z_containers/vector"
	"sds_use/bloomfilter/z_pb/plan"
	"sds_use/bloomfilter/z_process"
	"sds_use/bloomfilter/z_vm/engine"
	"sort"
	"sync/atomic"
)

type txnTable struct {
	primaryIdx     int
	tableDef       *plan.TableDef
	db             *txnDatabase
	proc           atomic.Pointer[process.Process]
	_partState     atomic.Pointer[logtailreplay.PartitionState]
	logtailUpdated atomic.Bool
	tableId        uint64
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

// Ranges : return all unmodified blocks
// NOTE: one entry point
func (tbl *txnTable) Ranges(ctx context.Context, exprs []*plan.Expr) (ranges engine.Ranges, err error) {
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

	if done, err := tbl.tryFastRanges(
		exprs, state, uncommittedObjects, dirtyBlks, outBlocks, tbl.db.txn.engine.fs,
	); err != nil {
		return err
	} else if done {
		return nil
	}

	//// for dynamic parameter, substitute param ref and const fold cast expression here to improve performance
	//newExprs, err := plan.ConstandFoldList(exprs, tbl.proc.Load(), true)
	//if err == nil {
	//	exprs = newExprs
	//}

	var (
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

	hasDeletes := len(dirtyBlks) > 0
	columnMap := make(map[int]int)

	if err = ForeachSnapshotObjects(
		tbl.db.txn.op.SnapshotTS(),
		func(obj logtailreplay.ObjectInfo, isCommitted bool) (err2 error) {
			var meta objectio.ObjectDataMeta
			skipObj = false

			s3BlkCnt += obj.BlkCnt()
			if skipObj {
				return
			}

			ForeachBlkInObjStatsList(true, meta, func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
				skipBlk := false

				if auxIdCnt > 0 {
					// eval filter expr on the block
					for _, expr := range exprs {
						if !colexec.EvaluateFilterByZoneMap(proc, expr, blkMeta, columnMap, zms, vecs) {
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
	blockio.RecordBlockSelectivity(bhit, btotal)
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

func (tbl *txnTable) collectUnCommittedObjects() []objectio.ObjectStats {
	return nil
}

func (tbl *txnTable) collectDirtyBlocks(
	state *logtailreplay.PartitionState,
	uncommittedObjects []objectio.ObjectStats) map[types.Blockid]struct{} {
	return nil
}

// tryFastFilterBlocks is going to replace the tryFastRanges completely soon, in progress now.
func (tbl *txnTable) tryFastFilterBlocks(
	exprs []*plan.Expr,
	snapshot *logtailreplay.PartitionState,
	uncommittedObjects []objectio.ObjectStats,
	dirtyBlocks map[types.Blockid]struct{},
	outBlocks *objectio.BlockInfoSlice,
	fs fileservice.FileService) (done bool, err error) {
	// TODO: refactor this code if composite key can be pushdown
	if tbl.tableDef.Pkey == nil || tbl.tableDef.Pkey.CompPkeyCol == nil {
		return TryFastFilterBlocks(
			tbl.db.txn.op.SnapshotTS(),
			tbl.tableDef,
			exprs,
			snapshot,
			uncommittedObjects,
			dirtyBlocks,
			outBlocks,
			fs,
			tbl.proc.Load(),
		)
	}
	return
}

func (tbl *txnTable) GetTableDef(ctx context.Context) *plan.TableDef {
	return nil
}

// get the table's snapshot.
// it is only initialized once for a transaction and will not change.
func (tbl *txnTable) getPartitionState(ctx context.Context) (*logtailreplay.PartitionState, error) {
	if tbl._partState.Load() == nil {
		if err := tbl.updateLogtail(ctx); err != nil {
			return nil, err
		}
		tbl._partState.Store(tbl.db.txn.engine.getPartition(tbl.db.databaseId, tbl.tableId).Snapshot())
	}
	return tbl._partState.Load(), nil
}

func (tbl *txnTable) updateLogtail(ctx context.Context) (err error) {
	defer func() {
		if err == nil {
			//tbl.db.txn.engine.globalStats.notifyLogtailUpdate(tbl.tableId)
			tbl.logtailUpdated.Store(true)
		}
	}()
	// if the logtail is updated, skip
	if tbl.logtailUpdated.Load() {
		return
	}

	// if the table is created in this txn, skip
	//accountId, err := defines.GetAccountId(ctx)
	//if err != nil {
	//	return err
	//}
	//if _, created := tbl.db.txn.createMap.Load(
	//	genTableKey(accountId, tbl.tableName, tbl.db.databaseId)); created {
	//	return
	//}
	//
	//tableId := tbl.tableId
	/*
		if the table is truncated once or more than once,
		it is suitable to use the old table id to sync logtail.

		CORNER CASE 1:
		create table t1(a int);
		begin;
		truncate t1; //table id changed. there is no new table id in DN.
		select count(*) from t1; // sync logtail for the new id failed.

		CORNER CASE 2:
		create table t1(a int);
		begin;
		select count(*) from t1; // sync logtail for the old succeeded.
		truncate t1; //table id changed. there is no new table id in DN.
		select count(*) from t1; // not sync logtail this time.

		CORNER CASE 3:
		create table t1(a int);
		begin;
		truncate t1; //table id changed. there is no new table id in DN.
		truncate t1; //table id changed. there is no new table id in DN.
		select count(*) from t1; // sync logtail for the new id failed.
	*/
	//if tbl.oldTableId != 0 {
	//	tableId = tbl.oldTableId
	//}
	//
	//if err = tbl.db.txn.engine.UpdateOfPush(ctx, tbl.db.databaseId, tableId, tbl.db.txn.op.SnapshotTS()); err != nil {
	//	return
	//}
	//if _, err = tbl.db.txn.engine.lazyLoad(ctx, tbl); err != nil {
	//	return
	//}

	return nil
}

func (tbl *txnTable) UpdateObjectInfos(ctx context.Context) (err error) {
	//tbl.tnList = []int{0}
	//
	//accountId, err := defines.GetAccountId(ctx)
	//if err != nil {
	//	return err
	//}
	//_, created := tbl.db.txn.createMap.Load(genTableKey(accountId, tbl.tableName, tbl.db.databaseId))
	//// check if the table is not created in this txn, and the block infos are not updated, then update:
	//// 1. update logtail
	//// 2. generate block infos
	//// 3. update the blockInfosUpdated and blockInfos fields of the table
	//if !created && !tbl.objInfosUpdated.Load() {
	//	if err = tbl.updateLogtail(ctx); err != nil {
	//		return
	//	}
	//	tbl.objInfosUpdated.Store(true)
	//}
	return
}
