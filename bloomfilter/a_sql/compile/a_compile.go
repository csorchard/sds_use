package compile

import (
	"sds_use/bloomfilter/d_objectio"
	"sds_use/bloomfilter/pb/plan"
	"sds_use/bloomfilter/z_vm/engine"
)

func (c *Compile) expandRanges(n *plan.Node, rel engine.Relation, blockFilterList []*plan.Expr) (engine.Ranges, error) {
	var err error
	var db engine.Database
	var ranges engine.Ranges
	ctx := c.ctx
	if util.TableIsClusterTable(n.TableDef.GetTableType()) {
		ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	}
	if n.ObjRef.PubInfo != nil {
		ctx = defines.AttachAccountId(ctx, uint32(n.ObjRef.PubInfo.GetTenantId()))
	}
	if util.TableIsLoggingTable(n.ObjRef.SchemaName, n.ObjRef.ObjName) {
		ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	}

	db, err = c.e.Database(ctx, n.ObjRef.SchemaName, c.proc.TxnOperator)
	if err != nil {
		return nil, err
	}
	ranges, err = rel.Ranges(ctx, blockFilterList)
	if err != nil {
		return nil, err
	}

	if n.TableDef.Partition != nil {
		if n.PartitionPrune != nil && n.PartitionPrune.IsPruned {
			for i, partitionItem := range n.PartitionPrune.SelectedPartitions {
				partTableName := partitionItem.PartitionTableName
				subrelation, err := db.Relation(ctx, partTableName, c.proc)
				if err != nil {
					return nil, err
				}
				subranges, err := subrelation.Ranges(ctx, n.BlockFilterList)
				if err != nil {
					return nil, err
				}
				// add partition number into objectio.BlockInfo.
				blkSlice := subranges.(*objectio.BlockInfoSlice)
				for j := 1; j < subranges.Len(); j++ {
					blkInfo := blkSlice.Get(j)
					blkInfo.PartitionNum = i
					ranges.Append(blkSlice.GetBytes(j))
				}
			}
		} else {
			partitionInfo := n.TableDef.Partition
			partitionNum := int(partitionInfo.PartitionNum)
			partitionTableNames := partitionInfo.PartitionTableNames
			for i := 0; i < partitionNum; i++ {
				partTableName := partitionTableNames[i]
				subrelation, err := db.Relation(ctx, partTableName, c.proc)
				if err != nil {
					return nil, err
				}
				subranges, err := subrelation.Ranges(ctx, n.BlockFilterList)
				if err != nil {
					return nil, err
				}
				// add partition number into objectio.BlockInfo.
				blkSlice := subranges.(*objectio.BlockInfoSlice)
				for j := 1; j < subranges.Len(); j++ {
					blkInfo := blkSlice.Get(j)
					blkInfo.PartitionNum = i
					ranges.Append(blkSlice.GetBytes(j))
				}
			}
		}
	}

	return ranges, nil
}
