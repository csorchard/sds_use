package logtailreplay

import (
	btree "github.com/tidwall/btree"
	"sds_use/bloomfilter/d_objectio"
	"sds_use/bloomfilter/z_containers/types"
)

type PartitionState struct {
	rows                  *btree.BTreeG[RowEntry]
	dataObjects           *btree.BTreeG[ObjectEntry]
	dataObjectsByCreateTS *btree.BTreeG[ObjectIndexByCreateTSEntry]
	blockDeltas           *btree.BTreeG[BlockDeltaEntry]
	primaryIndex          *btree.BTreeG[*PrimaryIndexEntry]
	dirtyBlocks           *btree.BTreeG[types.Blockid]
	objectIndexByTS       *btree.BTreeG[ObjectIndexByTSEntry]

	checkpoints []string
	noData      bool
	minTS       types.TS
}

type RowEntry struct{}

type ObjectEntry struct{}

type ObjectIndexByCreateTSEntry struct {
}

type BlockDeltaEntry struct {
}

type PrimaryIndexEntry struct {
}

type ObjectIndexByTSEntry struct {
}

type ObjectInfo struct {
	objectio.ObjectStats

	EntryState  bool
	Sorted      bool
	HasDeltaLoc bool
	CommitTS    types.TS
}

func (o ObjectInfo) Location() objectio.Location {
	return nil
}

func (o ObjectInfo) Extent() objectio.Extent {
	return nil
}
