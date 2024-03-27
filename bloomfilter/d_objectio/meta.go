package objectio

type BloomFilter []byte

func (bf BloomFilter) GetBloomFilter(BlockID uint32) []byte {
	return nil
}

type ObjectDataMeta []byte

func (o ObjectDataMeta) BlockCount() uint32 {
	return 10
}

func (o ObjectDataMeta) GetBlockMeta(u uint32) BlockObject {
	return nil
}

func (o ObjectDataMeta) MustGetColumn(seqnum uint16) ColumnMeta {
	//if seqnum > o.BlockHeader().MaxSeqnum() {
	//	return BuildObjectColumnMeta()
	//}
	//return GetObjectColumnMeta(seqnum, o[headerLen:])
	return nil
}
