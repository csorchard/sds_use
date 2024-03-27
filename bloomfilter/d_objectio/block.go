package objectio

type BlockObject []byte

func (bm BlockObject) MustGetColumn(seqnum uint16) ColumnMeta {
	return nil
}

func (bm BlockObject) GetRows() uint32 {
	return 0
}

func (bm BlockObject) IsEmpty() bool {
	return false
}
