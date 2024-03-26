package objectio

type ColumnMeta []byte

func (cm ColumnMeta) ZoneMap() ZoneMap {
	return ZoneMap(cm[0 : 0+10])
}
