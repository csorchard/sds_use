package objectio

import (
	"sds_use/hyperloglog/containers/batch"
)

type ObjectWriter struct {
}

func (w *ObjectWriter) Write(batch *batch.Batch) (BlockObject, error) {
	return nil, nil
}

func (w *ObjectWriter) WriteBF(blkIdx int, buf []byte) (err error) {
	//w.blocks[SchemaData][blkIdx].bloomFilter = buf
	return
}
