package objectio

import (
	"context"
	"sds_use/hyperloglog/d_containers/batch"
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

func (w *ObjectWriter) WriteObjectMeta(cnt uint32, meta []ColumnMeta) {
	//w.totalRow = totalrow
	//w.colmeta = metas
	panic("implement me")
}

func (w *ObjectWriter) WriteEnd(ctx context.Context) ([]BlockObject, error) {
	return nil, nil
}
