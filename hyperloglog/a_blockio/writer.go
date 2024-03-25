package blockio

import (
	"context"
	"sds_use/hyperloglog/b_objectio"
	"sds_use/hyperloglog/c_index"
	"sds_use/hyperloglog/d_containers/batch"
)

type BlockWriter struct {
	writer         *objectio.ObjectWriter
	objMetaBuilder *ObjectColumnMetasBuilder
	isSetPK        bool
	pk             uint16
}

// WriteBatch writes a batch to the block writer.
// NOTE: Entry point
func (w *BlockWriter) WriteBatch(batch *batch.Batch) (objectio.BlockObject, error) {
	// 1. Write Data
	block, err := w.writer.Write(batch)
	if err != nil {
		return nil, err
	}

	// 2. Write Metadata
	if w.objMetaBuilder == nil {
		w.objMetaBuilder = NewObjectColumnMetasBuilder(len(batch.Vecs))
	}
	for i, vec := range batch.Vecs {
		isPK := false
		if i == 0 {
			w.objMetaBuilder.AddRowCnt(int(vec.Length()))
		}

		if w.isSetPK && w.pk == uint16(i) {
			isPK = true
		}
		w.objMetaBuilder.InspectVector(i, vec, isPK)
		if !w.isSetPK || w.pk != uint16(i) {
			continue
		}

		w.objMetaBuilder.AddPKData(vec)

		bf, err := index.NewBinaryFuseFilter(vec)
		if err != nil {
			return nil, err
		}
		buf, err := bf.Marshal()
		if err != nil {
			return nil, err
		}

		if err = w.writer.WriteBF(int(block.GetID()), buf); err != nil {
			return nil, err
		}
	}
	return block, nil
}

func (w *BlockWriter) Sync(ctx context.Context) ([]objectio.BlockObject, objectio.Extent, error) {
	if w.objMetaBuilder != nil {
		if w.isSetPK {
			w.objMetaBuilder.SetPKNdv(w.pk, w.objMetaBuilder.GetTotalRow())
		}
		cnt, meta := w.objMetaBuilder.Build()
		w.writer.WriteObjectMeta(cnt, meta)
	}
	blocks, err := w.writer.WriteEnd(ctx)

	return blocks, nil, err
}
