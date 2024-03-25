package blockio

import (
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
	block, err := w.writer.Write(batch)
	if err != nil {
		return nil, err
	}

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
