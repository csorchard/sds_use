package blockio

import (
	hll "github.com/axiomhq/hyperloglog"
	"sds_use/hyperloglog/b_objectio"
	"sds_use/hyperloglog/d_containers"
)

type ObjectColumnMetasBuilder struct {
	totalRow uint32
	metas    []objectio.ColumnMeta
	sks      []*hll.Sketch
	pkData   []containers.Vector
}

func NewObjectColumnMetasBuilder(colIdx int) *ObjectColumnMetasBuilder {
	metas := make([]objectio.ColumnMeta, colIdx)
	for i := range metas {
		metas[i] = objectio.BuildObjectColumnMeta()
	}
	return &ObjectColumnMetasBuilder{
		metas:  metas,
		sks:    make([]*hll.Sketch, colIdx),
		pkData: make([]containers.Vector, 0),
	}
}

func (b *ObjectColumnMetasBuilder) AddRowCnt(rows int) {
	b.totalRow += uint32(rows)
}

func (b *ObjectColumnMetasBuilder) AddPKData(data containers.Vector) {
	b.pkData = append(b.pkData, data)
}

func (b *ObjectColumnMetasBuilder) InspectVector(idx int, vec containers.Vector, isPK bool) {
	if vec.HasNull() {
		cnt := b.metas[idx].NullCnt()
		cnt += uint32(vec.NullCount())
		b.metas[idx].SetNullCnt(cnt)
	}

	if isPK {
		return
	}
	if b.sks[idx] == nil {
		b.sks[idx] = hll.New()
	}
	if vec.IsConstNull() {
		return
	}
	containers.ForeachWindowBytes(vec, 0, vec.Length(), func(v []byte, isNull bool, row int) (err error) {
		if isNull {
			return
		}
		b.sks[idx].Insert(v)
		return
	})
}

func (b *ObjectColumnMetasBuilder) GetPKData() []containers.Vector {
	return b.pkData
}

func (b *ObjectColumnMetasBuilder) SetPKNdv(idx uint16, ndv uint32) {
	b.metas[idx].SetNdv(ndv)
}

func (b *ObjectColumnMetasBuilder) GetTotalRow() uint32 {
	return b.totalRow
}

func (b *ObjectColumnMetasBuilder) Build() (uint32, []objectio.ColumnMeta) {
	for i := range b.metas {
		if b.sks[i] != nil {
			b.metas[i].SetNdv(uint32(b.sks[i].Estimate()))
		}
	}
	ret := b.metas
	b.metas = nil
	b.sks = nil
	return b.totalRow, ret
}
