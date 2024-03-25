package containers

type Vector interface {
	HasNull() bool
	NullCount() uint32
	IsConstNull() bool
	Length() uint32
}

func ForeachWindowBytes(vec Vector, start, length uint32,
	windowFn func(val []byte, isNull bool, row int) (err error)) {

}
