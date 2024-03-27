package engine

type Ranges interface {
	GetBytes(i int) []byte

	Len() int

	Append([]byte)

	Size() int

	SetBytes([]byte)

	GetAllBytes() []byte

	Slice(i, j int) []byte
}
