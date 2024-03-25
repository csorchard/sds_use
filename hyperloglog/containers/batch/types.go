package batch

import "sds_use/hyperloglog/containers/vector"

type Batch struct {
	Ro    bool
	Attrs []string
	Vecs  []*vector.Vector
}
