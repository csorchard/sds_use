package batch

import "sds_use/hyperloglog/d_containers/vector"

type Batch struct {
	Ro    bool
	Attrs []string
	Vecs  []*vector.Vector
}
