package blockio

import "sds_use/bloomfilter/d_objectio"

var pipeline *IoPipeline

type IoPipeline struct {
	stats struct {
		selectivityStats *objectio.Stats
	}
}
