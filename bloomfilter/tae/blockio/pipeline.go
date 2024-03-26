package blockio

import "sds_use/bloomfilter/objectio"

var pipeline *IoPipeline

type IoPipeline struct {
	stats struct {
		selectivityStats *objectio.Stats
	}
}
