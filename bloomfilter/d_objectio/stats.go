package objectio

import "sds_use/bloomfilter/z_utils/metric/stats"

type hitStats struct {
	hit   stats.Counter
	total stats.Counter
}

func (s *hitStats) Record(hit, total int) {
	s.total.Add(int64(total))
	s.hit.Add(int64(hit))
}

type Stats struct {
	blockSelectivity      hitStats
	columnSelectivity     hitStats
	readFilterSelectivity hitStats
	readDelCnt            stats.Counter
	readDelOpTotal        stats.Counter
	readDelRead           stats.Counter
	readDelBisect         stats.Counter
}

func (s *Stats) RecordBlockSelectivity(hit, total int) {
	s.blockSelectivity.Record(hit, total)
}
