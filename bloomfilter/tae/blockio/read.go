package blockio

func RecordBlockSelectivity(hit, total int) {
	pipeline.stats.selectivityStats.RecordBlockSelectivity(hit, total)
}
