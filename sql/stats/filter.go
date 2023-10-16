package stats

func (s *Stats) RangeFilter(key []interface{}) *Histogram {
	// check columns compatible

	// find bucket with bound match

	// estimate bucket fraction matching filter

	// add previous buckets + current bucket fraction

	// new statistic for included values
	return nil
}

func (s *Stats) EqFilter(key []interface{}) *Histogram {

	// find bucket with bound match

	// check MCVS

	// estimate = (1-sum(mcv))/(distinct - sum(mcvCount))
	// (non mcv fraction)/(distinct - count mcvs)

	// new statistic for included values
	return nil
}

// multi filter

// equijoin
