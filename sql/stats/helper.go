package stats

import "github.com/dolthub/go-mysql-server/sql"

func Empty(s sql.Statistic) bool {
	return s == nil || len(s.Histogram()) == 0
}

func InterpolateNewCounts(from, to sql.Statistic) sql.Statistic {
	if Empty(from) {
		return to
	} else if Empty(from) {
		return to
	}

	if to.DistinctCount() < from.DistinctCount() {
		// invalid use of interpolate
		return to
	}

	filterSelectivity := (to.DistinctCount() - from.DistinctCount()) / to.DistinctCount()

	newHist := make([]*Bucket, len(to.Histogram()))
	for i, h := range to.Histogram() {
		newMcvs := make([]uint64, len(h.McvCounts()))
		for i, cnt := range h.McvCounts() {
			newMcvs[i] = cnt * filterSelectivity
		}
		newHist[i] = NewHistogramBucket(
			h.RowCount()*filterSelectivity,
			h.DistinctCount()*filterSelectivity,
			h.NullCount()*filterSelectivity,
			h.BoundCount()*filterSelectivity,
			h.UpperBound(),
			h.McvCounts(),
			h.Mcvs())
	}
	return UpdateCounts(NewStatistic(0, 0, 0, to.AvgSize(), to.CreatedAt(), to.Qualifier(), to.Columns(), to.Types(), newHist, to.IndexClass(), nil))
}
