package stats

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
)

// TODO align two histograms

// TODO join two histograms

func Join(s1, s2 sql.Statistic, lFields, rFields []int) (sql.Statistic, error) {
	// alignment algo
	// find the minimum buckets that overlap
	// either 1) stepwise bucket alignment and comparison, subtract mcvs from distinct estimates
	// or     2) direct compare first, do MCV card separately, and then range card as third

	// walk the buckets, interpolate new buckets and sizes
	h1 := s1.Histogram()
	h2 := s2.Histogram()

	var to1 sql.Histogram
	var to2 sql.Histogram
	var i int
	var j int

	cmp := func(row1, row2 sql.Row) (int, error) {
		var keyCmp int
		for i, f := range lFields {
			k1, ok, err := s1.Types()[f].Promote().Convert(row1[f])
			if !ok || err != nil {
				return 0, fmt.Errorf("incompatible types")
			}

			k2, ok, err := s2.Types()[f].Promote().Convert(row2[rFields[i]])
			if !ok || err != nil {
				return 0, fmt.Errorf("incompatible types")
			}

			cmp, err := s1.Types()[f].Promote().Compare(k1, k2)
			if err != nil {
				return 0, err
			}
			if cmp == 0 {
				continue
			}
			keyCmp = cmp
			break
		}
		return keyCmp, nil
	}

	for i < len(h1) && j < len(h2) {
		if keyCmp, err := cmp(h1[i].UpperBound(), h2[j].UpperBound()); err != nil {
			return nil, err
		} else if keyCmp == 0 {
			// keys are equal, increment both
			i++
			j++
			// add i-1 to i
		} else if keyCmp < 0 {
			// make cut in h2
			// add (prev, i) to firstHist
			// add (prev, i) to secondHist
		} else if keyCmp > 0 {
			// make cut in h1
		}
	}

	return s1
}
