package stats

import (
	"container/heap"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Join performs an alignment algorithm on two sets of statistics, and then
// pairwise estiamtes bucket cardinalities by joining MCVs directly and
// assuming key uniformity otherwise.
func Join(s1, s2 sql.Statistic, lFields, rFields []int, debug bool) (sql.Statistic, error) {
	cmp := func(row1, row2 sql.Row) (int, error) {
		var keyCmp int
		for i, f := range lFields {
			k1, _, err := s1.Types()[f].Promote().Convert(row1[f])
			if err != nil {
				return 0, fmt.Errorf("incompatible types")
			}

			k2, _, err := s2.Types()[f].Promote().Convert(row2[rFields[i]])
			if err != nil {
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

	s1Buckets, err := mergeOverlappingBuckets(s1.Histogram(), s1.Types())
	if err != nil {
		return nil, err
	}
	s2Buckets, err := mergeOverlappingBuckets(s2.Histogram(), s2.Types())
	if err != nil {
		return nil, err
	}

	s1AliHist, s2AliHist, err := AlignBuckets(s1Buckets, s2Buckets, s1.Types(), s2.Types(), cmp)
	if err != nil {
		return nil, err
	}
	if debug {
		log.Println("left", s1AliHist.DebugString())
		log.Println("right", s2AliHist.DebugString())
	}

	newHist, err := joinAlignedStats(s1AliHist, s2AliHist, cmp)
	ret := NewStatistic(0, 0, 0, s1.AvgSize(), time.Now(), s1.Qualifier(), s1.Columns(), s1.Types(), newHist, s1.IndexClass())
	return UpdateCounts(ret), nil
}

func joinAlignedStats(left, right sql.Histogram, cmp func(sql.Row, sql.Row) (int, error)) ([]*Bucket, error) {
	var newBuckets []*Bucket
	newCnt := uint64(0)
	for i := range left {
		l := left[i]
		r := right[i]
		lDistinct := float64(l.DistinctCount())
		rDistinct := float64(r.DistinctCount())

		lRows := float64(l.RowCount())
		rRows := float64(r.RowCount())

		var rows uint64

		// mcvs counted in isolation
		for i, key1 := range l.Mcvs() {
			for j, key2 := range r.Mcvs() {
				v, err := cmp(key1, key2)
				if err != nil {
					return nil, err
				}
				if v == 0 {
					rows += l.McvCounts()[i] * r.McvCounts()[j]
					lRows -= float64(l.McvCounts()[i])
					rRows -= float64(r.McvCounts()[j])
					lDistinct--
					rDistinct--
					break
				}
			}
		}

		// true up negative approximations
		if lRows < 0 {
			lRows = 0
		}
		if rRows < 0 {
			rRows = 0
		}

		if lDistinct < 0 {
			lDistinct = 0
		}
		if rDistinct < 0 {
			rDistinct = 0
		}

		// Selinger method on rest of bucket
		maxDistinct := lDistinct
		minDistinct := rDistinct
		if rDistinct > maxDistinct {
			maxDistinct = rDistinct
			minDistinct = lDistinct
		}
		fmt.Println(lRows, rRows, maxDistinct)
		if maxDistinct > 0 {
			rows += uint64(float64(lRows*rRows) / float64(maxDistinct))
		}

		newCnt += rows

		// TODO: something smarter with MCVs
		mcvs := append(l.Mcvs(), r.Mcvs()...)
		mcvCounts := append(l.McvCounts(), r.McvCounts()...)

		newBucket := NewHistogramBucket(
			rows,
			uint64(minDistinct),
			uint64(float64(l.NullCount()*r.NullCount())/float64(maxDistinct)),
			l.BoundCount()*r.BoundCount(), l.UpperBound(), mcvCounts, mcvs)
		newBuckets = append(newBuckets, newBucket)
	}
	return newBuckets, nil
}

// TODO this should match MCVS of first matchign bucket, and then do range
// Selinger for the range of values that overlap.
func coarseAlignment(s1, s2 sql.Statistic, lFields, rFields []int) (sql.Statistic, sql.Statistic, error) {
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

	// lower bounds
	firstCmp, err := cmp(s1.Histogram()[0].UpperBound(), s2.Histogram()[0].UpperBound())
	if err != nil {
		return nil, nil, err
	}

	var reverse bool
	if firstCmp != 0 {
		if firstCmp > 0 {
			reverse = !reverse
			s1, s2 = s2, s1
		}
		lowKey := s1.Histogram()[0].UpperBound()
		s2, err = PrefixGte(s2, lowKey[0])
	}

	// upper bounds
	lastCmp, err := cmp(
		s1.Histogram()[len(s1.Histogram())-1].UpperBound(),
		s2.Histogram()[len(s2.Histogram())-1].UpperBound())
	if err != nil {
		return nil, nil, err
	}

	if lastCmp != 0 {
		if lastCmp > 0 {
			reverse = !reverse
			s1, s2 = s2, s1
		}
		highKey := s1.Histogram()[len(s1.Histogram())-1].UpperBound()
		s2, err = PrefixLte(s2, highKey[0])
	}

	if reverse {
		s1, s2 = s2, s1
	}
	return s1, s2, nil
}

// AlignBuckets produces two histograms with the same number of buckets.
// For every misaligned pair of sorted buckets, cut the one with the
// higher bound value into two smaller buckets. We currently squash the
// ends of histograms to match bound conditions.
// TODO ends, squash? discard?
func AlignBuckets(h1, h2 sql.Histogram, s1Types, s2Types []sql.Type, cmp func(sql.Row, sql.Row) (int, error)) (sql.Histogram, sql.Histogram, error) {
	var numericTypes bool = true
	for _, t := range s1Types {
		if _, ok := t.(sql.NumberType); !ok {
			numericTypes = false
			break
		}
	}

	var leftRes sql.Histogram
	var rightRes sql.Histogram
	var leftStack []sql.HistogramBucket
	var rightStack []sql.HistogramBucket
	var nextL sql.HistogramBucket
	var nextR sql.HistogramBucket
	var keyCmp int
	var err error
	var reverse bool

	swap := func() {
		leftStack, rightStack = rightStack, leftStack
		nextL, nextR = nextR, nextL
		leftRes, rightRes = rightRes, leftRes
		h1, h2 = h2, h1
		reverse = !reverse
	}

	var state sjState = sjStateInit
	for state != sjStateEOF {
		switch state {
		case sjStateInit:

			s1Hist, err := mergeOverlappingBuckets(h1, s1Types)
			if err != nil {
				return nil, nil, err
			}
			s2Hist, err := mergeOverlappingBuckets(h2, s2Types)
			if err != nil {
				return nil, nil, err
			}

			s1Last := s1Hist[len(s1Hist)-1].UpperBound()
			s2Last := s2Hist[len(s2Hist)-1].UpperBound()
			idx1, err := PrefixLtHist(s1Hist, s2Last, cmp)
			idx2, err := PrefixLtHist(s2Hist, s1Last, cmp)
			if idx1 < len(s1Hist) {
				idx1++
			}
			if idx2 < len(s2Hist) {
				idx2++
			}
			s1Hist = s1Hist[:idx1]
			s2Hist = s2Hist[:idx2]

			if len(s1Hist) == 0 || len(s2Hist) == 0 {
				return nil, nil, nil
			}

			m := len(s1Hist) - 1
			leftStack = make([]sql.HistogramBucket, m)
			for i, b := range s1Hist {
				if i == 0 {
					nextL = b
					continue
				}
				leftStack[m-i] = b
			}

			n := len(s2Hist) - 1
			rightStack = make([]sql.HistogramBucket, n)
			for i, b := range s2Hist {
				if i == 0 {
					nextR = b
					continue
				}
				rightStack[n-i] = b
			}

			state = sjStateCmp

		case sjStateCmp:
			keyCmp, err = cmp(nextL.UpperBound(), nextR.UpperBound())
			if err != nil {
				return nil, nil, err
			}
			switch keyCmp {
			case 0:
				state = sjStateInc
			case 1:
				state = sjStateCutLeft
			case -1:
				state = sjStateCutRight
			}

		case sjStateCutLeft:
			// default cuts left
			state = sjStateCut

		case sjStateCutRight:
			// switch to make left the cut target
			swap()
			state = sjStateCut

		case sjStateCut:
			state = sjStateInc

			if !numericTypes {
				// TODO divide equally for string types
				// find bound value, divide equally among segments
				panic("")
			}

			if len(leftRes) == 0 {
				// trying to cut L, first bucket of L < R
				// extend L to match R
				// steal from the next bucket
				// calculate fraction of next bucket above R bound

				if len(rightStack) == 0 {
					continue
				}

				var peekR sql.HistogramBucket
				for len(rightStack) > 0 {
					peekR = rightStack[len(rightStack)-1]
					rightStack = rightStack[:len(rightStack)-1]
					keyCmp, err = cmp(peekR.UpperBound(), nextL.UpperBound())
					if err != nil {
						return nil, nil, err
					}
					if keyCmp > 0 {
						break
					}

					nextR = NewHistogramBucket(
						uint64(float64(nextR.RowCount())+float64(peekR.RowCount())),
						uint64(float64(nextR.DistinctCount())+float64(peekR.DistinctCount())),
						uint64(float64(nextR.NullCount())+float64(peekR.NullCount())),
						peekR.BoundCount(), peekR.UpperBound(), peekR.McvCounts(), peekR.Mcvs())
				}

				// nextR < nextL < peekR
				bucketMagnitude, err := euclideanDistance(nextR.UpperBound(), peekR.UpperBound())
				if err != nil {
					return nil, nil, err
				}

				if bucketMagnitude == 0 {
					//rightStack = append(rightStack, nextR)
					peekR = nil
					//state = sjStateExhaust
					continue
				}

				// estimate midpoint
				cutMagnitude, err := euclideanDistance(nextR.UpperBound(), nextL.UpperBound())
				if err != nil {
					return nil, nil, err
				}

				cutFrac := cutMagnitude / bucketMagnitude

				// lastL -> nextR
				firstHalf := NewHistogramBucket(
					uint64(float64(nextR.RowCount())+float64(peekR.RowCount())*cutFrac),
					uint64(float64(nextR.DistinctCount())+float64(peekR.DistinctCount())*cutFrac),
					uint64(float64(nextR.NullCount())+float64(peekR.NullCount())*cutFrac),
					1, nextL.UpperBound(), nil, nil)

				// nextR -> nextL
				secondHalf := NewHistogramBucket(
					uint64(float64(peekR.RowCount())*(1-cutFrac)),
					uint64(float64(peekR.DistinctCount())*(1-cutFrac)),
					uint64(float64(peekR.NullCount())*(1-cutFrac)),
					peekR.BoundCount(),
					peekR.UpperBound(),
					peekR.McvCounts(),
					peekR.Mcvs())

				nextR = firstHalf
				rightStack = append(rightStack, secondHalf)
				continue
			}

			// left is bigger than right and we have a previous bound, so cut on right boundary
			// get left "distance"
			bucketMagnitude, err := euclideanDistance(nextL.UpperBound(), leftRes[len(leftRes)-1].UpperBound())
			if err != nil {
				return nil, nil, err
			}

			// estimate midpoint
			cutMagnitude, err := euclideanDistance(nextL.UpperBound(), nextR.UpperBound())
			if err != nil {
				return nil, nil, err
			}

			cutFrac := cutMagnitude / bucketMagnitude

			// lastL -> nextR
			firstHalf := NewHistogramBucket(
				uint64(float64(nextL.RowCount())*cutFrac),
				uint64(float64(nextL.DistinctCount())*cutFrac),
				uint64(float64(nextL.NullCount())*cutFrac),
				1, nextR.UpperBound(), nil, nil)

			// nextR -> nextL
			secondHalf := NewHistogramBucket(
				uint64(float64(nextL.RowCount())*(1-cutFrac)),
				uint64(float64(nextL.DistinctCount())*(1-cutFrac)),
				uint64(float64(nextL.NullCount())*(1-cutFrac)),
				nextL.BoundCount(),
				nextL.UpperBound(),
				nextL.McvCounts(),
				nextL.Mcvs())

			nextL = firstHalf
			leftStack = append(leftStack, secondHalf)

		case sjStateInc:
			leftRes = append(leftRes, nextL)
			rightRes = append(rightRes, nextR)

			nextL = nil
			nextR = nil

			if len(leftStack) > 0 {
				nextL = leftStack[len(leftStack)-1]
				leftStack = leftStack[:len(leftStack)-1]
			}
			if len(rightStack) > 0 {
				nextR = rightStack[len(rightStack)-1]
				rightStack = rightStack[:len(rightStack)-1]
			}

			state = sjStateCmp

			if nextL == nil || nextR == nil {
				state = sjStateExhaust
			}

		case sjStateExhaust:
			state = sjStateEOF

			// count rows, count distinct in one with more buckets
			// adjust compared to last bucket on other side
			if nextL == nil && nextR == nil {
				continue
			}

			if nextL == nil {
				// swap so right side is nil
				swap()
			}

			leftStack = append(leftStack, nextL)
			nextL = leftRes[len(leftRes)-1]
			leftRes = leftRes[:len(leftRes)-1]
			for len(leftStack) > 0 {
				peekL := leftStack[len(leftStack)-1]
				leftStack = leftStack[:len(leftStack)-1]
				nextL = NewHistogramBucket(
					uint64(float64(nextL.RowCount())+float64(peekL.RowCount())),
					uint64(float64(nextL.DistinctCount())+float64(peekL.DistinctCount())),
					uint64(float64(nextL.NullCount())+float64(peekL.NullCount())),
					peekL.BoundCount(), peekL.UpperBound(), peekL.McvCounts(), peekL.Mcvs())
			}
			leftRes = append(leftRes, nextL)
			nextL = nil

		}
	}

	if reverse {
		leftRes, rightRes = rightRes, leftRes
	}
	return leftRes, rightRes, nil
}

// mergeMcvs combines two sets of most common values, merging the bound keys
// with the same value and keeping the top k of the merge result.
func mergeMcvs(mcvs1, mcvs2 []sql.Row, mcvCnts1, mcvCnts2 []uint64, cmp func(sql.Row, sql.Row) (int, error)) ([]sql.Row, []uint64, error) {
	if len(mcvs1) < len(mcvs2) {
		// mcvs2 is low
		mcvs1, mcvs2 = mcvs2, mcvs1
		mcvCnts1, mcvCnts2 = mcvCnts2, mcvCnts1
	}
	ret := NewSqlHeap(len(mcvs2))
	seen := make(map[int]bool)
	for i, row1 := range mcvs1 {
		matched := -1
		for j, row2 := range mcvs2 {
			c, err := cmp(row1, row2)
			if err != nil {
				return nil, nil, err
			}
			if c == 0 {
				matched = j
				break
			}
		}
		if matched > 0 {
			seen[matched] = true
			heap.Push(ret, NewHeapRow(mcvs1[i], int(mcvCnts1[i]+mcvCnts2[matched])))
		} else {
			heap.Push(ret, NewHeapRow(mcvs1[i], int(mcvCnts1[i])))
		}
	}
	for j := range mcvs2 {
		if !seen[j] {
			heap.Push(ret, NewHeapRow(mcvs2[j], int(mcvCnts2[j])))

		}
	}
	return ret.Array(), ret.Counts(), nil
}

// mergeOverlappingBuckets folds bins with one element into the previous
// bucket when the bounds keys match.
func mergeOverlappingBuckets(h sql.Histogram, types []sql.Type) (sql.Histogram, error) {
	cmp := func(l, r sql.Row) (int, error) {
		for i := range l {
			cmp, err := types[i].Compare(l[i], r[i])
			if err != nil {
				return 0, err
			}
			switch cmp {
			case 0:
				continue
			case -1:
				return -1, nil
			case 1:
				return 1, nil
			}
		}
		return 0, nil
	}
	j := 0
	i := 0
	k := 0
	for i < len(h) {
		j = i + 1
		h[k] = h[i]
		if j >= len(h) {
			k++
			break
		}
		mcvs, mcvCnts, err := mergeMcvs(h[i].Mcvs(), h[j].Mcvs(), h[i].McvCounts(), h[j].McvCounts(), cmp)
		if err != nil {
			return nil, err
		}
		eq, err := cmp(h[k].UpperBound(), h[j].UpperBound())
		if err != nil {
			return nil, err
		}
		for j < len(h) && h[j].DistinctCount() == 1 && eq == 0 {
			h[k] = NewHistogramBucket(
				h[k].RowCount()+h[j].RowCount(),
				h[k].DistinctCount(),
				h[k].NullCount()+h[j].NullCount(),
				h[k].BoundCount()+h[j].BoundCount(),
				h[k].UpperBound(),
				mcvCnts,
				mcvs)
			j++
			if j >= len(h) {
				break
			}
			eq, err = cmp(h[k].UpperBound(), h[j].UpperBound())
			if err != nil {
				return nil, err
			}
		}
		i = j
		k++
	}
	return h[:k], nil
}

type sjState int8

const (
	sjStateUnknown = iota
	sjStateInit
	sjStateCmp
	sjStateCutLeft
	sjStateCutRight
	sjStateCut
	sjStateInc
	sjStateExhaust
	sjStateTrimOverhang
	sjStateEOF
)

// euclideanDistance is a pairwise sum of squares distance between
// two numeric types.
func euclideanDistance(row1, row2 sql.Row) (float64, error) {
	var distSq float64
	for i := range row1 {
		v1, _, err := types.Float64.Convert(row1[i])
		if err != nil {
			return 0, err
		}
		v2, _, err := types.Float64.Convert(row2[i])
		if err != nil {
			return 0, err
		}
		f1 := v1.(float64)
		f2 := v2.(float64)
		distSq += f1*f1 - 2*f1*f2 + f2*f2
	}
	return math.Sqrt(distSq), nil
}
