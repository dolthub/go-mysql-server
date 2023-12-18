package stats

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"math"
	"time"
)

func Join(s1, s2 sql.Statistic, lFields, rFields []int) (sql.Statistic, error) {
	// alignment algo
	// find the minimum buckets that overlap
	// either 1) stepwise bucket alignment and comparison, subtract mcvs from distinct estimates
	// or     2) direct compare first, do MCV card separately, and then range card as third

	left, right, err := alignBuckets(s1, s2, lFields, rFields)
	if err != nil {
		return nil, err
	}
	fmt.Println("left", left.Histogram().DebugString())
	fmt.Println("right", right.Histogram().DebugString())

	var newBuckets []*Bucket
	newCnt := uint64(0)
	for i := range left.Histogram() {
		l := left.Histogram()[i]
		r := right.Histogram()[i]
		distinct := l.DistinctCount()
		if r.DistinctCount() > distinct {
			distinct = r.DistinctCount()
		}

		mcvs := append(l.Mcvs(), r.Mcvs()...)
		mcvCounts := append(l.McvCounts(), r.McvCounts()...)
		rows := uint64(float64(l.RowCount()*r.RowCount()) / float64(distinct))
		newCnt += rows
		newBucket := NewHistogramBucket(
			rows,
			distinct,
			uint64(float64(l.NullCount()*r.NullCount())/float64(distinct)),
			l.BoundCount()*r.BoundCount(), l.UpperBound(), mcvCounts, mcvs)
		newBuckets = append(newBuckets, newBucket)
	}

	mult := float64(left.RowCount())
	if right.RowCount() < left.RowCount() {
		mult = float64(right.RowCount())
	}
	mult = float64(newCnt) / mult
	for _, b := range newBuckets {
		for i, v := range b.McvCounts() {
			b.McvCounts()[i] = uint64(float64(v) * mult)
		}
	}

	resStat := NewStatistic(0, 0, 0, s1.AvgSize(), time.Now(), s1.Qualifier(), s1.Columns(), s1.Types(), newBuckets, s1.IndexClass())
	return UpdateCounts(resStat), nil
	// add MCVs and scale by selectivity
}

func coarseAlignment(s1, s2 sql.Statistic, lFields, rFields []int) (sql.Statistic, sql.Statistic, error) {
	// find overlapping buckets
	// one of first upper bounds is higher
	// find bucket upper bound in other histogram that's just above
	// do same thing for last key

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
	//var cardEst float64
	//cardEst += float64(s1.Histogram()[0].RowCount() * s2.Histogram()[0].RowCount())
	//
	//firstS1 := s1.Histogram()[0]
	//firstS2 := s2.Histogram()[0]
	//// count distinct in the two ranges
	//distinct := s1.DistinctCount() - firstS1.DistinctCount()
	//if s2Distinct := s2.DistinctCount() - firstS2.DistinctCount(); s2Distinct > distinct {
	//	distinct = s2Distinct
	//}
	//
	//// freq = rows / distinct
	//cardEst += float64(s1.RowCount()-firstS1.RowCount()+s2.RowCount()-firstS2.RowCount()) / float64(distinct)
	//return cardEst, nil
}

// maybe not returned in same order
func alignBuckets(s1, s2 sql.Statistic, lFields, rFields []int) (sql.Statistic, sql.Statistic, error) {
	// walk the buckets, interpolate new buckets and sizes

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

	var numericTypes bool = true
	for _, t := range s1.Types() {
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
	var state sjState = sjStateInit
	for state != sjStateEOF {
		switch state {
		case sjStateInit:

			s1Hist := compressBuckets(s1.Histogram(), s1.Types())
			s2Hist := compressBuckets(s2.Histogram(), s2.Types())

			//s1, s2, err = coarseAlignment(s1, s2, lFields, rFields)
			if len(s1Hist) == 0 || len(s2Hist) == 0 {
				return s1, s2, nil
			}

			// TODO copy these
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
			leftStack, rightStack = rightStack, leftStack
			nextL, nextR = nextR, nextL
			leftRes, rightRes = rightRes, leftRes
			lFields, rFields = rFields, lFields
			s1, s2 = s2, s1
			state = sjStateCut

		case sjStateCut:
			// if string key, just cut in half
			state = sjStateInc

			if !numericTypes {
				// find bound value, divide equally among segments
				panic("")
			}

			if len(leftRes) == 0 {
				// trying to cut L, first bucket of L < R
				// extend L to match R
				// steal from the next bucket
				// calculate fraction of next bucket above R bound

				// left is bigger than right and we have a previous bound, so cut on right boundary
				// get left "distance"
				peekR := rightStack[len(rightStack)-1]
				rightStack = rightStack[:len(rightStack)-1]

				// compress peek while upper bound still less than nextR.UpperBound()
				keyCmp, err = cmp(peekR.UpperBound(), nextL.UpperBound())
				if err != nil {
					return nil, nil, err
				}
				for keyCmp < 0 {
					// combine peekL and
					nextR = NewHistogramBucket(
						uint64(float64(nextL.RowCount())+float64(peekR.RowCount())),
						uint64(float64(nextL.DistinctCount())+float64(peekR.DistinctCount())),
						uint64(float64(nextL.NullCount())+float64(peekR.NullCount())),
						peekR.BoundCount(), peekR.UpperBound(), peekR.McvCounts(), peekR.Mcvs())
					peekR = rightStack[len(rightStack)-1]
					rightStack = rightStack[:len(rightStack)-1]
					keyCmp, err = cmp(peekR.UpperBound(), nextL.UpperBound())
					if err != nil {
						return nil, nil, err
					}
				}

				// nextR < nextL < peekR
				bucketMagnitude, err := euclideanDistance(nextR.UpperBound(), peekR.UpperBound())
				if err != nil {
					return nil, nil, err
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
				state = sjStateFinalize
			}

		case sjStateFinalize:
			state = sjStateEOF

			// count rows, count distinct in one with more buckets
			// adjust compared to last bucket on other side
			if nextL == nil && nextR == nil {
				continue
			}

			if nextL == nil {
				// swap so right side is nil
				leftStack, rightStack = rightStack, leftStack
				nextL, nextR = nextR, nextL
				leftRes, rightRes = rightRes, leftRes
				lFields, rFields = rFields, lFields
				s1, s2 = s2, s1
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

	leftRes = compressBuckets(leftRes, s1.Types())
	newS1, err := s1.WithHistogram(leftRes)
	if err != nil {
		return nil, nil, err
	}
	rightRes = compressBuckets(rightRes, s2.Types())
	newS2, err := s1.WithHistogram(rightRes)
	if err != nil {
		return nil, nil, err
	}

	return newS1, newS2, nil
}

// compress buckets folds buckets with one element into the previous
// bucket when the bounds keys match.
func compressBuckets(h sql.Histogram, types []sql.Type) sql.Histogram {
	cmp := func(l, r sql.Row) int {
		for i := range l {
			cmp, _ := types[i].Compare(l[i], r[i])
			switch cmp {
			case 0:
				continue
			case -1:
				return -1
			case 1:
				return 1
			}
		}
		return 0
	}
	j := 0
	i := 0
	k := 0
	for i < len(h) {
		j = i + 1
		h[k] = h[i]
		for j < len(h) && h[j].DistinctCount() == 1 && cmp(h[k].UpperBound(), h[j].UpperBound()) == 0 {
			h[k] = NewHistogramBucket(
				h[i].RowCount()+h[k].RowCount(),
				h[j].DistinctCount(),
				h[i].NullCount()+h[j].NullCount(),
				h[i].BoundCount()+h[j].BoundCount(),
				h[i].UpperBound(),
				// TODO MCVs
				nil, nil)
			j++
		}
		i = j
		k++
	}
	return h[:k]
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
	sjStateFinalize
	sjStateEOF
)

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
