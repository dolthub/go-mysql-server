// Copyright 2023 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package stats

import (
	"sort"

	"github.com/dolthub/go-mysql-server/sql"
)

func Union(s1, s2 sql.Statistic) sql.Statistic {
	return s1
}

func Intersect(s1, s2 sql.Statistic) sql.Statistic {
	return s1
}

func PrefixKey(statistic sql.Statistic, key []interface{}, nullable []bool) (sql.Statistic, error) {
	idxCols := statistic.ColSet()
	var constant sql.ColSet
	var notNull sql.ColSet
	var i sql.ColumnId
	for _, null := range nullable[:len(key)] {
		i, _ = idxCols.Next(i + 1)
		constant.Add(i)
		if !null {
			notNull.Add(i)
		}
	}

	old := statistic.FuncDeps()
	new := sql.NewFilterFDs(old, old.NotNull().Union(notNull), old.Constants().Union(constant), nil)
	ret := statistic.WithFuncDeps(new)

	// find index of bucket >= the key
	buckets := []sql.HistogramBucket(statistic.Histogram())
	var searchErr error
	lowBucket := sort.Search(len(buckets), func(i int) bool {
		// lowest index that func is true
		// lowest index where bucketKey >= key
		bucketKey := buckets[i].UpperBound()
		for i, _ := range key {
			t := statistic.Types()[i]
			cmp, err := nilSafeCmp(t, bucketKey[i], key[i])
			if err != nil {
				searchErr = err
			}
			switch cmp {
			case 0:
				// equal, keep searching for ineq
			case 1:
				return true
			case -1:
				// bucket upper range too low
				return false
			}
		}
		return true
	})
	if searchErr != nil {
		return nil, searchErr
	}

	upperBucket := lowBucket
	equals := true
	var err error
	for equals && upperBucket < len(buckets) {
		equals, err = keysEqual(statistic.Types(), buckets[upperBucket].UpperBound(), key)
		if err != nil {
			return nil, err
		}
		upperBucket++
	}

	ret, err = ret.WithHistogram(buckets[lowBucket:upperBucket])
	if err != nil {
		return nil, err
	}

	return UpdateCounts(ret), nil
}

func nilSafeCmp(typ sql.Type, left, right interface{}) (int, error) {
	if left == nil && right == nil {
		return 0, nil
	} else if left == nil && right != nil {
		return -1, nil
	} else if left != nil && right == nil {
		return 1, nil
	} else {
		return typ.Compare(left, right)
	}
}

func UpdateCounts(statistic sql.Statistic) sql.Statistic {
	buckets := []sql.HistogramBucket(statistic.Histogram())
	if len(buckets) == 0 {
		return statistic
	}
	var rowCount uint64
	var distinctCount uint64
	var nullCount uint64
	for _, b := range buckets {
		rowCount += b.RowCount()
		distinctCount += b.DistinctCount()
		nullCount += b.NullCount()
	}
	return statistic.WithRowCount(rowCount).WithDistinct(distinctCount).WithNullCount(nullCount)
}

func keysEqual(types []sql.Type, left, right []interface{}) (bool, error) {
	for i, _ := range right {
		t := types[i]
		cmp, err := t.Compare(left[i], right[i])
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}

func PrefixLt(statistic sql.Statistic, val interface{}) (sql.Statistic, error) {
	// first bucket whose upper bound is greater than val
	buckets := []sql.HistogramBucket(statistic.Histogram())
	var searchErr error
	idx := sort.Search(len(buckets), func(i int) bool {
		// lowest index that func is true
		bucketKey := buckets[i].UpperBound()
		typ := statistic.Types()[0]
		cmp, err := nilSafeCmp(typ, bucketKey[0], val)
		if err != nil {
			searchErr = err
		}
		return cmp >= 0
	})
	if searchErr != nil {
		return nil, searchErr
	}
	// inclusive of idx bucket
	ret, err := statistic.WithHistogram(buckets[:idx])
	if err != nil {
		return nil, err
	}
	return PrefixIsNotNull(ret)
}

func PrefixGt(statistic sql.Statistic, val interface{}) (sql.Statistic, error) {
	buckets := []sql.HistogramBucket(statistic.Histogram())
	var searchErr error
	idx := sort.Search(len(buckets), func(i int) bool {
		// lowest index that func is true
		bucketKey := buckets[i].UpperBound()
		typ := statistic.Types()[0]
		cmp, err := nilSafeCmp(typ, bucketKey[0], val)
		if err != nil {
			searchErr = err
		}
		return cmp > 0
	})
	if searchErr != nil {
		return nil, searchErr
	}
	// inclusive of idx bucket
	ret, err := statistic.WithHistogram(buckets[idx:])
	if err != nil {
		return nil, err
	}
	return PrefixIsNotNull(ret)
}

func PrefixLte(statistic sql.Statistic, val interface{}) (sql.Statistic, error) {
	// first bucket whose upper bound is greater than val
	buckets := []sql.HistogramBucket(statistic.Histogram())
	var searchErr error
	idx := sort.Search(len(buckets), func(i int) bool {
		// lowest index that func is true
		bucketKey := buckets[i].UpperBound()
		typ := statistic.Types()[0]
		cmp, err := nilSafeCmp(typ, bucketKey[0], val)
		if err != nil {
			searchErr = err
		}
		return cmp > 0
	})
	if searchErr != nil {
		return nil, searchErr
	}
	// inclusive of idx bucket
	ret, err := statistic.WithHistogram(buckets[:idx])
	if err != nil {
		return nil, err
	}
	return PrefixIsNotNull(ret)
}

func PrefixGte(statistic sql.Statistic, val interface{}) (sql.Statistic, error) {
	buckets := []sql.HistogramBucket(statistic.Histogram())
	var searchErr error
	idx := sort.Search(len(buckets), func(i int) bool {
		// lowest index that func is true
		bucketKey := buckets[i].UpperBound()
		typ := statistic.Types()[0]
		cmp, err := nilSafeCmp(typ, bucketKey[0], val)
		if err != nil {
			searchErr = err
		}
		return cmp >= 0
	})
	if searchErr != nil {
		return nil, searchErr
	}
	// inclusive of idx bucket
	ret, err := statistic.WithHistogram(buckets[idx:])
	if err != nil {
		return nil, err
	}
	return PrefixIsNotNull(ret)
}

func PrefixIsNull(statistic sql.Statistic) (sql.Statistic, error) {
	buckets := []sql.HistogramBucket(statistic.Histogram())
	var searchErr error
	idx := sort.Search(len(buckets), func(i int) bool {
		// lowest index that func is true
		bucketKey := buckets[i].UpperBound()
		return bucketKey[0] != nil
	})
	if searchErr != nil {
		return nil, searchErr
	}
	// exclusive of idx bucket
	ret, err := statistic.WithHistogram(buckets[:idx])
	if err != nil {
		return nil, err
	}
	return UpdateCounts(ret), nil
}

func PrefixIsNotNull(statistic sql.Statistic) (sql.Statistic, error) {
	buckets := []sql.HistogramBucket(statistic.Histogram())
	var searchErr error
	idx := sort.Search(len(buckets), func(i int) bool {
		// lowest index that func is true
		bucketKey := buckets[i].UpperBound()
		return bucketKey[0] != nil

	})
	if searchErr != nil {
		return nil, searchErr
	}
	// inclusive of idx bucket
	ret, err := statistic.WithHistogram(buckets[idx:])
	if err != nil {
		return nil, err
	}
	return UpdateCounts(ret), nil
}

func McvPrefixGt(statistic sql.Statistic, i int, val interface{}) (sql.Statistic, error) {
	return statistic, nil
}

func McvPrefixLt(statistic sql.Statistic, i int, val interface{}) (sql.Statistic, error) {
	return statistic, nil
}

func McvPrefixGte(statistic sql.Statistic, i int, val interface{}) (sql.Statistic, error) {
	return statistic, nil
}

func McvPrefixLte(statistic sql.Statistic, i int, val interface{}) (sql.Statistic, error) {
	return statistic, nil
}

func McvPrefixIsNull(statistic sql.Statistic, i int, val interface{}) (sql.Statistic, error) {
	return statistic, nil
}

func McvPrefixIsNotNull(statistic sql.Statistic, i int, val interface{}) (sql.Statistic, error) {
	return statistic, nil
}
