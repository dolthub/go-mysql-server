// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package information_schema

import (
	"io"
	"math"
	"sort"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// NewHistogramMapFromTable will construct a HistogramMap given a Table
// TODO: have option for number of buckets (and logic to convert freqMap into those buckets)
// TODO: could iterate over Partitions asynchronously (after exchange is rewritten)
func NewHistogramMapFromTable(ctx *sql.Context, t sql.Table) (sql.HistogramMap, error) {
	// initialize histogram map
	histMap := make(sql.HistogramMap)
	cols := t.Schema()
	for _, col := range cols {
		hist := new(sql.Histogram)
		hist.Min = math.MaxFloat64
		hist.Max = -math.MaxFloat64
		histMap[col.Name] = hist
	}

	// freqMap can be adapted to a histogram with any number of buckets
	freqMap := make(map[string]map[float64]uint64)
	for _, col := range cols {
		freqMap[col.Name] = make(map[float64]uint64)
	}

	partIter, err := t.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	for {
		part, err := partIter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		iter, err := t.PartitionRows(ctx, part)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		for {
			row, err := iter.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}

			for i, col := range cols {
				hist, ok := histMap[col.Name]
				if !ok {
					panic("histogram was not initialized for this column; shouldn't be possible")
				}

				if row[i] == nil {
					hist.NullCount++
					continue
				}

				val, err := types.Float64.Convert(row[i])
				if err != nil {
					continue // silently skip unsupported column types for now
				}
				v := val.(float64)

				if freq, ok := freqMap[col.Name][v]; ok {
					freq++
				} else {
					freqMap[col.Name][v] = 1
					hist.DistinctCount++
				}

				hist.Mean += v
				hist.Min = math.Min(hist.Min, v)
				hist.Max = math.Max(hist.Max, v)
				hist.Count++
			}
		}
	}

	// add buckets to histogram in sorted order
	for colName, freqs := range freqMap {
		keys := make([]float64, 0)
		for k, _ := range freqs {
			keys = append(keys, k)
		}
		sort.Float64s(keys)

		hist := histMap[colName]
		if hist.Count == 0 {
			hist.Min = 0
			hist.Max = 0
			continue
		}

		hist.Mean /= float64(hist.Count)
		for _, k := range keys {
			bucket := &sql.HistogramBucket{
				LowerBound: k,
				UpperBound: k,
				Frequency:  float64(freqs[k]) / float64(hist.Count),
			}
			hist.Buckets = append(hist.Buckets, bucket)
		}
	}

	return histMap, nil
}
