// Copyright 2023 Dolthub, Inc.
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

package stats

// This is a presentation layer package. Custom implementations converge here
// as a conversion between SQL inputs/outputs. These do not add anything to the
// interfaces defined in |sql|, but the separation is necessary for import conflicts.

import (
	"fmt"
	"regexp"
	"time"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func NewStatistic(rowCount, distinctCount, nullCount, avgSize uint64, createdAt time.Time, qualifier sql.StatQualifier, columns []string, types []sql.Type, histogram []*Bucket) *Statistic {
	return &Statistic{
		RowCnt:      rowCount,
		DistinctCnt: distinctCount,
		NullCnt:     nullCount,
		AvgRowSize:  avgSize,
		Created:     createdAt,
		Qual:        qualifier,
		Cols:        columns,
		Typs:        types,
		Hist:        histogram,
	}
}

type Statistic struct {
	RowCnt      uint64            `json:"row_count"`
	DistinctCnt uint64            `json:"distinct_count"`
	NullCnt     uint64            `json:"null_count"`
	AvgRowSize  uint64            `json:"avg_size"`
	Created     time.Time         `json:"created_at"`
	Qual        sql.StatQualifier `json:"qualifier"`
	Cols        []string          `json:"columns"`
	Typs        []sql.Type        `json:"types"`
	Hist        []*Bucket         `json:"buckets"`
}

func (s *Statistic) SetTypes(t []sql.Type) {
	s.Typs = t
}

func (s *Statistic) SetColumns(c []string) {
	s.Cols = c
}

func (s *Statistic) SetQualifier(q sql.StatQualifier) {
	s.Qual = q
}

func (s *Statistic) RowCount() uint64 {
	return s.RowCnt
}

func (s *Statistic) DistinctCount() uint64 {
	return s.DistinctCnt
}

func (s *Statistic) NullCount() uint64 {
	return s.NullCnt
}

func (s *Statistic) AvgSize() uint64 {
	return s.AvgRowSize
}

func (s *Statistic) CreatedAt() time.Time {
	return s.Created
}

func (s *Statistic) Columns() []string {
	return s.Cols
}

func (s *Statistic) Qualifier() sql.StatQualifier {
	return s.Qual
}

func (s *Statistic) Types() []sql.Type {
	return s.Typs
}

func (s *Statistic) Histogram() sql.Histogram {
	buckets := make([]sql.HistogramBucket, len(s.Hist))
	for i, b := range s.Hist {
		buckets[i] = b
	}
	return buckets
}

var _ sql.JSONWrapper = (*Statistic)(nil)
var _ sql.Statistic = (*Statistic)(nil)

func (s *Statistic) ToInterface() interface{} {
	typs := make([]string, len(s.Typs))
	for i, t := range s.Typs {
		typs[i] = t.String()
	}
	return map[string]interface{}{
		"statistic": map[string]interface{}{
			"row_count":      s.RowCount(),
			"null_count":     s.RowCount(),
			"distinct_count": s.DistinctCount(),
			"avg_size":       s.AvgSize(),
			"created_at":     s.CreatedAt(),
			"qualifier":      s.Qualifier().String(),
			"columns":        s.Columns(),
			"types:":         typs,
			"buckets":        s.Histogram().ToInterface(),
		},
	}
}

func ParseTypeStrings(typs []string) ([]sql.Type, error) {
	if len(typs) == 0 {
		return nil, nil
	}
	ret := make([]sql.Type, len(typs))
	var err error
	typRegex := regexp.MustCompile("([a-z]+)\\((\\d+)\\)")
	for i, typ := range typs {
		typMatch := typRegex.FindStringSubmatch(typ)
		colType := &sqlparser.ColumnType{}
		if typMatch == nil {
			colType.Type = typ
		} else {
			colType.Type = typMatch[1]
			if len(typMatch) > 2 {
				colType.Length = &sqlparser.SQLVal{Val: []byte(typMatch[2]), Type: sqlparser.IntVal}
			}
		}
		ret[i], err = types.ColumnTypeToType(colType)
		if err != nil {
			return nil, fmt.Errorf("failed to parse histogram type: %s", typMatch)
		}
	}
	return ret, nil
}

func NewHistogramBucket(rowCount, distinctCount, nullCount, boundCount uint64, boundValue sql.Row, mcvCounts []uint64, mcvs []sql.Row) *Bucket {
	return &Bucket{
		RowCnt:      rowCount,
		DistinctCnt: distinctCount,
		NullCnt:     nullCount,
		McvsCnt:     mcvCounts,
		BoundCnt:    boundCount,
		BoundVal:    boundValue,
		McvVals:     mcvs,
	}
}

type Bucket struct {
	RowCnt      uint64    `json:"row_count"`
	DistinctCnt uint64    `json:"distinct_count"`
	NullCnt     uint64    `json:"null_count"`
	McvsCnt     []uint64  `json:"mcvs_count"`
	BoundCnt    uint64    `json:"bound_count"`
	BoundVal    sql.Row   `json:"upper_bound"`
	McvVals     []sql.Row `json:"mcvs"`
}

var _ sql.HistogramBucket = (*Bucket)(nil)

func (b Bucket) RowCount() uint64 {
	return b.RowCnt
}

func (b Bucket) DistinctCount() uint64 {
	return b.DistinctCnt
}

func (b Bucket) NullCount() uint64 {
	return b.NullCnt
}

func (b Bucket) BoundCount() uint64 {
	return b.BoundCnt
}

func (b Bucket) UpperBound() sql.Row {
	return b.BoundVal
}

func (b Bucket) McvCounts() []uint64 {
	return b.McvsCnt
}

func (b Bucket) Mcvs() []sql.Row {
	return b.McvVals
}
