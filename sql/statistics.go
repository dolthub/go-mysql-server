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

package sql

import (
	"fmt"
	"strings"
	"time"
)

// StatisticsTable is a table that can provide information about its number of rows and other facts to improve query
// planning performance.
type StatisticsTable interface {
	Table
	// DataLength returns the length of the data file (varies by engine).
	DataLength(ctx *Context) (uint64, error)
	// RowCount returns the row count for this table and whether the count is exact
	RowCount(ctx *Context) (uint64, bool, error)
}

// StatsProvider is a catalog extension for databases that can
// build and provide index statistics.
type StatsProvider interface {
	// GetTableStats returns all statistics for the table
	GetTableStats(ctx *Context, db, table string) ([]Statistic, error)
	// RefreshTableStats updates all statistics associated with a given table
	RefreshTableStats(ctx *Context, table Table, db string) error
	// SetStats updates or overwrites a set of table statistics
	SetStats(ctx *Context, stats Statistic) error
	// GetStats fetches a set of statistics for a set of table columns
	GetStats(ctx *Context, qual StatQualifier, cols []string) (Statistic, bool)
	// DropStats deletes a set of column statistics
	DropStats(ctx *Context, qual StatQualifier, cols []string) error
	// DropAllStats deletes all database statistics
	DropDbStats(ctx *Context, db string, flush bool) error
	// RowCount returns the number of rows in a table
	RowCount(ctx *Context, db, table string) (uint64, error)
	// DataLength returns the estimated size of each row in the table
	DataLength(ctx *Context, db, table string) (uint64, error)
}

type IndexClass uint8

const (
	IndexClassDefault = iota
	IndexClassSpatial
	IndexClassFulltext
)

// Statistic is the top-level interface for accessing cardinality and
// costing estimates for an index prefix.
type Statistic interface {
	JSONWrapper
	MutableStatistic
	RowCount() uint64
	DistinctCount() uint64
	NullCount() uint64
	AvgSize() uint64
	CreatedAt() time.Time
	Columns() []string
	Types() []Type
	Qualifier() StatQualifier
	Histogram() Histogram
	IndexClass() IndexClass
	FuncDeps() *FuncDepSet
	ColSet() ColSet
	LowerBound() Row
}

type MutableStatistic interface {
	WithColSet(ColSet) Statistic
	WithFuncDeps(*FuncDepSet) Statistic
	WithHistogram(Histogram) (Statistic, error)
	WithDistinctCount(uint64) Statistic
	WithRowCount(uint64) Statistic
	WithNullCount(uint64) Statistic
	WithAvgSize(uint64) Statistic
	WithLowerBound(Row) Statistic
}

func NewQualifierFromString(q string) (StatQualifier, error) {
	parts := strings.Split(q, ".")
	if len(parts) < 3 {
		return StatQualifier{}, fmt.Errorf("invalid qualifier string: '%s', expected '<database>.<table>.<index>'", q)
	}
	return StatQualifier{Database: parts[0], Tab: parts[1], Idx: parts[2]}, nil
}

func NewStatQualifier(db, table, index string) StatQualifier {
	return StatQualifier{Database: db, Tab: table, Idx: index}
}

// StatQualifier is the namespace hierarchy for a given statistic.
// The qualifier and set of columns completely describes a unique stat.
type StatQualifier struct {
	Database string `json:"database"`
	Tab      string `json:"table"`
	Idx      string `json:"index"`
}

func (q StatQualifier) String() string {
	if q.Idx != "" {
		return fmt.Sprintf("%s.%s.%s", q.Database, q.Tab, q.Idx)
	}
	return fmt.Sprintf("%s.%s", q.Database, q.Tab)
}

func (q StatQualifier) Empty() bool {
	return q.Idx == "" || q.Tab == "" || q.Database == ""
}

func (q StatQualifier) Db() string {
	return q.Database
}

func (q StatQualifier) Table() string {
	return q.Tab
}

func (q StatQualifier) Index() string {
	return q.Idx
}

// Histogram is a collection of non-overlapping buckets that
// estimate the costing statistics for an index prefix.
// Note that a non-unique key can cross bucket boundaries.
type Histogram []HistogramBucket

func (h Histogram) IsEmpty() bool {
	return len(h) == 0
}

func (h Histogram) ToInterface() interface{} {
	ret := make([]interface{}, len(h))
	for i, b := range h {
		var upperBound Row
		for _, v := range b.UpperBound() {
			upperBound = append(upperBound, v)
		}
		mcvs := make([]Row, len(b.Mcvs()))
		for i, mcv := range b.Mcvs() {
			var row Row
			for _, v := range mcv {
				row = append(row, v)
			}
			mcvs[i] = row
		}
		ret[i] = map[string]interface{}{
			"row_count":      b.RowCount(),
			"null_count":     b.NullCount(),
			"distinct_count": b.DistinctCount(),
			"bound_count":    b.BoundCount(),
			"mcv_counts":     b.McvCounts(),
			"mcvs":           mcvs,
			"upper_bound":    upperBound,
		}
	}
	return ret
}

func (h Histogram) DebugString() string {
	var bounds []string
	var cnts []int
	var allCnt int
	for _, bucket := range h {
		cnt := int(bucket.RowCount())
		var key []string
		for _, v := range bucket.UpperBound() {
			key = append(key, fmt.Sprintf("%v", v))
		}
		bounds = append(bounds, strings.Join(key, ","))
		allCnt += cnt
		cnts = append(cnts, cnt)
	}

	flatten := 50 / float64(allCnt)
	b := strings.Builder{}
	b.WriteString("histogram:\n")
	for j, bound := range bounds {
		b.WriteString(bound + ": ")
		for i := 0; i < int(float64(cnts[j])*flatten); i++ {
			b.WriteString("*")
		}
		fmt.Fprintf(&b, "(%d)\n", cnts[j])
	}
	return b.String()
}

// HistogramBucket contains statistics for a fragment of an
// index's keyspace.
type HistogramBucket interface {
	RowCount() uint64
	DistinctCount() uint64
	NullCount() uint64
	BoundCount() uint64
	UpperBound() Row
	McvCounts() []uint64
	// Mcvs are the "most common values" (keys) in the index
	Mcvs() []Row
}

// JSONWrapper is an integrator specific implementation of a JSON field value.
// The query engine can utilize these optimized access methods improve performance
// by minimizing the need to unmarshall a JSONWrapper into a JSONDocument.
type JSONWrapper interface {
	// ToInterface converts a JSONWrapper to an interface{} of simple types
	ToInterface() interface{}
}
