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
	// RowCount returns the number of rows in a table
	RowCount(ctx *Context, db, table string) (uint64, error)
	// DataLength returns the estimated size of each row in the table
	DataLength(ctx *Context, db, table string) (uint64, error)
}

// Statistic is the top-level interface for accessing cardinality and
// costing estimates for an index prefix.
type Statistic interface {
	JSONWrapper
	RowCount() uint64
	DistinctCount() uint64
	NullCount() uint64
	AvgSize() uint64
	CreatedAt() time.Time
	Columns() []string
	Types() []Type
	Qualifier() StatQualifier
	Histogram() Histogram
}

func NewQualifierFromString(q string) (StatQualifier, error) {
	parts := strings.Split(q, ".")
	if len(parts) < 3 {
		return StatQualifier{}, fmt.Errorf("invalid qualifier string: '%s', expected '<database>.<table>.<index>'", q)
	}
	return StatQualifier{db: parts[0], table: parts[1], index: parts[2]}, nil
}

func NewStatQualifier(db, table, index string) StatQualifier {
	return StatQualifier{db: db, table: table, index: index}
}

// StatQualifier is the namespace hierarchy for a given statistic.
// The qualifier and set of columns completely describes a unique stat.
type StatQualifier struct {
	db    string
	table string
	index string
}

func (q StatQualifier) String() string {
	if q.index != "" {
		return fmt.Sprintf("%s.%s.%s", q.db, q.table, q.index)
	}
	return fmt.Sprintf("%s.%s", q.db, q.table)
}

func (q StatQualifier) Db() string {
	return q.db
}

func (q StatQualifier) Table() string {
	return q.table
}

func (q StatQualifier) Index() string {
	return q.index
}

// Histogram is a collection of non-overlapping buckets that
// estimate the costing statistics for an index prefix.
// Note that a non-unique key can cross bucket boundaries.
type Histogram []HistogramBucket

func (h Histogram) ToInterface() interface{} {
	ret := make([]interface{}, len(h))
	for i, b := range h {
		upperBound := make([]interface{}, len(b.UpperBound()))
		for i, v := range b.UpperBound() {
			upperBound[i] = v
		}
		mcvs := make([][]interface{}, len(b.Mcvs()))
		for i, mcv := range b.Mcvs() {
			mcvs[i] = make([]interface{}, len(mcv))
			for j, v := range mcv {
				mcvs[i][j] = v
			}
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

// HistogramBucket contains statistics for a fragment of an
// index's keyspace.
type HistogramBucket interface {
	RowCount() uint64
	DistinctCount() uint64
	NullCount() uint64
	BoundCount() uint64
	UpperBound() Row
	McvCounts() []uint64
	Mcvs() []Row
}

// JSONWrapper is an integrator specific implementation of a JSON field value.
// The query engine can utilize these optimized access methods improve performance
// by minimizing the need to unmarshall a JSONWrapper into a JSONDocument.
type JSONWrapper interface {
	// ToInterface converts a JSONWrapper to an interface{} of simple types
	ToInterface() interface{}
}
