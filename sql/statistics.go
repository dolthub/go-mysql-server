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
	"time"
)

// StatisticsTable is a table that can provide information about its number of rows and other facts to improve query
// planning performance.
type StatisticsTable interface {
	Table
	// DataLength returns the length of the data file (varies by engine).
	DataLength(ctx *Context) (uint64, error)
	// RowCount returns the row count for this table
	RowCount(ctx *Context) (uint64, error)
}

type StatsReader interface {
	CatalogTable
	// Hist returns a HistogramMap providing statistics for a table's columns
	Hist(ctx *Context, db, table string) (HistogramMap, error)
	// RowCount returns a table's row count if the table implements sql.StatisticsTable,
	// false if the table does not, or an error if the table was not found.
	RowCount(ctx *Context, db, table string) (uint64, bool, error)
}

type StatsWriter interface {
	CatalogTable
	Analyze(ctx *Context, db, table string) error
}

type StatsReadWriter interface {
	StatsReader
	StatsWriter
}

// HistogramBucket represents a bucket in a histogram
// inspiration pulled from MySQL and Cockroach DB
type HistogramBucket struct {
	LowerBound float64 // inclusive
	UpperBound float64 // inclusive
	Frequency  float64
}
type StatisticsIndex interface {
	RowCount() uint64
}
// Histogram is all statistics we care about for each column
type Histogram struct {
	Buckets       []*HistogramBucket
	Mean          float64
	Min           float64
	Max           float64
	Count         uint64
	NullCount     uint64
	DistinctCount uint64
}

// HistogramMap is a map from column name to associated histogram
type HistogramMap map[string]*Histogram

// TableStatistics provides access to statistical information about the values stored in a table
type TableStatistics struct {
	// RowCount returns the number of rows in this table.
	RowCount uint64
	// CreatedAt returns the time at which the current statistics for this table were generated.
	CreatedAt time.Time
	// Histograms returns a map from all column names to their associated histograms.
	Histograms HistogramMap
}

func (ts *TableStatistics) Histogram(colName string) (*Histogram, error) {
	if res, ok := ts.Histograms[colName]; ok {
		return res, nil
	}
	return &Histogram{}, fmt.Errorf("column %s not found", colName)
}
