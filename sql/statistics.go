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

import "github.com/dolthub/go-mysql-server/sql/stats"

// StatisticsTable is a table that can provide information about its number of rows and other facts to improve query
// planning performance.
type StatisticsTable interface {
	Table
	// DataLength returns the length of the data file (varies by engine).
	DataLength(ctx *Context) (uint64, error)
	// RowCount returns the row count for this table
	RowCount(ctx *Context) (uint64, error)
}

type StatsProvider interface {
	// GetTableStats returns all statistics for the table
	GetTableStats(ctx *Context, db, table string) ([]*stats.Stats, error)
	// RefreshTableStats updates all statistics associated with a given table
	RefreshTableStats(ctx *Context, table Table, db string) error
	// SetStats updates or overwrites a set of table statistics
	SetStats(ctx *Context, db, table string, stats *stats.Stats) error
	// GetStats fetches a set of statistics for a set of table columns
	GetStats(ctx *Context, db, table string, cols []string) (*stats.Stats, bool)
	// DropStats deletes a set of column statistics
	DropStats(ctx *Context, db, table string, cols []string) error
	// RowCount returns the number of rows in a table
	RowCount(ctx *Context, db, table string) (uint64, error)
	// DataLength returns the estimated size of each row in the table
	DataLength(ctx *Context, db, table string) (uint64, error)
}
