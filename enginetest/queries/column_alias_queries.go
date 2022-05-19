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

package queries

import (
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
)

var ColumnAliasQueries = []QueryTest{
	{
		Query: `SELECT i AS cOl FROM mytable`,
		ExpectedColumns: sql.Schema{
			{
				Name: "cOl",
				Type: sql.Int64,
			},
		},
		Expected: []sql.Row{
			{int64(1)},
			{int64(2)},
			{int64(3)},
		},
	},
	{
		Query: `SELECT i AS cOl, s as COL FROM mytable`,
		ExpectedColumns: sql.Schema{
			{
				Name: "cOl",
				Type: sql.Int64,
			},
			{
				Name: "COL",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
	},
	{
		// TODO: this is actually inconsistent with MySQL, which doesn't allow column aliases in the where clause
		Query: `SELECT i AS cOl, s as COL FROM mytable where cOl = 1`,
		ExpectedColumns: sql.Schema{
			{
				Name: "cOl",
				Type: sql.Int64,
			},
			{
				Name: "COL",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
		Expected: []sql.Row{
			{int64(1), "first row"},
		},
	},
	{
		Query: `SELECT s as COL1, SUM(i) COL2 FROM mytable group by s order by cOL2`,
		ExpectedColumns: sql.Schema{
			{
				Name: "COL1",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
			{
				Name: "COL2",
				Type: sql.Float64,
			},
		},
		// TODO: SUM should be integer typed for integers
		Expected: []sql.Row{
			{"first row", float64(1)},
			{"second row", float64(2)},
			{"third row", float64(3)},
		},
	},
	{
		Query: `SELECT s as COL1, SUM(i) COL2 FROM mytable group by col1 order by col2`,
		ExpectedColumns: sql.Schema{
			{
				Name: "COL1",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
			{
				Name: "COL2",
				Type: sql.Float64,
			},
		},
		Expected: []sql.Row{
			{"first row", float64(1)},
			{"second row", float64(2)},
			{"third row", float64(3)},
		},
	},
	{
		Query: `SELECT s as coL1, SUM(i) coL2 FROM mytable group by 1 order by 2`,
		ExpectedColumns: sql.Schema{
			{
				Name: "coL1",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
			{
				Name: "coL2",
				Type: sql.Float64,
			},
		},
		Expected: []sql.Row{
			{"first row", float64(1)},
			{"second row", float64(2)},
			{"third row", float64(3)},
		},
	},
	{
		Query: `SELECT s as Date, SUM(i) TimeStamp FROM mytable group by 1 order by 2`,
		ExpectedColumns: sql.Schema{
			{
				Name: "Date",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
			{
				Name: "TimeStamp",
				Type: sql.Float64,
			},
		},
		Expected: []sql.Row{
			{"first row", float64(1)},
			{"second row", float64(2)},
			{"third row", float64(3)},
		},
	},
}
