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

var BrokenAliasQueries = []QueryTest{
	{
		// The dual table's schema can conflict with aliases
		//
		Query:    `select "foo" as dummy, (select dummy)`,
		Expected: []sql.Row{{"foo", "foo"}},
	},
	{
		// This query crashes Dolt with "panic: unexpected type x"
		// Seems likely to be coming from dual table's schema and dummy value.
		// https://github.com/dolthub/dolt/issues/4256
		Query:    `SELECT *, (select pk union select pk) as a from mydb;`,
		Expected: []sql.Row{{1, 1}},
	},
	{
		// This query actually works correctly in GMS, but not in Dolt
		// Removing the second "as a" makes it work in Dolt. This seems to happen because GMS uses a map of
		// aliases by name and the second a alias overwrites the one that should be used. GMS should not allow
		// the subquery to see the second "as a" alias, since that expression should only see aliases defined
		// in previous expressions (i.e. forward-references are disallowed).
		Query:    `SELECT 1 as a, (select a) as a;`,
		Expected: []sql.Row{{1, 1}},
	},
	{
		// Fails with an unresolved *plan.Project node error
		// The second Project in the union subquery doens't seem to get its alias reference resolved
		Query:    `SELECT 1 as a, (select a union select a) as b;`,
		Expected: []sql.Row{{1, 1}},
	},
	{
		// GMS executes this query, but it is not valid because of the forward ref of alias b.
		// GMS should return an error about an invalid forward-ref.
		Query:    `select 1 as a, (select b), 0 as b;`,
		Expected: []sql.Row{},
	},
	{
		// GMS returns the error "found HAVING clause with no GROUP BY", but MySQL executes
		// this query without any problems.
		Query:    "select t1.pk as a from mydb as t1 having a = t1.pk;",
		Expected: []sql.Row{{1}},
	},
	{
		// GMS returns "expression 'dt.two' doesn't appear in the group by expressions", but MySQL will execute
		// this query. It does seem odd that we are not selecting dt.two though. Perhaps MySQL sees those values are
		// always going to be literals and optimizes?
		Query:    "select 1 as a, one + 1 as mod1, dt.* from mydb as t1, (select 1, 2 from t) as dt (one, two) where dt.one > 0 group by one;",
		Expected: []sql.Row{{1}},
	},
}

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
