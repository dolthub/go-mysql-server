// Copyright 2020-2021 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// DeleteTests contains tests for deletes that implicitly target the single table mentioned
// in the from clause.
var DeleteTests = []WriteQueryTest{
	{
		WriteQuery:          "DELETE FROM mytable;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      nil,
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i = 2;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "first row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE I = 2;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "first row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i < 3;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i > 1;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "first row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i <= 2;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i >= 2;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "first row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE s = 'first row';",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE s <> 'dne';",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      nil,
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i in (2,3);",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "first row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE s LIKE '%row';",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      nil,
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE s = 'dne';",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable ORDER BY i ASC LIMIT 2;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable ORDER BY i DESC LIMIT 1;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "first row"}, {int64(2), "second row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable ORDER BY i DESC LIMIT 1 OFFSET 1;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "first row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE (i,s) = (1, 'first row');",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          `DELETE FROM tabletest where 's' = 'something'`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 0}}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "with t (n) as (select (1) from dual) delete from mytable where i in (select n from t)",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
		SelectQuery:         "select * from mytable order by i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		WriteQuery:          "with recursive t (n) as (select (1) from dual union all select n + 1 from t where n < 2) delete from mytable where i in (select n from t)",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 2}}},
		SelectQuery:         "select * from mytable order by i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{3, "third row"},
		},
	},
}

// DeleteJoinTests contains tests for deletes that explicitly list the table from which
// to delete, and whose source may contain joined table relations.
var DeleteJoinTests = []WriteQueryTest{
	{
		WriteQuery:          "DELETE mytable FROM mytable join tabletest where mytable.i=tabletest.i;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.UntypedSqlRow{{0, 3}},
	},
	{
		WriteQuery:          "DELETE MYTABLE FROM mytAble join tAbletest where mytable.i=tabletest.i;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.UntypedSqlRow{{0, 3}},
	},
	{
		WriteQuery:          "DELETE tabletest FROM mytable join tabletest where mytable.i=tabletest.i;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.UntypedSqlRow{{3, 0}},
	},
	{
		WriteQuery:          "DELETE t1 FROM mytable as t1 join tabletest where t1.i=tabletest.i;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.UntypedSqlRow{{0, 3}},
	},
	{
		WriteQuery:          "DELETE mytable, tabletest FROM mytable join tabletest where mytable.i=tabletest.i;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.UntypedSqlRow{{0, 0}},
	},
	{
		WriteQuery:          "DELETE MYTABLE, TABLETEST FROM mytable join tabletest where mytable.i=tabletest.i;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.UntypedSqlRow{{0, 0}},
	},
	{
		WriteQuery:          "DELETE mytable FROM mytable;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT count(*) FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{0}},
	},
	{
		WriteQuery:          "DELETE mytable FROM mytable WHERE i > 9999;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
		SelectQuery:         "SELECT count(*) FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{3}},
	},
	{
		WriteQuery:          "DELETE FROM mytable USING mytable inner join tabletest on mytable.i=tabletest.i;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.UntypedSqlRow{{0, 3}},
	},
	{
		WriteQuery:          "DELETE FROM tabletest USING mytable inner join tabletest on mytable.i=tabletest.i;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.UntypedSqlRow{{3, 0}},
	},
	{
		WriteQuery:          "DELETE FROM mytable, tabletest USING mytable inner join tabletest on mytable.i=tabletest.i;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.UntypedSqlRow{{0, 0}},
	},
	{
		WriteQuery:          "DELETE mytable FROM mytable join tabletest where mytable.i=tabletest.i and mytable.i = 2;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.UntypedSqlRow{{2, 3}},
	},
	{
		WriteQuery:          "DELETE mytable, tabletest FROM mytable join tabletest where mytable.i=tabletest.i and mytable.i = 2;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.UntypedSqlRow{{2, 2}},
	},
	{
		WriteQuery:          "DELETE tabletest, mytable FROM mytable join tabletest where mytable.i=tabletest.i and mytable.i = 2;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.UntypedSqlRow{{2, 2}},
	},
	{
		WriteQuery:          "DELETE mytable FROM mytable join (select 1 as i union all select 2 as i) dt where mytable.i=dt.i;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.UntypedSqlRow{{1, 3}},
	},
	{
		WriteQuery:          "with t (n) as (select (1) from dual) delete mytable from mytable join tabletest where mytable.i=tabletest.i and mytable.i in (select n from t)",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.UntypedSqlRow{{2, 3}},
	},
	{
		WriteQuery:          "with t (n) as (select (1) from dual) delete mytable, tabletest from mytable join tabletest where mytable.i=tabletest.i and mytable.i in (select n from t)",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.UntypedSqlRow{{2, 2}},
	},
	{
		// Single target table, join with table function
		WriteQuery:          "DELETE mytable FROM mytable join tabletest on mytable.i=tabletest.i join JSON_TABLE('[{\"x\": 1},{\"x\": 2}]', '$[*]' COLUMNS (x INT PATH '$.x')) as jt on jt.x=mytable.i;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.UntypedSqlRow{{1, 3}},
	},
	{
		// Multiple target tables, join with table function
		WriteQuery:          "DELETE mytable, tabletest FROM mytable join tabletest on mytable.i=tabletest.i join JSON_TABLE('[{\"x\": 1},{\"x\": 2}]', '$[*]' COLUMNS (x INT PATH '$.x')) as jt on jt.x=mytable.i;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.UntypedSqlRow{{1, 1}},
	},
}

var SpatialDeleteTests = []WriteQueryTest{
	{
		WriteQuery:          "DELETE FROM point_table;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM point_table;",
		ExpectedSelect:      nil,
	},
	{
		WriteQuery:          "DELETE FROM line_table;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM line_table;",
		ExpectedSelect:      nil,
	},
	{
		WriteQuery:          "DELETE FROM polygon_table;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM polygon_table;",
		ExpectedSelect:      nil,
	},
}

var DeleteErrorTests = []ScriptTest{
	{
		Name: "DELETE FROM error cases",
		Assertions: []ScriptTestAssertion{
			{
				// unknown table
				Query:          "DELETE FROM invalidtable WHERE x < 1;",
				ExpectedErrStr: "table not found: invalidtable",
			},
			{
				// invalid column
				Query:          "DELETE FROM mytable WHERE z = 'dne';",
				ExpectedErrStr: "column \"z\" could not be found in any table in scope",
			},
			{
				// missing binding
				Query:          "DELETE FROM mytable WHERE i = ?;",
				ExpectedErrStr: "unbound variable \"v1\" in query",
			},
			{
				// negative limit
				Query:          "DELETE FROM mytable LIMIT -1;",
				ExpectedErrStr: "syntax error at position 28 near 'LIMIT'",
			},
			{
				// negative offset
				Query:          "DELETE FROM mytable LIMIT 1 OFFSET -1;",
				ExpectedErrStr: "syntax error at position 37 near 'OFFSET'",
			},
			{
				// missing keyword from
				Query:          "DELETE mytable WHERE i = 1;",
				ExpectedErrStr: "syntax error at position 21 near 'WHERE'",
			},
			{
				// targets subquery alias
				Query:          "DELETE FROM (SELECT * FROM mytable) mytable WHERE i = 1;",
				ExpectedErrStr: "syntax error at position 14 near 'FROM'",
			},
		},
	},
	{
		Name: "DELETE FROM JOIN error cases",
		Assertions: []ScriptTestAssertion{
			{
				// targeting tables in multiple databases
				Query:          "DELETE mydb.mytable, test.other FROM mydb.mytable inner join test.other on mydb.mytable.i=test.other.pk;",
				ExpectedErrStr: "multiple databases specified as delete from targets",
			},
			{
				// unknown table in delete join
				Query:          "DELETE unknowntable FROM mytable WHERE i < 1;",
				ExpectedErrStr: "table not found: unknowntable",
			},
			{
				// invalid table in delete join
				Query:          "DELETE tabletest FROM mytable WHERE i < 1;",
				ExpectedErrStr: "table \"tabletest\" not found in DELETE FROM sources",
			},
			{
				// repeated table in delete join
				Query:          "DELETE mytable, mytable FROM mytable WHERE i < 1;",
				ExpectedErrStr: "duplicate tables specified as delete from targets",
			},
			{
				// targets join with no explicit target tables
				Query:          "DELETE FROM mytable one, mytable two WHERE one.i = 1;",
				ExpectedErrStr: "syntax error at position 24 near 'one'",
			},
			{
				// targets table function alias
				Query:          "DELETE jt FROM mytable join tabletest on mytable.i=tabletest.i join JSON_TABLE('[{\"x\": 1},{\"x\": 2}]', '$[*]' COLUMNS (x INT PATH '$.x')) as jt on jt.x=mytable.i;",
				ExpectedErrStr: "target table jt of the DELETE is not updatable",
			},
			{
				// targets valid table and table function alias
				Query:          "DELETE mytable, jt FROM mytable join tabletest on mytable.i=tabletest.i join JSON_TABLE('[{\"x\": 1},{\"x\": 2}]', '$[*]' COLUMNS (x INT PATH '$.x')) as jt on jt.x=mytable.i;",
				ExpectedErrStr: "target table jt of the DELETE is not updatable",
			},
		},
	},
}
