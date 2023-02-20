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

// DeleteJoinTests contains tests for deletes that explicitly list the table from which
// to delete, and whose source may contain joined table relations.
var DeleteJoinTests = []WriteQueryTest{
	{
		WriteQuery:          "DELETE mytable FROM mytable join tabletest where mytable.i=tabletest.i;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.Row{{0, 3}},
	},
	{
		WriteQuery:          "DELETE MYTABLE FROM mytAble join tAbletest where mytable.i=tabletest.i;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.Row{{0, 3}},
	},
	{
		WriteQuery:          "DELETE tabletest FROM mytable join tabletest where mytable.i=tabletest.i;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.Row{{3, 0}},
	},
	{
		// TODO: This test is failing – we aren't able to see the alias name yet for some reason...
		WriteQuery:          "DELETE t1 FROM mytable as t1 join tabletest where t1.i=tabletest.i;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.Row{{0, 3}},
	},
	{
		WriteQuery:          "DELETE mytable, tabletest FROM mytable join tabletest where mytable.i=tabletest.i;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.Row{{0, 0}},
	},
	{
		WriteQuery:          "DELETE MYTABLE, TABLETEST FROM mytable join tabletest where mytable.i=tabletest.i;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.Row{{0, 0}},
	},
	{
		WriteQuery:          "DELETE mytable FROM mytable;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT count(*) FROM mytable;",
		ExpectedSelect:      []sql.Row{{0}},
	},
	{
		WriteQuery:          "DELETE mytable FROM mytable WHERE i > 9999;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SELECT count(*) FROM mytable;",
		ExpectedSelect:      []sql.Row{{3}},
	},
	{
		WriteQuery:          "DELETE mytable FROM mytable join tabletest where mytable.i=tabletest.i and mytable.i = 2;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.Row{{2, 3}},
	},
	{
		WriteQuery:          "DELETE mytable, tabletest FROM mytable join tabletest where mytable.i=tabletest.i and mytable.i = 2;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.Row{{2, 2}},
	},
	{
		WriteQuery:          "DELETE tabletest, mytable FROM mytable join tabletest where mytable.i=tabletest.i and mytable.i = 2;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.Row{{2, 2}},
	},
	{
		WriteQuery:          "with t (n) as (select (1) from dual) delete mytable from mytable join tabletest where mytable.i=tabletest.i and mytable.i in (select n from t)",
		ExpectedWriteResult: []sql.Row{{types.OkResult{RowsAffected: 1}}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.Row{{2, 3}},
	},
	{
		WriteQuery:          "with t (n) as (select (1) from dual) delete mytable, tabletest from mytable join tabletest where mytable.i=tabletest.i and mytable.i in (select n from t)",
		ExpectedWriteResult: []sql.Row{{types.OkResult{RowsAffected: 1}}},
		SelectQuery:         "SELECT (select count(*) FROM mytable), (SELECT count(*) from tabletest);",
		ExpectedSelect:      []sql.Row{{2, 2}},
	},
}

// DeleteTests contains tests for deletes that implicitly target the single table mentioned
// in the from clause.
var DeleteTests = []WriteQueryTest{
	{
		WriteQuery:          "DELETE FROM mytable;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      nil,
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i = 2;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE I = 2;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i < 3;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i > 1;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i <= 2;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i >= 2;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE s = 'first row';",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE s <> 'dne';",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      nil,
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i in (2,3);",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE s LIKE '%row';",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      nil,
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE s = 'dne';",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE i = 'invalid';",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable ORDER BY i ASC LIMIT 2;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable ORDER BY i DESC LIMIT 1;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}, {int64(2), "second row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable ORDER BY i DESC LIMIT 1 OFFSET 1;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "DELETE FROM mytable WHERE (i,s) = (1, 'first row');",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          `DELETE FROM tabletest where 's' = 'something'`,
		ExpectedWriteResult: []sql.Row{{types.OkResult{RowsAffected: 0}}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "with t (n) as (select (1) from dual) delete from mytable where i in (select n from t)",
		ExpectedWriteResult: []sql.Row{{types.OkResult{RowsAffected: 1}}},
		SelectQuery:         "select * from mytable order by i",
		ExpectedSelect: []sql.Row{
			sql.NewRow(2, "second row"),
			sql.NewRow(3, "third row"),
		},
	},
	{
		WriteQuery:          "with recursive t (n) as (select (1) from dual union all select n + 1 from t where n < 2) delete from mytable where i in (select n from t)",
		ExpectedWriteResult: []sql.Row{{types.OkResult{RowsAffected: 2}}},
		SelectQuery:         "select * from mytable order by i",
		ExpectedSelect: []sql.Row{
			sql.NewRow(3, "third row"),
		},
	},
}

var SpatialDeleteTests = []WriteQueryTest{
	{
		WriteQuery:          "DELETE FROM point_table;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM point_table;",
		ExpectedSelect:      nil,
	},
	{
		WriteQuery:          "DELETE FROM line_table;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM line_table;",
		ExpectedSelect:      nil,
	},
	{
		WriteQuery:          "DELETE FROM polygon_table;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM polygon_table;",
		ExpectedSelect:      nil,
	},
}

// TODO: Add tests for delete from join errors:
//   - targeting tables that don't exist
//   - targeting tables not in the join
var DeleteErrorTests = []GenericErrorQueryTest{
	{
		Name:  "invalid table",
		Query: "DELETE FROM invalidtable WHERE x < 1;",
	},
	{
		Name:  "invalid column",
		Query: "DELETE FROM mytable WHERE z = 'dne';",
	},
	{
		Name:  "missing binding",
		Query: "DELETE FROM mytable WHERE i = ?;",
	},
	{
		Name:  "negative limit",
		Query: "DELETE FROM mytable LIMIT -1;",
	},
	{
		Name:  "negative offset",
		Query: "DELETE FROM mytable LIMIT 1 OFFSET -1;",
	},
	{
		Name:  "missing keyword from",
		Query: "DELETE mytable WHERE id = 1;",
	},
	{
		Name:  "targets join",
		Query: "DELETE FROM mytable one, mytable two WHERE id = 1;",
	},
	{
		Name:  "targets subquery alias",
		Query: "DELETE FROM (SELECT * FROM mytable) mytable WHERE id = 1;",
	},
}
