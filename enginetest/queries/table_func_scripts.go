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

package queries

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

var TableFunctionScriptTests = []ScriptTest{
	{
		Name:        "undefined table function",
		Query:       "SELECT * from does_not_exist('q', 123);",
		ExpectedErr: sql.ErrTableFunctionNotFound,
	},
	{
		Name:        "projection of non-existent column from table function",
		Query:       "SELECT none from simple_TABLE_function(123);",
		ExpectedErr: sql.ErrColumnNotFound,
	},
	{
		Name:        "projection of non-existent qualified column from table function",
		Query:       "SELECT simple_TABLE_function.none from simple_TABLE_function(123);",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Name:        "projection of non-existent aliased qualified column from table function",
		Query:       "SELECT stf.none from simple_TABLE_function(123) as stf;",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Name:        "projection of non-existent aliased qualified column from table function in join",
		Query:       "SELECT stf1.none from simple_TABLE_function(123) as stf1 join simple_TABLE_function(123) stf2;",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Name:        "alias overwrites original name",
		Query:       "SELECT simple_table_function.none from simple_TABLE_function(123) stf;",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Name:        "projection of aliased non-existent qualified column from table function",
		Query:       "SELECT stf.none as none from simple_TABLE_function(123) as stf;",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Name:     "basic table function",
		Query:    "SELECT * from simple_table_function(123);",
		Expected: []sql.Row{{"foo", 123}},
	},
	{
		Name:     "basic table function",
		Query:    "SELECT * from simple_TABLE_function(123);",
		Expected: []sql.Row{{"foo", 123}},
	},
	{
		Name:     "aggregate function applied to a table function",
		Query:    "SELECT count(*) from simple_TABLE_function(123);",
		Expected: []sql.Row{{1}},
	},
	{
		Name:     "projection of table function",
		Query:    "SELECT one from simple_TABLE_function(123);",
		Expected: []sql.Row{{"foo"}},
	},
	{
		Name:     "nested expressions in table function arguments",
		Query:    "SELECT * from simple_TABLE_function(concat('f', 'o', 'o'));",
		Expected: []sql.Row{{"foo", 123}},
	},
	{
		Name:     "filtering table function results",
		Query:    "SELECT * from simple_TABLE_function(123) where one='foo';",
		Expected: []sql.Row{{"foo", 123}},
	},
	{
		Name:     "filtering table function results to no results",
		Query:    "SELECT * from simple_TABLE_function(123) where one='none';",
		Expected: []sql.Row{},
	},
	{
		Name:     "grouping table function results",
		Query:    "SELECT count(one) from simple_TABLE_function(123) group by one;",
		Expected: []sql.Row{{1}},
	},
	{
		Name:     "table function as subquery",
		Query:    "SELECT * from (select * from simple_TABLE_function(123)) as tf;",
		Expected: []sql.Row{{"foo", 123}},
	},
	{
		Query:    "select * from sequence_table('x', 5)",
		Expected: []sql.Row{{0}, {1}, {2}, {3}, {4}},
	},
	{
		Query:    "select sequence_table.x from sequence_table('x', 5)",
		Expected: []sql.Row{{0}, {1}, {2}, {3}, {4}},
	},
	{
		Query:    "select sequence_table.x from sequence_table('x', 5)",
		Expected: []sql.Row{{0}, {1}, {2}, {3}, {4}},
	},
	{
		Query:       "select * from sequence_table('x', 5) join sequence_table('y', 5) on x = y",
		ExpectedErr: sql.ErrDuplicateAliasOrTable,
	},
	{
		Query:       "select * from sequence_table('x', 5) join sequence_table('y', 5) on x = 0",
		ExpectedErr: sql.ErrDuplicateAliasOrTable,
	},
	{
		Query:    "select * from sequence_table('x', 2) where x is not null",
		Expected: []sql.Row{{0}, {1}},
	},
	{
		Query:    "select seq.x from sequence_table('x', 5) as seq",
		Expected: []sql.Row{{0}, {1}, {2}, {3}, {4}},
	},
	{
		Query:    "select seq.x from sequence_table('x', 5) seq",
		Expected: []sql.Row{{0}, {1}, {2}, {3}, {4}},
	},
	{
		Query:       "select not_seq.x from sequence_table('x', 5) as seq",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:           "select /*+ MERGE_JOIN(seq1,seq2) */ seq1.x, seq2.y from sequence_table('x', 5) seq1 join sequence_table('y', 5) seq2 on seq1.x = seq2.y",
		Expected:        []sql.Row{{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}},
		ExpectedIndexes: []string{"y", "x"},
	},
	{
		Query:           "select /*+ LOOKUP_JOIN(seq1,seq2) */ seq1.x, seq2.y from sequence_table('x', 5) seq1 join sequence_table('y', 5) seq2 on seq1.x = seq2.y",
		Expected:        []sql.Row{{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}},
		ExpectedIndexes: []string{"x"},
	},
	{
		Query:           "select /*+ MERGE_JOIN(seq1,seq2) */ * from sequence_table('x', 5) seq1 join sequence_table('y', 5) seq2 on x = 0",
		Expected:        []sql.Row{{0, 0}, {0, 1}, {0, 2}, {0, 3}, {0, 4}},
		ExpectedIndexes: []string{"x"},
	},
	{
		Query:           "select /*+ LOOKUP_JOIN(seq1,seq2) */ * from sequence_table('x', 5) seq1 join sequence_table('y', 5) seq2 on x = 0",
		Expected:        []sql.Row{{0, 0}, {0, 1}, {0, 2}, {0, 3}, {0, 4}},
		ExpectedIndexes: []string{"x"},
	},
	{
		Query:    "with cte as (select seq.x from sequence_table('x', 5) seq) select cte.x from cte",
		Expected: []sql.Row{{0}, {1}, {2}, {3}, {4}},
	},
	{
		Query:    "select sq.x from (select seq.x from sequence_table('x', 5) seq) sq",
		Expected: []sql.Row{{0}, {1}, {2}, {3}, {4}},
	},
	{
		Query:       "select seq.x from (select seq.x from sequence_table('x', 5) seq) sq",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:    "select sq.xx from (select seq.x as xx from sequence_table('x', 5) seq) sq",
		Expected: []sql.Row{{0}, {1}, {2}, {3}, {4}},
	},
	{
		Name:            "sequence_table allows point lookups",
		Query:           "select * from sequence_table('x', 5) where x = 2",
		Expected:        []sql.Row{{2}},
		ExpectedIndexes: []string{"x"},
	},
	{
		Name:            "sequence_table allows range lookups",
		Query:           "select * from sequence_table('x', 5) where x >= 1 and x <= 3",
		Expected:        []sql.Row{{1}, {2}, {3}},
		ExpectedIndexes: []string{"x"},
	},
	{
		Name:     "basic behavior of point_lookup_table",
		Query:    "select seq.x from point_lookup_table('x', 5) seq",
		Expected: []sql.Row{{0}, {1}, {2}, {3}, {4}},
	},
	{
		Name:            "point_lookup_table allows point lookups",
		Query:           "select * from point_lookup_table('x', 5) where x = 2",
		Expected:        []sql.Row{{2}},
		ExpectedIndexes: []string{"x"},
	},
	{
		Name:            "point_lookup_table disallows range lookups",
		Query:           "select * from point_lookup_table('x', 5) where x >= 1 and x <= 3",
		Expected:        []sql.Row{{1}, {2}, {3}},
		ExpectedIndexes: []string{},
	},
	{
		Name:            "point_lookup_table disallows merge join",
		Query:           "select /*+ MERGE_JOIN(l,r) */ * from point_lookup_table('x', 5) l join point_lookup_table('y', 5) r where x = y",
		Expected:        []sql.Row{{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}},
		JoinTypes:       []plan.JoinType{plan.JoinTypeLookup},
		ExpectedIndexes: []string{"x"},
	},
}
