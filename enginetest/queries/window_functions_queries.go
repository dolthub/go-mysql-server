// Copyright 2026 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// WindowFunctionsScriptTests tests window function queries such as rank, dense_rank, percent_rank,
// first_value, last_value, lead, lag, and the bitwise aggregate functions.
var WindowFunctionsScriptTests = []ScriptTest{
	{
		Name: "case-sensitive literals in window expressions",
		Query: `SELECT
			FIRST_VALUE(ASCII('a')) OVER (),
			FIRST_VALUE(ASCII('A')) OVER ()`,
		Expected: []sql.Row{{uint8(97), uint8(65)}},
	},
	{
		Name: "INET_NTOA round trip above signed 32-bit range",
		SetUpScript: []string{
			"CREATE TABLE inet_ntoa_test (ip VARCHAR(15))",
			"INSERT INTO inet_ntoa_test VALUES ('192.0.2.1')",
		},
		Query: `SELECT INET_NTOA(
			FIRST_VALUE(INET_ATON(ip)) OVER ()
		) FROM inet_ntoa_test`,
		Expected: []sql.Row{{"192.0.2.1"}},
	},
	{
		Name: "ceil and floor do not mutate shared decimal window results",
		SetUpScript: []string{
			"CREATE TABLE decimal_window_values (id BIGINT, d DECIMAL(10,2))",
			"INSERT INTO decimal_window_values VALUES (1, 12.34), (2, 12.34), (3, -12.34), (4, -12.34)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT id, d, CEIL(d), FLOOR(d) FROM (SELECT id, MAX(d) OVER (PARTITION BY d) AS d FROM decimal_window_values) s ORDER BY id",
				Expected: []sql.Row{
					{int64(1), "12.34", int64(13), int64(12)},
					{int64(2), "12.34", int64(13), int64(12)},
					{int64(3), "-12.34", int64(-12), int64(-13)},
					{int64(4), "-12.34", int64(-12), int64(-13)},
				},
			},
		},
	},
	{
		Name: "window functions, empty table",
		SetUpScript: []string{
			"CREATE TABLE empty_tbl (a int, b int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SELECT a, rank() over (order by b) FROM empty_tbl order by a`,
				Expected: []sql.Row{},
			},
			{
				Query:    `SELECT a, dense_rank() over (order by b) FROM empty_tbl order by a`,
				Expected: []sql.Row{},
			},
			{
				Query:    `SELECT a, percent_rank() over (order by b) FROM empty_tbl order by a`,
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "window functions, rank/dense_rank/percent_rank partitioned by subject",
		SetUpScript: []string{
			"CREATE TABLE results (name varchar(20), subject varchar(20), mark int)",
			"INSERT INTO results VALUES ('Pratibha', 'Maths', 100),('Ankita','Science',80),('Swarna','English',100),('Ankita','Maths',65),('Pratibha','Science',80),('Swarna','Science',50),('Pratibha','English',70),('Swarna','Maths',85),('Ankita','English',90)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT subject, name, mark, rank() OVER (partition by subject order by mark desc ) FROM results order by subject, mark desc, name`,
				Expected: []sql.Row{
					{"English", "Swarna", 100, uint64(1)},
					{"English", "Ankita", 90, uint64(2)},
					{"English", "Pratibha", 70, uint64(3)},
					{"Maths", "Pratibha", 100, uint64(1)},
					{"Maths", "Swarna", 85, uint64(2)},
					{"Maths", "Ankita", 65, uint64(3)},
					{"Science", "Ankita", 80, uint64(1)},
					{"Science", "Pratibha", 80, uint64(1)},
					{"Science", "Swarna", 50, uint64(3)},
				},
			},
			{
				Query: `SELECT subject, name, mark, dense_rank() OVER (partition by subject order by mark desc ) FROM results order by subject, mark desc, name`,
				Expected: []sql.Row{
					{"English", "Swarna", 100, uint64(1)},
					{"English", "Ankita", 90, uint64(2)},
					{"English", "Pratibha", 70, uint64(3)},
					{"Maths", "Pratibha", 100, uint64(1)},
					{"Maths", "Swarna", 85, uint64(2)},
					{"Maths", "Ankita", 65, uint64(3)},
					{"Science", "Ankita", 80, uint64(1)},
					{"Science", "Pratibha", 80, uint64(1)},
					{"Science", "Swarna", 50, uint64(2)},
				},
			},
			{
				Query: `SELECT subject, name, mark, percent_rank() OVER (partition by subject order by mark desc ) FROM results order by subject, mark desc, name`,
				Expected: []sql.Row{
					{"English", "Swarna", 100, float64(0)},
					{"English", "Ankita", 90, float64(0.5)},
					{"English", "Pratibha", 70, float64(1)},
					{"Maths", "Pratibha", 100, float64(0)},
					{"Maths", "Swarna", 85, float64(0.5)},
					{"Maths", "Ankita", 65, float64(1)},
					{"Science", "Ankita", 80, float64(0)},
					{"Science", "Pratibha", 80, float64(0)},
					{"Science", "Swarna", 50, float64(1)},
				},
			},
		},
	},
	{
		Name: "window functions, rank/percent_rank/first_value/last_value/lead/lag",
		SetUpScript: []string{
			"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER, c integer)",
			"INSERT INTO t1 VALUES (0,0,0), (1,1,1), (2,2,0), (3,0,0), (4,1,0), (5,3,0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT a, percent_rank() over (order by b) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, 0.0},
					{1, 0.4},
					{2, 0.8},
					{3, 0.0},
					{4, 0.4},
					{5, 1.0},
				},
			},
			{
				Query: `SELECT a, rank() over (order by b) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, uint64(1)},
					{1, uint64(3)},
					{2, uint64(5)},
					{3, uint64(1)},
					{4, uint64(3)},
					{5, uint64(6)},
				},
			},
			{
				Query: `SELECT a, dense_rank() over (order by b) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, uint64(1)},
					{1, uint64(2)},
					{2, uint64(3)},
					{3, uint64(1)},
					{4, uint64(2)},
					{5, uint64(4)},
				},
			},
			{
				Query: `SELECT a, percent_rank() over (order by b desc) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, 0.8},
					{1, 0.4},
					{2, 0.2},
					{3, 0.8},
					{4, 0.4},
					{5, 0.0},
				},
			},
			{
				Query: `SELECT a, rank() over (order by b desc) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, uint64(5)},
					{1, uint64(3)},
					{2, uint64(2)},
					{3, uint64(5)},
					{4, uint64(3)},
					{5, uint64(1)},
				},
			},
			{
				Query: `SELECT a, dense_rank() over (order by b desc) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, uint64(4)},
					{1, uint64(3)},
					{2, uint64(2)},
					{3, uint64(4)},
					{4, uint64(3)},
					{5, uint64(1)},
				},
			},
			{
				Query: `SELECT a, percent_rank() over (partition by c order by b) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, 0.0},
					{1, 0.0},
					{2, 0.75},
					{3, 0.0},
					{4, 0.5},
					{5, 1.0},
				},
			},
			{
				Query: `SELECT a, rank() over (partition by c order by b) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, uint64(1)},
					{1, uint64(1)},
					{2, uint64(4)},
					{3, uint64(1)},
					{4, uint64(3)},
					{5, uint64(5)},
				},
			},
			{
				Query: `SELECT a, dense_rank() over (partition by c order by b) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, uint64(1)},
					{1, uint64(1)},
					{2, uint64(3)},
					{3, uint64(1)},
					{4, uint64(2)},
					{5, uint64(4)},
				},
			},
			{
				Query: `SELECT a, percent_rank() over (partition by b order by c) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, 0.0},
					{1, 1.0},
					{2, 0.0},
					{3, 0.0},
					{4, 0.0},
					{5, 0.0},
				},
			},
			{
				Query: `SELECT a, rank() over (partition by b order by c) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, uint64(1)},
					{1, uint64(2)},
					{2, uint64(1)},
					{3, uint64(1)},
					{4, uint64(1)},
					{5, uint64(1)},
				},
			},
			{
				Query: `SELECT a, dense_rank() over (partition by b order by c) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, uint64(1)},
					{1, uint64(2)},
					{2, uint64(1)},
					{3, uint64(1)},
					{4, uint64(1)},
					{5, uint64(1)},
				},
			},
			{
				// no order by clause -> all rows are peers
				Query: `SELECT a, percent_rank() over (partition by b) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, 0.0},
					{1, 0.0},
					{2, 0.0},
					{3, 0.0},
					{4, 0.0},
					{5, 0.0},
				},
			},
			{
				// no order by clause -> all rows are peers
				Query: `SELECT a, rank() over (partition by b) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, uint64(1)},
					{1, uint64(1)},
					{2, uint64(1)},
					{3, uint64(1)},
					{4, uint64(1)},
					{5, uint64(1)},
				},
			},
			{
				// no order by clause -> all rows are peers
				Query: `SELECT a, dense_rank() over (partition by b) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, uint64(1)},
					{1, uint64(1)},
					{2, uint64(1)},
					{3, uint64(1)},
					{4, uint64(1)},
					{5, uint64(1)},
				},
			},
			{
				Query: `SELECT a, first_value(b) over (partition by c order by b) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, 0},
					{1, 1},
					{2, 0},
					{3, 0},
					{4, 0},
					{5, 0},
				},
			},
			{
				Query: `SELECT a, first_value(a) over (partition by b order by a ASC, c ASC) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, 0},
					{1, 1},
					{2, 2},
					{3, 0},
					{4, 1},
					{5, 5},
				},
			},
			{
				Query: `SELECT a, first_value(a-1) over (partition by b order by a ASC, c ASC) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, -1},
					{1, 0},
					{2, 1},
					{3, -1},
					{4, 0},
					{5, 4},
				},
			},
			{
				Query: `SELECT a, first_value(c) over (partition by b order by a) FROM t1 order by a*b,a`,
				Expected: []sql.Row{
					{0, 0},
					{3, 0},
					{1, 1},
					{2, 0},
					{4, 1},
					{5, 0},
				},
			},
			{
				Query: `SELECT a, lead(a) over (partition by c order by a) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, 2},
					{1, nil},
					{2, 3},
					{3, 4},
					{4, 5},
					{5, nil},
				},
			},
			{
				Query: `SELECT a, lead(a, 1) over (partition by c order by a) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, 2},
					{1, nil},
					{2, 3},
					{3, 4},
					{4, 5},
					{5, nil},
				},
			},
			{
				Query: `SELECT a, lead(a+2) over (partition by c order by a) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, 4},
					{1, nil},
					{2, 5},
					{3, 6},
					{4, 7},
					{5, nil},
				},
			},
			{
				Query: `SELECT a, lead(a, 1, a-1) over (partition by c order by a) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, 2},
					{1, 0},
					{2, 3},
					{3, 4},
					{4, 5},
					{5, 4},
				},
			},
			{
				Query: `SELECT a, lead(a, 0) over (partition by c order by a) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, 0},
					{1, 1},
					{2, 2},
					{3, 3},
					{4, 4},
					{5, 5},
				},
			},
			{
				Query: `SELECT a, lead(a, 1, -1) over (partition by c order by a) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, 2},
					{1, -1},
					{2, 3},
					{3, 4},
					{4, 5},
					{5, -1},
				},
			},
			{
				Query: `SELECT a, lead(a, 3, -1) over (partition by c order by a) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, 4},
					{1, -1},
					{2, 5},
					{3, -1},
					{4, -1},
					{5, -1},
				},
			},
			{
				Query: `SELECT a, lead('s') over (partition by c order by a) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, "s"},
					{1, nil},
					{2, "s"},
					{3, "s"},
					{4, "s"},
					{5, nil},
				},
			},
			{
				Query: `SELECT a, last_value(b) over (partition by c order by b) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, 0},
					{1, 1},
					{2, 2},
					{3, 0},
					{4, 1},
					{5, 3},
				},
			},
			{
				Query: `SELECT a, last_value(a) over (partition by b order by a ASC, c ASC) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, 0},
					{1, 1},
					{2, 2},
					{3, 3},
					{4, 4},
					{5, 5},
				},
			},
			{
				Query: `SELECT a, last_value(a-1) over (partition by b order by a ASC, c ASC) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, -1},
					{1, 0},
					{2, 1},
					{3, 2},
					{4, 3},
					{5, 4},
				},
			},
			{
				Query: `SELECT a, last_value(c) over (partition by b order by c) FROM t1 order by a*b,a`,
				Expected: []sql.Row{
					{0, 0},
					{3, 0},
					{1, 1},
					{2, 0},
					{4, 0},
					{5, 0},
				},
			},
			{
				Query: `SELECT a, lag(a) over (partition by c order by a) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, nil},
					{1, nil},
					{2, 0},
					{3, 2},
					{4, 3},
					{5, 4},
				},
			},
			{
				Query: `SELECT a, lag(a, 1) over (partition by c order by a) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, nil},
					{1, nil},
					{2, 0},
					{3, 2},
					{4, 3},
					{5, 4},
				},
			},
			{
				Query: `SELECT a, lag(a+2) over (partition by c order by a) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, nil},
					{1, nil},
					{2, 2},
					{3, 4},
					{4, 5},
					{5, 6},
				},
			},
			{
				Query: `SELECT a, lag(a, 1, a-1) over (partition by c order by a) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, -1},
					{1, 0},
					{2, 0},
					{3, 2},
					{4, 3},
					{5, 4},
				},
			},
			{
				Query: `SELECT a, lag(a, 0) over (partition by c order by a) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, 0},
					{1, 1},
					{2, 2},
					{3, 3},
					{4, 4},
					{5, 5},
				},
			},
			{
				Query: `SELECT a, lag(a, 1, -1) over (partition by c order by a) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, -1},
					{1, -1},
					{2, 0},
					{3, 2},
					{4, 3},
					{5, 4},
				},
			},
			{
				Query: `SELECT a, lag(a, 3, -1) over (partition by c order by a) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, -1},
					{1, -1},
					{2, -1},
					{3, -1},
					{4, 0},
					{5, 2},
				},
			},
			{
				Query: `SELECT a, lag('s') over (partition by c order by a) FROM t1 order by a`,
				Expected: []sql.Row{
					{0, nil},
					{1, nil},
					{2, "s"},
					{3, "s"},
					{4, "s"},
					{5, "s"},
				},
			},
			{
				Query:       "SELECT a, lag(a, -1) over (partition by c) FROM t1",
				ExpectedErr: expression.ErrInvalidOffset,
			},
			{
				Query:       "SELECT a, lag(a, 's') over (partition by c) FROM t1",
				ExpectedErr: expression.ErrInvalidOffset,
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/11468
		Name: "FIRST_VALUE receives star placeholder",
		SetUpScript: []string{
			"CREATE TABLE t(id INT PRIMARY KEY, v INT NOT NULL)",
			"INSERT INTO t VALUES (1, 10), (2, 20)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "SELECT FIRST_VALUE(*) OVER (ORDER BY id) AS first_row FROM t",
				ExpectedErr: sql.ErrSyntaxError,
			},
		},
	},
	{
		Name: "window functions, bit_and/bit_or/bit_xor",
		SetUpScript: []string{
			"CREATE TABLE t2 (a int, b int, c int)",
			"INSERT INTO t2 VALUES (1,1,1), (3,2,2), (7,4,5)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SELECT bit_and(a), bit_or(b), bit_xor(c) FROM t2`,
				Expected: []sql.Row{{uint64(1), uint64(7), uint64(6)}},
			},
		},
	},
	{
		Name: "window functions, bit_and/bit_or/bit_xor over non-numeric column",
		SetUpScript: []string{
			"CREATE TABLE t3 (x varchar(100))",
			"INSERT INTO t3 VALUES ('these'), ('are'), ('strings')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SELECT bit_and(x) from t3`,
				Expected: []sql.Row{{uint64(0)}},
			},
			{
				Query:    `SELECT bit_or(x) from t3`,
				Expected: []sql.Row{{uint64(0)}},
			},
			{
				Query:    `SELECT bit_xor(x) from t3`,
				Expected: []sql.Row{{uint64(0)}},
			},
		},
	},
	{
		Name: "window functions, bit_and/bit_or/bit_xor over empty table",
		SetUpScript: []string{
			"CREATE TABLE t4 (x int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SELECT bit_and(x) from t4`,
				Expected: []sql.Row{{^uint64(0)}},
			},
			{
				Query:    `SELECT bit_or(x) from t4`,
				Expected: []sql.Row{{uint64(0)}},
			},
			{
				Query:    `SELECT bit_xor(x) from t4`,
				Expected: []sql.Row{{uint64(0)}},
			},
		},
	},
	{
		Name: "window functions, row_number partitioned by multiple columns",
		SetUpScript: []string{
			"CREATE TABLE t5 (a INTEGER, b INTEGER)",
			"INSERT INTO t5 VALUES (0,0), (0,1), (1,0), (1,1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT a, b, row_number() over (partition by a, b) FROM t5 order by a, b`,
				Expected: []sql.Row{
					{0, 0, 1},
					{0, 1, 1},
					{1, 0, 1},
					{1, 1, 1},
				},
			},
		},
	},
	{
		Name: "window functions, row_number partitioned by NULL keys",
		SetUpScript: []string{
			"CREATE TABLE nullable_partition_keys (id INT PRIMARY KEY, g INT NULL)",
			"INSERT INTO nullable_partition_keys VALUES (1, NULL), (2, NULL), (3, 1)",
		},
		Query: "SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) FROM nullable_partition_keys ORDER BY id",
		Expected: []sql.Row{
			{int64(1), int64(1)},
			{int64(2), int64(2)},
			{int64(3), int64(1)},
		},
	},
	{
		Name: "identical expressions over different windows should produce different results",
		SetUpScript: []string{
			"CREATE TABLE t(a INT, b INT);",
			"INSERT INTO t(a, b) VALUES (1, 1), (1, 2), (1, 3), (2, 4), (2, 5), (2, 6);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT SUM(b) OVER (PARTITION BY a ORDER BY b) FROM t ORDER BY 1;",
				Expected: []sql.Row{{float64(1)}, {float64(3)}, {float64(4)}, {float64(6)}, {float64(9)}, {float64(15)}},
			},
			{
				Query:    "SELECT SUM(b) OVER (ORDER BY b) FROM t ORDER BY 1;",
				Expected: []sql.Row{{float64(1)}, {float64(3)}, {float64(6)}, {float64(10)}, {float64(15)}, {float64(21)}},
			},
			{
				Query: "SELECT SUM(b) OVER (PARTITION BY a ORDER BY b), SUM(b) OVER (ORDER BY b) FROM t ORDER BY 1;",
				Expected: []sql.Row{
					{float64(1), float64(1)},
					{float64(3), float64(3)},
					{float64(4), float64(10)},
					{float64(6), float64(6)},
					{float64(9), float64(15)},
					{float64(15), float64(21)},
				},
			},
		},
	},
	{
		Name:    "ntile tests",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int primary key, j int);",
			"insert into t values (1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 2), (7, 2), (8, 2), (9, 2), (10, 2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "select i, ntile(0) over() from t;",
				ExpectedErr: sql.ErrInvalidArgument,
			},
			{
				Query:       "select i, ntile(9223372036854775808) over() from t;",
				ExpectedErr: sql.ErrInvalidArgument,
			},
			{
				Query:       "select i, ntile(18446744073709551615) over() from t;",
				ExpectedErr: sql.ErrInvalidArgument,
			},
			{
				Query: "select i, ntile(100) over() from t;",
				Expected: []sql.Row{
					{1, uint64(1)},
					{2, uint64(2)},
					{3, uint64(3)},
					{4, uint64(4)},
					{5, uint64(5)},
					{6, uint64(6)},
					{7, uint64(7)},
					{8, uint64(8)},
					{9, uint64(9)},
					{10, uint64(10)},
				},
			},
			{
				Query: "select i, ntile(10) over() from t;",
				Expected: []sql.Row{
					{1, uint64(1)},
					{2, uint64(2)},
					{3, uint64(3)},
					{4, uint64(4)},
					{5, uint64(5)},
					{6, uint64(6)},
					{7, uint64(7)},
					{8, uint64(8)},
					{9, uint64(9)},
					{10, uint64(10)},
				},
			},
			{
				Query: "select i, ntile(9) over() from t;",
				Expected: []sql.Row{
					{1, uint64(1)},
					{2, uint64(1)},
					{3, uint64(2)},
					{4, uint64(3)},
					{5, uint64(4)},
					{6, uint64(5)},
					{7, uint64(6)},
					{8, uint64(7)},
					{9, uint64(8)},
					{10, uint64(9)},
				},
			},
			{
				Query: "select i, ntile(8) over() from t;",
				Expected: []sql.Row{
					{1, uint64(1)},
					{2, uint64(1)},
					{3, uint64(2)},
					{4, uint64(2)},
					{5, uint64(3)},
					{6, uint64(4)},
					{7, uint64(5)},
					{8, uint64(6)},
					{9, uint64(7)},
					{10, uint64(8)},
				},
			},
			{
				Query: "select i, ntile(7) over() from t;",
				Expected: []sql.Row{
					{1, uint64(1)},
					{2, uint64(1)},
					{3, uint64(2)},
					{4, uint64(2)},
					{5, uint64(3)},
					{6, uint64(3)},
					{7, uint64(4)},
					{8, uint64(5)},
					{9, uint64(6)},
					{10, uint64(7)},
				},
			},
			{
				Query: "select i, ntile(6) over() from t;",
				Expected: []sql.Row{
					{1, uint64(1)},
					{2, uint64(1)},
					{3, uint64(2)},
					{4, uint64(2)},
					{5, uint64(3)},
					{6, uint64(3)},
					{7, uint64(4)},
					{8, uint64(4)},
					{9, uint64(5)},
					{10, uint64(6)},
				},
			},
			{
				Query: "select i, ntile(5) over() from t;",
				Expected: []sql.Row{
					{1, uint64(1)},
					{2, uint64(1)},
					{3, uint64(2)},
					{4, uint64(2)},
					{5, uint64(3)},
					{6, uint64(3)},
					{7, uint64(4)},
					{8, uint64(4)},
					{9, uint64(5)},
					{10, uint64(5)},
				},
			},
			{
				Query: "select i, ntile(4) over() from t;",
				Expected: []sql.Row{
					{1, uint64(1)},
					{2, uint64(1)},
					{3, uint64(1)},
					{4, uint64(2)},
					{5, uint64(2)},
					{6, uint64(2)},
					{7, uint64(3)},
					{8, uint64(3)},
					{9, uint64(4)},
					{10, uint64(4)},
				},
			},
			{
				Query: "select i, ntile(3) over() from t;",
				Expected: []sql.Row{
					{1, uint64(1)},
					{2, uint64(1)},
					{3, uint64(1)},
					{4, uint64(1)},
					{5, uint64(2)},
					{6, uint64(2)},
					{7, uint64(2)},
					{8, uint64(3)},
					{9, uint64(3)},
					{10, uint64(3)},
				},
			},
			{
				Query: "select i, ntile(2) over() from t;",
				Expected: []sql.Row{
					{1, uint64(1)},
					{2, uint64(1)},
					{3, uint64(1)},
					{4, uint64(1)},
					{5, uint64(1)},
					{6, uint64(2)},
					{7, uint64(2)},
					{8, uint64(2)},
					{9, uint64(2)},
					{10, uint64(2)},
				},
			},
			{
				Query: "select i, ntile(1) over() from t;",
				Expected: []sql.Row{
					{1, uint64(1)},
					{2, uint64(1)},
					{3, uint64(1)},
					{4, uint64(1)},
					{5, uint64(1)},
					{6, uint64(1)},
					{7, uint64(1)},
					{8, uint64(1)},
					{9, uint64(1)},
					{10, uint64(1)},
				},
			},
			{
				Query: "select i, j, ntile(2) over(partition by j) from t;",
				Expected: []sql.Row{
					{1, 1, uint64(1)},
					{2, 1, uint64(1)},
					{3, 1, uint64(1)},
					{4, 1, uint64(2)},
					{5, 1, uint64(2)},
					{6, 2, uint64(1)},
					{7, 2, uint64(1)},
					{8, 2, uint64(1)},
					{9, 2, uint64(2)},
					{10, 2, uint64(2)},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/6899
		Name: "window function tests",
		SetUpScript: []string{
			"CREATE TABLE c (c_id INT PRIMARY KEY, bill TEXT);",
			"CREATE TABLE o (o_id INT PRIMARY KEY, c_id INT, ship TEXT);",
			"INSERT INTO c VALUES (1, 'CA'), (2, 'TX'), (3, 'MA'), (4, 'TX'), (5, NULL), (6, 'FL');",
			"INSERT INTO o VALUES (10, 1, 'CA'), (20, 1, 'CA'), (30, 1, 'CA'), (40, 2, 'CA'), (50, 2, 'TX'), (60, 2, NULL), (70, 4, 'WY'), (80, 4, NULL), (90, 6, 'WA');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select row_number() over () as rn from o where c_id=-999",
				Expected: []sql.Row{},
			},
			{
				// TODO: valid query in Postgres. https://github.com/dolthub/doltgresql/issues/1796
				Dialect:  "mysql",
				Query:    "select row_number() over () as rn from o where c_id=1",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				Query:    "select rank() over() as rnk from o where c_id=-999",
				Expected: []sql.Row{},
			},
			{
				// TODO: valid query in Postgres. https://github.com/dolthub/doltgresql/issues/1796
				Dialect: "mysql",
				Query:   "select o_id, c_id, rank() over(order by o_id) as rnk from o where c_id=1",
				Expected: []sql.Row{
					{10, 1, uint64(1)},
					{20, 1, uint64(2)},
					{30, 1, uint64(3)},
				},
			},
			{
				Query:    "select dense_rank() over() as rnk from o where c_id=-999",
				Expected: []sql.Row{},
			},
			{
				// TODO: valid query in Postgres. But Postgres orders nil at the end. Maybe rewrite query to filter out
				//  ship=null https://github.com/dolthub/doltgresql/issues/1796
				Dialect: "mysql",
				Query:   "select ship, dense_rank() over (order by ship) as drnk from o where c_id in (1, 2) order by ship",
				Expected: []sql.Row{
					{nil, uint64(1)},
					{"CA", uint64(2)},
					{"CA", uint64(2)},
					{"CA", uint64(2)},
					{"CA", uint64(2)},
					{"TX", uint64(3)},
				},
			},
			{
				Query:    "select count(*) from o where c_id=-999",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/11381
		Name: "window aggregate functions with order by col",
		SetUpScript: []string{
			"CREATE TABLE t (id BIGINT, name VARCHAR(255));",
			"INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'a');",
			"CREATE TABLE t2 (id BIGINT PRIMARY KEY, grp VARCHAR(10), val INT);",
			"INSERT INTO t2 VALUES (1,'a',10), (2,'a',20), (3,'b',30), (4,'b',5), (5,'c',15), (6,'c',25);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT id, SUM(id)  OVER (ORDER BY name) FROM t ORDER BY id;",
				Expected: []sql.Row{
					{1, 6.0},
					{2, 6.0},
					{3, 6.0},
				},
			},
			{
				Query: "SELECT id, AVG(id)  OVER (ORDER BY name) FROM t ORDER BY id;",
				Expected: []sql.Row{
					{1, 2.0},
					{2, 2.0},
					{3, 2.0},
				},
			},
			{
				Query: "SELECT id, COUNT(id)  OVER (ORDER BY name) FROM t ORDER BY id;",
				Expected: []sql.Row{
					{1, 3},
					{2, 3},
					{3, 3},
				},
			},
			{
				Query: "SELECT id, MAX(val) OVER (ORDER BY grp) FROM t2 ORDER BY id;",
				Expected: []sql.Row{
					{1, 20},
					{2, 20},
					{3, 30},
					{4, 30},
					{5, 30},
					{6, 30},
				},
			},
			{
				Query: "SELECT id, MIN(val) OVER (ORDER BY grp) FROM t2 ORDER BY id;",
				Expected: []sql.Row{
					{1, 10},
					{2, 10},
					{3, 5},
					{4, 5},
					{5, 5},
					{6, 5},
				},
			},
			{
				// TODO: This test should work in Doltgres but panics.
				//  https://github.com/dolthub/doltgresql/issues/3038
				Dialect: "mysql",
				Query:   "SELECT id, STDDEV_POP(val) OVER (ORDER BY grp) FROM t2 ORDER BY id;",
				Expected: []sql.Row{
					{1, 5.0},
					{2, 5.0},
					{3, 9.60143218483576},
					{4, 9.60143218483576},
					{5, 8.539125638299666},
					{6, 8.539125638299666},
				},
			},
			{
				// TODO: This test should work in Doltgres but panics.
				//  https://github.com/dolthub/doltgresql/issues/3038
				Dialect: "mysql",
				Query:   "SELECT id, STDDEV_SAMP(val) OVER (ORDER BY grp) FROM t2 ORDER BY id;",
				Expected: []sql.Row{
					{1, 7.0710678118654755},
					{2, 7.0710678118654755},
					{3, 11.086778913041726},
					{4, 11.086778913041726},
					{5, 9.354143466934854},
					{6, 9.354143466934854},
				},
			},
			{
				// TODO: This test should work in Doltgres but panics.
				//  https://github.com/dolthub/doltgresql/issues/3038
				Dialect: "mysql",
				Query:   "SELECT id, VAR_POP(val) OVER (ORDER BY grp) FROM t2 ORDER BY id;",
				Expected: []sql.Row{
					{1, 25.0},
					{2, 25.0},
					{3, 92.1875},
					{4, 92.1875},
					{5, 72.91666666666667},
					{6, 72.91666666666667},
				},
			},
			{
				// TODO: This test should work in Doltgres but panics.
				//  https://github.com/dolthub/doltgresql/issues/3038
				Dialect: "mysql",
				Query:   "SELECT id, VAR_SAMP(val) OVER (ORDER BY grp) FROM t2 ORDER BY id;",
				Expected: []sql.Row{
					{1, 50.0},
					{2, 50.0},
					{3, 122.91666666666667},
					{4, 122.91666666666667},
					{5, 87.5},
					{6, 87.5},
				},
			},
			{
				Query: "SELECT id, JSON_ARRAYAGG(val) OVER (ORDER BY grp) FROM t2 ORDER BY id;",
				Expected: []sql.Row{
					{1, types.MustJSON(`[10, 20]`)},
					{2, types.MustJSON(`[10, 20]`)},
					{3, types.MustJSON(`[10, 20, 30, 5]`)},
					{4, types.MustJSON(`[10, 20, 30, 5]`)},
					{5, types.MustJSON(`[10, 20, 30, 5, 15, 25]`)},
					{6, types.MustJSON(`[10, 20, 30, 5, 15, 25]`)},
				},
			},
		},
	},
	{
		Name: "Window aggregations with empty OVER clause",
		SetUpScript: []string{
			"CREATE TABLE t(id INT PRIMARY KEY, v INT);",
			"INSERT INTO t VALUES (1,10),(2,20);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// https://github.com/dolthub/dolt/issues/11428
				Query: "SELECT id, FIRST_VALUE(v) OVER () AS fv FROM t ORDER BY id;",
				Expected: []sql.Row{
					{1, 10},
					{2, 10},
				},
			},
			{
				Query: "SELECT id, LAST_VALUE(v) OVER () AS lv FROM t ORDER BY id;",
				Expected: []sql.Row{
					{1, 20},
					{2, 20},
				},
			},
			{
				Query: "SELECT id, LAG(v) OVER () AS l FROM t ORDER BY id;",
				Expected: []sql.Row{
					{1, nil},
					{2, 10},
				},
			},
			{
				Query: "SELECT id, LEAD(v) OVER () AS l FROM t ORDER BY id;",
				Expected: []sql.Row{
					{1, 20},
					{2, nil},
				},
			},
		},
	},
	{
		Name: "format with window function",
		SetUpScript: []string{
			"CREATE TABLE t(locale TINYINT);",
			"INSERT INTO t VALUES (1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Invalid locale still works
				Query:                 "SELECT FORMAT(1, 0, FIRST_VALUE(locale) OVER ()) AS actual FROM t;",
				ExpectedWarningsCount: 1,
				ExpectedWarning:       1649,
				Expected: []sql.Row{
					{"1"},
				},
			},
		},
	},
}

// WindowRowFramesScriptTests tests window functions using ROWS frame specifications.
var WindowRowFramesScriptTests = []ScriptTest{
	{
		Name: "window row frames",
		SetUpScript: []string{
			"CREATE TABLE a (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER)",
			"INSERT INTO a VALUES (0,0,0), (1,1,0), (2,2,0), (3,0,0), (4,1,0), (5,3,0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SELECT sum(y) over (partition by z order by x rows unbounded preceding) FROM a order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(7)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x rows current row) FROM a order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(1)}, {float64(2)}, {float64(0)}, {float64(1)}, {float64(3)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x rows 2 preceding) FROM a order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x rows between current row and 1 following) FROM a order by x`,
				Expected: []sql.Row{{float64(1)}, {float64(3)}, {float64(2)}, {float64(1)}, {float64(4)}, {float64(3)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x rows between 1 preceding and current row) FROM a order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(2)}, {float64(1)}, {float64(4)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x rows between current row and 2 following) FROM a order by x`,
				Expected: []sql.Row{{float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(4)}, {float64(3)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x rows between current row and current row) FROM a order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(1)}, {float64(2)}, {float64(0)}, {float64(1)}, {float64(3)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x rows between current row and unbounded following) FROM a order by x`,
				Expected: []sql.Row{{float64(7)}, {float64(7)}, {float64(6)}, {float64(4)}, {float64(4)}, {float64(3)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x rows between 1 preceding and 1 following) FROM a order by x`,
				Expected: []sql.Row{{float64(1)}, {float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(4)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x rows between 1 preceding and unbounded following) FROM a order by x`,
				Expected: []sql.Row{{float64(7)}, {float64(7)}, {float64(7)}, {float64(6)}, {float64(4)}, {float64(4)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x rows between unbounded preceding and unbounded following) FROM a order by x`,
				Expected: []sql.Row{{float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x rows between 2 preceding and 1 preceding) FROM a order by x`,
				Expected: []sql.Row{{nil}, {float64(0)}, {float64(1)}, {float64(3)}, {float64(2)}, {float64(1)}},
			},
		},
	},
}

// WindowRangeFramesScriptTests tests window functions using RANGE frame specifications, including
// interval-based range boundaries over DATE columns.
var WindowRangeFramesScriptTests = []ScriptTest{
	{
		Name: "window range frames",
		SetUpScript: []string{
			"CREATE TABLE a (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER)",
			"INSERT INTO a VALUES (0,0,0), (1,1,0), (2,2,0), (3,0,0), (4,1,0), (5,3,0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SELECT sum(y) over (partition by z order by x range unbounded preceding) FROM a order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(7)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x range current row) FROM a order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(1)}, {float64(2)}, {float64(0)}, {float64(1)}, {float64(3)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x range 2 preceding) FROM a order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x range between current row and 1 following) FROM a order by x`,
				Expected: []sql.Row{{float64(1)}, {float64(3)}, {float64(2)}, {float64(1)}, {float64(4)}, {float64(3)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x range between 1 preceding and current row) FROM a order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(2)}, {float64(1)}, {float64(4)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x range between current row and 2 following) FROM a order by x`,
				Expected: []sql.Row{{float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(4)}, {float64(3)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x range between current row and current row) FROM a order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(1)}, {float64(2)}, {float64(0)}, {float64(1)}, {float64(3)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x range between current row and unbounded following) FROM a order by x`,
				Expected: []sql.Row{{float64(7)}, {float64(7)}, {float64(6)}, {float64(4)}, {float64(4)}, {float64(3)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x range between 1 preceding and 1 following) FROM a order by x`,
				Expected: []sql.Row{{float64(1)}, {float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(4)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x range between 1 preceding and unbounded following) FROM a order by x`,
				Expected: []sql.Row{{float64(7)}, {float64(7)}, {float64(7)}, {float64(6)}, {float64(4)}, {float64(4)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x range between unbounded preceding and unbounded following) FROM a order by x`,
				Expected: []sql.Row{{float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by x range between 2 preceding and 1 preceding) FROM a order by x`,
				Expected: []sql.Row{{nil}, {float64(0)}, {float64(1)}, {float64(3)}, {float64(2)}, {float64(1)}},
			},
			// range framing without an order by clause
			{
				Query:    `SELECT sum(y) over (partition by y range between unbounded preceding and unbounded following) FROM a order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(2)}, {float64(2)}, {float64(0)}, {float64(2)}, {float64(3)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by y range between unbounded preceding and current row) FROM a order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(2)}, {float64(2)}, {float64(0)}, {float64(2)}, {float64(3)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by y range between current row and unbounded following) FROM a order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(2)}, {float64(2)}, {float64(0)}, {float64(2)}, {float64(3)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by y range between current row and current row) FROM a order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(2)}, {float64(2)}, {float64(0)}, {float64(2)}, {float64(3)}},
			},
		},
	},
	{
		Name: "window range frames, fixed interval size",
		// These queries use MySQL's bare numeric interval literal ("interval 1 DAY"), which isn't
		// valid syntax in Postgres (Postgres requires a quoted quantity: "interval '1' DAY").
		// The equivalent quoted-quantity form is exercised below in the next ScriptTest.
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE b (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER, date DATE)",
			"INSERT INTO b VALUES (0,0,0,'2022-01-26'), (1,0,0,'2022-01-27'), (2,0,0, '2022-01-28'), (3,1,0,'2022-01-29'), (4,1,0,'2022-01-30'), (5,3,0,'2022-01-31')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SELECT sum(y) over (partition by z order by date range between interval 2 DAY preceding and interval 1 DAY preceding) FROM b order by x`,
				Expected: []sql.Row{{nil}, {float64(0)}, {float64(0)}, {float64(0)}, {float64(1)}, {float64(2)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by date range between interval 1 DAY preceding and interval 1 DAY following) FROM b order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(0)}, {float64(1)}, {float64(2)}, {float64(5)}, {float64(4)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by date range between interval 1 DAY following and interval 2 DAY following) FROM b order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(1)}, {float64(2)}, {float64(4)}, {float64(3)}, {nil}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by date range interval 1 DAY preceding) FROM b order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(0)}, {float64(0)}, {float64(1)}, {float64(2)}, {float64(4)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by date range between interval 1 DAY preceding and current row) FROM b order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(0)}, {float64(0)}, {float64(1)}, {float64(2)}, {float64(4)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by date range between interval 1 DAY preceding and unbounded following) FROM b order by x`,
				Expected: []sql.Row{{float64(5)}, {float64(5)}, {float64(5)}, {float64(5)}, {float64(5)}, {float64(4)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by date range between unbounded preceding and interval 1 DAY following) FROM b order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(0)}, {float64(1)}, {float64(2)}, {float64(5)}, {float64(5)}},
			},
		},
	},
	{
		// These queries compute RANGE frame boundaries as a DATE column offset by an INTERVAL.
		Name: "window range frames, variable interval size",
		SetUpScript: []string{
			"CREATE TABLE c (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER, date DATE)",
			"INSERT INTO c VALUES (0,0,0,'2022-01-26'), (1,0,0,'2022-01-26'), (2,0,0, '2022-01-26'), (3,1,0,'2022-01-27'), (4,1,0,'2022-01-29'), (5,3,0,'2022-01-30'), (6,0,0, '2022-02-03'), (7,1,0,'2022-02-03'), (8,1,0,'2022-02-04'), (9,3,0,'2022-02-04')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SELECT sum(y) over (partition by z order by date range between interval '2' DAY preceding and interval '1' DAY preceding) FROM c order by x`,
				Expected: []sql.Row{{nil}, {nil}, {nil}, {float64(0)}, {float64(1)}, {float64(1)}, {nil}, {nil}, {float64(1)}, {float64(1)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by date range between interval '1' DAY preceding and interval '1' DAY following) FROM c order by x`,
				Expected: []sql.Row{{float64(1)}, {float64(1)}, {float64(1)}, {float64(1)}, {float64(4)}, {float64(4)}, {float64(5)}, {float64(5)}, {float64(5)}, {float64(5)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by date range between interval '1' DAY preceding and current row) FROM c order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(0)}, {float64(0)}, {float64(1)}, {float64(1)}, {float64(4)}, {float64(1)}, {float64(1)}, {float64(5)}, {float64(5)}},
			},
			{
				Query:    `SELECT avg(y) over (partition by z order by date range between interval '1' DAY preceding and unbounded following) FROM c order by x`,
				Expected: []sql.Row{{float64(1)}, {float64(1)}, {float64(1)}, {float64(1)}, {float64(3) / float64(2)}, {float64(3) / float64(2)}, {float64(5) / float64(4)}, {float64(5) / float64(4)}, {float64(5) / float64(4)}, {float64(5) / float64(4)}},
			},
			{
				Query:    `SELECT sum(y) over (partition by z order by date range between unbounded preceding and interval '1' DAY following) FROM c order by x`,
				Expected: []sql.Row{{float64(1)}, {float64(1)}, {float64(1)}, {float64(1)}, {float64(5)}, {float64(5)}, {float64(10)}, {float64(10)}, {float64(10)}, {float64(10)}},
			},
			{
				Query:    `SELECT count(y) over (partition by z order by date range between interval '1' DAY following and interval '2' DAY following) FROM c order by x`,
				Expected: []sql.Row{{1}, {1}, {1}, {1}, {1}, {0}, {2}, {2}, {0}, {0}},
			},
			{
				Query:    `SELECT count(y) over (partition by z order by date range between interval '1' DAY preceding and interval '2' DAY following) FROM c order by x`,
				Expected: []sql.Row{{4}, {4}, {4}, {5}, {2}, {2}, {4}, {4}, {4}, {4}},
			},
			{
				Query:    "SELECT sum(y) over (partition by z order by date range interval 'e' DAY preceding) FROM c order by x",
				Expected: []sql.Row{{float64(0)}, {float64(0)}, {float64(0)}, {float64(1)}, {float64(1)}, {float64(3)}, {float64(1)}, {float64(1)}, {float64(4)}, {float64(4)}},
			},
			{
				Query:       "SELECT sum(y) over (partition by z range between unbounded preceding and interval '1' DAY following) FROM c order by x",
				ExpectedErr: aggregation.ErrRangeInvalidOrderBy,
			},
		},
	},
	{
		// RANGE frame arithmetic (offset applied to the order-by expression) on a SET order-by column can
		// produce a value outside that SET's valid domain. https://github.com/dolthub/dolt/issues/11397
		Name: "window range frames, SET order-by column",
		// The SET syntax isn't supported in Postgres, so this script is restricted to the mysql dialect.
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE d (id INTEGER PRIMARY KEY, g INTEGER, s SET('x','y','z'), v INTEGER NOT NULL)",
			"INSERT INTO d VALUES (1, 0, 'x,y', 10), (2, 0, 'x,y,z', 20)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SELECT sum(v) over (partition by g order by s range between current row and 1 following) FROM d order by id`,
				Expected: []sql.Row{{float64(10)}, {float64(20)}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/11469
		Name:    "window range frames, offset pushes BIT order-by value outside domain",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t(id INT PRIMARY KEY, k BIT(2), v INT);",
			"INSERT INTO t VALUES (1, b'01', 10), (2, b'01', 20), (3, b'11', 30);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT id,
       SUM(v) OVER (
         ORDER BY k
         RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING
       ) AS wf
FROM t
ORDER BY id;`,
				Expected: []sql.Row{
					{1, float64(30)},
					{2, float64(30)},
					{3, float64(30)},
				},
			},
		},
	},
	{
		Name: "windows without ORDER BY should be treated as RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
		SetUpScript: []string{
			"CREATE TABLE t(a INT, b INT);",
			"INSERT INTO t(a, b) VALUES (1, 1), (1, 2), (1, 3), (2, 4), (2, 5), (2, 6);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT SUM(b) OVER (PARTITION BY a) FROM t ORDER BY 1;",
				Expected: []sql.Row{{float64(6)}, {float64(6)}, {float64(6)}, {float64(15)}, {float64(15)}, {float64(15)}},
			},
			{
				Query:    "SELECT SUM(b) OVER () FROM t ORDER BY 1;",
				Expected: []sql.Row{{float64(21)}, {float64(21)}, {float64(21)}, {float64(21)}, {float64(21)}, {float64(21)}},
			},
			{
				Query: "SELECT SUM(b) OVER (PARTITION BY a), SUM(b) OVER () FROM t;",
				Expected: []sql.Row{
					{float64(6), float64(21)},
					{float64(6), float64(21)},
					{float64(6), float64(21)},
					{float64(15), float64(21)},
					{float64(15), float64(21)},
					{float64(15), float64(21)},
				},
			},
		},
	},
}

// NamedWindowsScriptTests tests the WINDOW clause, including window inheritance, merging, and errors.
var NamedWindowsScriptTests = []ScriptTest{
	{
		Name: "named windows",
		SetUpScript: []string{
			"CREATE TABLE a (x INTEGER PRIMARY KEY, y INTEGER, z INTEGER)",
			"INSERT INTO a VALUES (0,0,0), (1,1,0), (2,2,0), (3,0,0), (4,1,0), (5,3,0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SELECT sum(y) over (w1) FROM a WINDOW w1 as (order by z) order by x`,
				Expected: []sql.Row{{float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}},
			},
			{
				// window names should not be case-sensitive
				Query:    `SELECT sum(y) over (w1) FROM a WINDOW W1 as (order by z) order by x`,
				Expected: []sql.Row{{float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}},
			},
			{
				Query:    `SELECT sum(y) over (w1) FROM a WINDOW w1 as (partition by z) order by x`,
				Expected: []sql.Row{{float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}, {float64(7)}},
			},
			{
				// A named window with an ORDER BY but no explicit frame must default to a running (cumulative)
				// frame, same as an equivalent inline OVER (...) clause -- not the full-partition frame. The
				// default-frame inference has to run against the referenced window's merged ORDER BY, not the
				// (possibly empty) ORDER BY on the OVER w reference itself.
				Query:    `SELECT sum(y) over w1 FROM a WINDOW w1 as (partition by z order by x) order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(7)}},
			},
			{
				Query:    `SELECT sum(y) over w FROM a WINDOW w as (partition by z order by x rows unbounded preceding) order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(4)}, {float64(7)}},
			},
			{
				Query:    `SELECT sum(y) over w FROM a WINDOW w as (partition by z order by x rows current row) order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(1)}, {float64(2)}, {float64(0)}, {float64(1)}, {float64(3)}},
			},
			{
				Query:    `SELECT sum(y) over (w) FROM a WINDOW w as (partition by z order by x rows 2 preceding) order by x`,
				Expected: []sql.Row{{float64(0)}, {float64(1)}, {float64(3)}, {float64(3)}, {float64(3)}, {float64(4)}},
			},
			{
				Query:    `SELECT row_number() over (w3) FROM a WINDOW w3 as (w2), w2 as (w1), w1 as (partition by z order by x) order by x`,
				Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}, {int64(5)}, {int64(6)}},
			},
			{
				Query:       "SELECT sum(y) over (w1 partition by x) FROM a WINDOW w1 as (partition by z) order by x",
				ExpectedErr: sql.ErrInvalidWindowInheritance,
			},
			{
				Query:       "SELECT sum(y) over (w1 order by x) FROM a WINDOW w1 as (order by z) order by x",
				ExpectedErr: sql.ErrInvalidWindowInheritance,
			},
			{
				Query:       "SELECT sum(y) over (w1 rows unbounded preceding) FROM a WINDOW w1 as (range unbounded preceding) order by x",
				ExpectedErr: sql.ErrInvalidWindowInheritance,
			},
			{
				Query:       "SELECT sum(y) over (w3) FROM a WINDOW w1 as (w2), w2 as (w3), w3 as (w1) order by x",
				ExpectedErr: sql.ErrCircularWindowInheritance,
			},
			{
				// https://github.com/dolthub/dolt/issues/11426
				Query:       "SELECT sum(y) over w AS s FROM a WINDOW w AS (missing ORDER BY x) ORDER BY x",
				ExpectedErr: sql.ErrUnknownWindowName,
			},
			{
				Query:       "SELECT sum(y) over (w1) FROM a WINDOW w2 as (order by z) order by x",
				ExpectedErr: sql.ErrUnknownWindowName,
			},
			// TODO parser needs to differentiate between window replacement and copying -- window frames can't be copied
			// {
			// 	Query:       "SELECT sum(y) over w FROM a WINDOW (w) as (partition by z order by x rows unbounded preceding) order by x",
			// 	ExpectedErr: sql.ErrInvalidWindowInheritance,
			// },
		},
	},
	{
		// A named window with only a PARTITION BY (no ORDER BY) gets a full-partition default frame
		// baked in when it's built on its own. If a second window inherits from it and adds an ORDER
		// BY -- either through another named window (w2 AS (w1 ORDER BY id)) or an inline override
		// (OVER (w1 ORDER BY id)) -- that stale full-partition frame must not survive the merge; the
		// running sum implied by the newly-added ORDER BY has to win.
		Name: "named windows, inherited partition with added order by",
		SetUpScript: []string{
			"CREATE TABLE b (id INTEGER PRIMARY KEY, grp INTEGER, amt INTEGER)",
			"INSERT INTO b VALUES (1,1,10), (2,1,20), (3,1,30), (4,2,5), (5,2,15)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `SELECT sum(amt) over w2 FROM b WINDOW w1 as (partition by grp), w2 as (w1 order by id) order by id`,
				Expected: []sql.Row{{float64(10)}, {float64(30)}, {float64(60)}, {float64(5)}, {float64(20)}},
			},
			{
				Query:    `SELECT sum(amt) over (w1 order by id) FROM b WINDOW w1 as (partition by grp) order by id`,
				Expected: []sql.Row{{float64(10)}, {float64(30)}, {float64(60)}, {float64(5)}, {float64(20)}},
			},
		},
	},
}
