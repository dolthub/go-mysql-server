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
	"github.com/dolthub/go-mysql-server/sql/analyzer/analyzererrors"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// AggregationScriptTests contains self-contained aggregation script tests.
var AggregationScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/9987
		Name: "GROUP BY nil pointer dereference in Dispose when Next() never called",
		SetUpScript: []string{
			"CREATE TABLE test_table (id INT PRIMARY KEY, value INT, category VARCHAR(50))",
			"INSERT INTO test_table VALUES (1, 100, 'A'), (2, 200, 'B'), (3, 300, 'A')",
		},
		Assertions: []ScriptTestAssertion{
			{
				// LIMIT 0 causes the iterator to close without ever calling Next() on groupByIter
				// This leaves all buffer elements as nil causing panic in Dispose(), or empty depending data struct
				Query:    "SELECT category, SUM(value) FROM test_table GROUP BY category LIMIT 0",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT category, COUNT(*) FROM test_table GROUP BY category LIMIT 0",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT SUM(value) FROM test_table LIMIT 0",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "count nullable columns in a keyless table",
		SetUpScript: []string{
			"CREATE TABLE keyless_count (p BIGINT, q BIGINT, r BIGINT)",
			"INSERT INTO keyless_count VALUES (1, NULL, NULL), (2, 20, NULL), (2, 20, NULL), (3, 30, 300), (NULL, NULL, NULL)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM keyless_count",
				Expected: []sql.Row{{5}},
			},
			{
				Query:    "SELECT COUNT(p) FROM keyless_count",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "SELECT COUNT(q) FROM keyless_count",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "SELECT COUNT(r) FROM keyless_count",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "sqllogictest evidence/slt_lang_aggfunc.test",
		SetUpScript: []string{
			"CREATE TABLE t1( x INTEGER, y VARCHAR(8) )",
			"INSERT INTO t1 VALUES(1,'true')",
			"INSERT INTO t1 VALUES(0,'false')",
			"INSERT INTO t1 VALUES(NULL,'NULL')",
		},
		Query: "SELECT count(DISTINCT x) FROM t1",
		Expected: []sql.Row{
			{2},
		},
	},
	{
		Name: "same alias names for result column name and alias table column name",
		SetUpScript: []string{
			"CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER)",
			"INSERT INTO tab0 VALUES(83,0,38)",
			"INSERT INTO tab0 VALUES(26,0,79)",
			"INSERT INTO tab0 VALUES(43,81,24)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT + 13 AS col0 FROM tab0 GROUP BY tab0.col0",
				Expected: []sql.Row{{13}, {13}, {13}},
			},
			{
				Query:    "SELECT 82 col1 FROM tab0 AS cor0 GROUP BY cor0.col1",
				Expected: []sql.Row{{82}, {82}},
			},
			{
				Query:    "SELECT - cor0.col2 * - col2 AS col1 FROM tab0 AS cor0 GROUP BY col2, cor0.col1",
				Expected: []sql.Row{{1444}, {6241}, {576}},
			},
			{
				Query:    "SELECT ALL + 40 col1 FROM tab0 AS cor0 GROUP BY cor0.col1",
				Expected: []sql.Row{{40}, {40}},
			},
			{
				Query:    "SELECT DISTINCT - cor0.col1 col1 FROM tab0 AS cor0 GROUP BY cor0.col1, cor0.col2",
				Expected: []sql.Row{{-81}, {0}},
			},
			{
				Query:    "SELECT DISTINCT ( cor0.col0 ) - col0 AS col2 FROM tab0 AS cor0 GROUP BY cor0.col2, cor0.col0, cor0.col0",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name:    "Group Concat Queries",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE x (pk int)",
			"INSERT INTO x VALUES (1),(2),(3),(4),(NULL)",

			"create table t (o_id int, attribute longtext, value longtext)",
			"INSERT INTO t VALUES (2, 'color', 'red'), (2, 'fabric', 'silk')",
			"INSERT INTO t VALUES (3, 'color', 'green'), (3, 'shape', 'square')",

			"create table nulls(pk int)",
			"INSERT INTO nulls VALUES (NULL)",

			"CREATE TABLE group_concat_students (id INT PRIMARY KEY, first_name VARCHAR(20), last_name VARCHAR(20))",
			"INSERT INTO group_concat_students VALUES (1, 'Alice', 'Smith'), (2, 'Bob', 'Jones'), (3, 'Alice', 'Brown'), (4, 'Eve', NULL), (5, '', 'White')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT GROUP_CONCAT(first_name, ' ', last_name ORDER BY id SEPARATOR '; ') FROM group_concat_students",
				Expected: []sql.Row{{"Alice Smith; Bob Jones; Alice Brown;  White"}},
			},
			{
				Query:    "SELECT GROUP_CONCAT(DISTINCT first_name, ' ', last_name ORDER BY id SEPARATOR '; ') FROM group_concat_students",
				Expected: []sql.Row{{"Alice Smith; Bob Jones; Alice Brown;  White"}},
			},
			{
				Query: `SELECT FIRST_VALUE(x) OVER () FROM (
					SELECT GROUP_CONCAT(v ORDER BY id SEPARATOR '|') AS x
					FROM (SELECT 1 AS id, '' AS v UNION ALL SELECT 2, 'a' UNION ALL SELECT 3, '') t
				) q`,
				Expected: []sql.Row{{"|a|"}},
			},
			{
				Query:    `SELECT group_concat(pk ORDER BY pk) FROM x;`,
				Expected: []sql.Row{{"1,2,3,4"}},
			},
			{
				Query:    `SELECT group_concat(DISTINCT pk ORDER BY pk) FROM x;`,
				Expected: []sql.Row{{"1,2,3,4"}},
			},
			{
				Query:    `SELECT group_concat(DISTINCT pk ORDER BY pk SEPARATOR '-') FROM x;`,
				Expected: []sql.Row{{"1-2-3-4"}},
			},
			{
				Query:    "SELECT group_concat(`attribute` ORDER BY `attribute`) FROM t group by o_id order by o_id asc",
				Expected: []sql.Row{{"color,fabric"}, {"color,shape"}},
			},
			{
				Query:    "SELECT group_concat(DISTINCT `attribute` ORDER BY value DESC SEPARATOR ';') FROM t group by o_id order by o_id asc",
				Expected: []sql.Row{{"fabric;color"}, {"shape;color"}},
			},
			{
				Query:    "SELECT group_concat(DISTINCT `attribute` ORDER BY `attribute`) FROM t",
				Expected: []sql.Row{{"color,fabric,shape"}},
			},
			{
				Query:    "SELECT group_concat(`attribute` ORDER BY `attribute`) FROM t",
				Expected: []sql.Row{{"color,color,fabric,shape"}},
			},
			{
				Query:    `SELECT group_concat((SELECT 2)) FROM x;`,
				Expected: []sql.Row{{"2,2,2,2,2"}},
			},
			{
				Query:    `SELECT group_concat(DISTINCT (SELECT 2)) FROM x;`,
				Expected: []sql.Row{{"2"}},
			},
			{
				Query:    "SELECT group_concat(DISTINCT `attribute` ORDER BY `attribute` ASC) FROM t",
				Expected: []sql.Row{{"color,fabric,shape"}},
			},
			{
				Query:    "SELECT group_concat(DISTINCT `attribute` ORDER BY `attribute` DESC) FROM t",
				Expected: []sql.Row{{"shape,fabric,color"}},
			},
			{
				Query:    `SELECT group_concat(pk) FROM nulls`,
				Expected: []sql.Row{{nil}},
			},
			{
				Query:       `SELECT group_concat((SELECT * FROM t LIMIT 1)) from t`,
				ExpectedErr: sql.ErrInvalidOperandColumns,
			},
			{
				Query:       `SELECT group_concat((SELECT * FROM x)) from t`,
				ExpectedErr: sql.ErrExpectedSingleRow,
			},
			{
				Query:    "SELECT group_concat(`attribute` order by attribute) FROM t where o_id=2 order by attribute",
				Expected: []sql.Row{{"color,fabric"}},
			},
			{
				Query:    "SELECT group_concat(`attribute` order by attribute desc) FROM t where o_id=2 order by attribute",
				Expected: []sql.Row{{"fabric,color"}},
			},
			{
				Query:    "SELECT group_concat(DISTINCT `attribute` ORDER BY value DESC SEPARATOR ';') FROM t group by o_id order by o_id asc",
				Expected: []sql.Row{{"fabric;color"}, {"shape;color"}},
			},
			{
				Query:    "SELECT group_concat(o_id order by o_id) FROM t WHERE `attribute`='color' order by o_id",
				Expected: []sql.Row{{"2,3"}},
			},
			{
				Query:    "SELECT group_concat(attribute order by attribute separator '') FROM t WHERE o_id=2 ORDER BY attribute",
				Expected: []sql.Row{{"colorfabric"}},
			},
		},
	},
	{
		Name:    "Group Concat with Subquery in ORDER BY",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE test_data (id INT PRIMARY KEY, name VARCHAR(50), age INT, category VARCHAR(10))",
			`INSERT INTO test_data VALUES  
(1, 'Alice', 25, 'A'),
(2, 'Bob', 30, 'B'), 
(3, 'Charlie', 22, 'A'), 
(4, 'Diana', 28, 'C'), 
(5, 'Eve', 35, 'B'), 
(6, 'Frank', 26, 'A')`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT category, group_concat(name ORDER BY (SELECT COUNT(*) FROM test_data t2 WHERE t2.category = test_data.category AND t2.age < test_data.age)) FROM test_data GROUP BY category ORDER BY category",
				Expected: []sql.Row{{"A", "Charlie,Alice,Frank"}, {"B", "Bob,Eve"}, {"C", "Diana"}},
			},
			{
				Query:    "SELECT group_concat(name ORDER BY (SELECT AVG(age) FROM test_data t2 WHERE t2.category = test_data.category), id) FROM test_data;",
				Expected: []sql.Row{{"Alice,Charlie,Frank,Diana,Bob,Eve"}},
			},
			{
				Query:    "SELECT category, group_concat(name ORDER BY (SELECT MAX(age) FROM test_data t2 WHERE t2.id <= test_data.id)) FROM test_data GROUP BY category ORDER BY category",
				Expected: []sql.Row{{"A", "Alice,Charlie,Frank"}, {"B", "Bob,Eve"}, {"C", "Diana"}},
			},
		},
	},
	{
		Name:    "Group Concat with Subquery in ORDER BY - Additional Edge Cases",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2), category_id INT, supplier_id INT)",
			"CREATE TABLE categories (id INT PRIMARY KEY, name VARCHAR(50), priority INT)",
			"CREATE TABLE suppliers (id INT PRIMARY KEY, name VARCHAR(50), rating INT)",
			"INSERT INTO products VALUES (1, 'Laptop', 999.99, 1, 1), (2, 'Mouse', 25.50, 1, 2), (3, 'Keyboard', 75.00, 1, 1)",
			"INSERT INTO products VALUES (4, 'Chair', 150.00, 2, 3), (5, 'Desk', 300.00, 2, 3), (6, 'Monitor', 250.00, 1, 2)",
			"INSERT INTO categories VALUES (1, 'Electronics', 1), (2, 'Furniture', 2)",
			"INSERT INTO suppliers VALUES (1, 'TechCorp', 5), (2, 'GadgetInc', 4), (3, 'OfficeSupply', 3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT category_id, GROUP_CONCAT(name ORDER BY (SELECT rating FROM suppliers WHERE suppliers.id = products.supplier_id) DESC, id ASC) FROM products GROUP BY category_id ORDER BY category_id",
				Expected: []sql.Row{{1, "Laptop,Keyboard,Mouse,Monitor"}, {2, "Chair,Desk"}},
			},
			{
				Query:    "SELECT GROUP_CONCAT(name ORDER BY (SELECT COUNT(*) FROM products p2 WHERE p2.price < products.price), id) FROM products",
				Expected: []sql.Row{{"Mouse,Keyboard,Chair,Monitor,Desk,Laptop"}},
			},
			{
				Query:    "SELECT category_id, GROUP_CONCAT(DISTINCT supplier_id ORDER BY (SELECT rating FROM suppliers WHERE suppliers.id = products.supplier_id)) FROM products GROUP BY category_id",
				Expected: []sql.Row{{1, "2,1"}, {2, "3"}},
			},
			{
				Query:    "SELECT GROUP_CONCAT(name ORDER BY (SELECT priority FROM categories WHERE categories.id = products.category_id), price) FROM products",
				Expected: []sql.Row{{"Mouse,Keyboard,Monitor,Laptop,Chair,Desk"}},
			},
			{
				Query:    "SELECT category_id, GROUP_CONCAT(name ORDER BY (SELECT AVG(price) FROM products p2 WHERE p2.category_id = products.category_id) DESC, name) FROM products GROUP BY category_id ORDER BY category_id",
				Expected: []sql.Row{{1, "Keyboard,Laptop,Monitor,Mouse"}, {2, "Chair,Desk"}},
			},
		},
	},
	{
		Name:    "Group Concat Subquery ORDER BY Error Cases",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(50), value INT)",
			"INSERT INTO test_table VALUES (1, 'A', 10), (2, 'B', 20), (3, 'C', 30)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "SELECT GROUP_CONCAT(name ORDER BY (SELECT name, value FROM test_table t2 WHERE t2.id = test_table.id)) FROM test_table",
				ExpectedErr: sql.ErrInvalidOperandColumns,
			},
			{
				Query:       "SELECT GROUP_CONCAT(name ORDER BY (SELECT value FROM test_table)) FROM test_table",
				ExpectedErr: sql.ErrExpectedSingleRow,
			},
		},
	},
	{
		Name:    "Group Concat Subquery ORDER BY Additional Edge Cases",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE complex_test (id INT PRIMARY KEY, name VARCHAR(50), value INT, category VARCHAR(10), created_at DATE)",
			"INSERT INTO complex_test VALUES (1, 'Alpha', 100, 'X', '2023-01-01')",
			"INSERT INTO complex_test VALUES (2, 'Beta', 50, 'Y', '2023-01-15')",
			"INSERT INTO complex_test VALUES (3, 'Gamma', 75, 'X', '2023-02-01')",
			"INSERT INTO complex_test VALUES (4, 'Delta', 25, 'Z', '2023-02-15')",
			"INSERT INTO complex_test VALUES (5, 'Epsilon', 90, 'Y', '2023-03-01')",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Test with subquery returning NULL values
				Query: "SELECT category, GROUP_CONCAT(name ORDER BY (SELECT CASE WHEN complex_test.value > 80 THEN NULL ELSE complex_test.value END), name) FROM complex_test GROUP BY category ORDER BY category",
				Expected: []sql.Row{
					{"X", "Alpha,Gamma"},
					{"Y", "Epsilon,Beta"},
					{"Z", "Delta"},
				},
			},
			{
				// Test with correlated subquery using multiple tables
				Query:    "SELECT GROUP_CONCAT(name ORDER BY (SELECT COUNT(*) FROM complex_test c2 WHERE c2.category = complex_test.category AND c2.value > complex_test.value), name) FROM complex_test",
				Expected: []sql.Row{{"Alpha,Delta,Epsilon,Beta,Gamma"}},
			},
			{
				// Test with subquery using multiple columns errors
				Query:       "SELECT category, GROUP_CONCAT(name ORDER BY (SELECT value, name FROM complex_test c2 WHERE c2.id <= complex_test.id) DESC) FROM complex_test GROUP BY category ORDER BY category",
				ExpectedErr: sql.ErrInvalidOperandColumns,
			},
			{
				// Test with subquery using aggregate functions with HAVING
				Query: "SELECT category, GROUP_CONCAT(name ORDER BY (SELECT AVG(value) FROM complex_test c2 WHERE c2.id <= complex_test.id HAVING AVG(value) > 50) DESC) FROM complex_test GROUP BY category ORDER BY category",
				Expected: []sql.Row{
					{"X", "Alpha,Gamma"},
					{"Y", "Beta,Epsilon"},
					{"Z", "Delta"},
				},
			},
			{
				// Test with DISTINCT and complex subquery
				Query:    "SELECT GROUP_CONCAT(DISTINCT category ORDER BY (SELECT SUM(value) FROM complex_test c2 WHERE c2.category = complex_test.category) DESC SEPARATOR '|') FROM complex_test",
				Expected: []sql.Row{{"X|Y|Z"}},
			},
			{
				// Test with nested subqueries
				Query:    "SELECT GROUP_CONCAT(name ORDER BY (SELECT SUM(value) FROM complex_test c2 WHERE c2.value != (SELECT MIN(value) FROM complex_test c3 where c3.id = complex_test.id))) FROM complex_test;",
				Expected: []sql.Row{{"Alpha,Epsilon,Gamma,Beta,Delta"}},
			},
		},
	},
	{
		Name:    "Group Concat Subquery ORDER BY Performance and Boundary Cases",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE perf_test (id INT PRIMARY KEY, data VARCHAR(10), weight DECIMAL(5,2))",
			"INSERT INTO perf_test VALUES (1, 'A', 1.5), (2, 'B', 2.5), (3, 'C', 0.5), (4, 'D', 3.5), (5, 'E', 2.0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Test with subquery returning same value for multiple rows (stability)
				Query:    "SELECT GROUP_CONCAT(data ORDER BY (SELECT 42), id) FROM perf_test",
				Expected: []sql.Row{{"A,B,C,D,E"}},
			},
			{
				// Test with subquery using LIMIT
				Query:    "SELECT GROUP_CONCAT(data ORDER BY (SELECT weight FROM perf_test p2 WHERE p2.id = perf_test.id LIMIT 1)) FROM perf_test",
				Expected: []sql.Row{{"C,A,E,B,D"}},
			},
			{
				// Test with very small decimal differences in ORDER BY subquery
				Query:    "SELECT GROUP_CONCAT(data ORDER BY (SELECT weight + 0.001 * perf_test.id FROM perf_test p2 WHERE p2.id = perf_test.id)) FROM perf_test",
				Expected: []sql.Row{{"C,A,E,B,D"}},
			},
		},
	},
	{
		Name: "Run through some complex queries with DISTINCT and aggregates",
		SetUpScript: []string{
			"CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER)",
			"CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER)",
			"INSERT INTO tab1 VALUES(51,14,96)",
			"INSERT INTO tab1 VALUES(85,5,59)",
			"INSERT INTO tab1 VALUES(91,47,68)",
			"INSERT INTO tab2 VALUES(64,77,40)",
			"INSERT INTO tab2 VALUES(75,67,58)",
			"INSERT INTO tab2 VALUES(46,51,23)",
			"CREATE TABLE mytable (pk int, v1 int)",
			"INSERT INTO mytable VALUES(1,1)",
			"INSERT INTO mytable VALUES(1,1)",
			"INSERT INTO mytable VALUES(2,2)",
			"INSERT INTO mytable VALUES(1,2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT - SUM( DISTINCT - - 71 ) AS col2 FROM tab2 cor0",
				Expected: []sql.Row{{float64(-71)}},
			},
			{
				Query:    "SELECT - SUM ( DISTINCT - - 71 ) AS col2 FROM tab2 cor0",
				Expected: []sql.Row{{float64(-71)}},
			},
			{
				Query:    "SELECT + MAX( DISTINCT ( - col0 ) ) FROM tab1 AS cor0",
				Expected: []sql.Row{{-51}},
			},
			{
				Query:    "SELECT SUM( DISTINCT + col1 ) * - 22 - - ( - COUNT( * ) ) col0 FROM tab1 AS cor0",
				Expected: []sql.Row{{float64(-1455)}},
			},
			{
				Query:    "SELECT MIN (DISTINCT col1) from tab1 GROUP BY col0 ORDER BY col0",
				Expected: []sql.Row{{14}, {5}, {47}},
			},
			{
				Query:    "SELECT SUM (DISTINCT col1) from tab1 GROUP BY col0 ORDER BY col0",
				Expected: []sql.Row{{float64(14)}, {float64(5)}, {float64(47)}},
			},
			{
				Query:    "SELECT pk, SUM(DISTINCT v1), MAX(v1) FROM mytable GROUP BY pk ORDER BY pk",
				Expected: []sql.Row{{int64(1), float64(3), int64(2)}, {int64(2), float64(2), int64(2)}},
			},
			{
				Query:    "SELECT pk, MIN(DISTINCT v1), MAX(DISTINCT v1) FROM mytable GROUP BY pk ORDER BY pk",
				Expected: []sql.Row{{int64(1), int64(1), int64(2)}, {int64(2), int64(2), int64(2)}},
			},
			{
				Query:    "SELECT SUM(DISTINCT pk * v1) from mytable",
				Expected: []sql.Row{{float64(7)}},
			},
			{
				Query:    "SELECT SUM(DISTINCT POWER(v1, 2)) FROM mytable",
				Expected: []sql.Row{{float64(5)}},
			},
			{
				Query:    "SELECT + + 97 FROM tab1 GROUP BY tab1.col1",
				Expected: []sql.Row{{97}, {97}, {97}},
			},
			{
				Query:    "SELECT rand(10) FROM tab1 GROUP BY tab1.col1",
				Expected: []sql.Row{{0.5660920659323543}, {0.5660920659323543}, {0.5660920659323543}},
			},
			{
				Query:    "SELECT ALL - cor0.col0 * + cor0.col0 AS col2 FROM tab1 AS cor0 GROUP BY cor0.col0",
				Expected: []sql.Row{{-2601}, {-7225}, {-8281}},
			},
			{
				Query:    "SELECT cor0.col0 * cor0.col0 + cor0.col0 AS col2 FROM tab1 AS cor0 GROUP BY cor0.col0 order by 1",
				Expected: []sql.Row{{2652}, {7310}, {8372}},
			},
			{
				Query:    "SELECT - floor(cor0.col0) * ceil(cor0.col0) AS col2 FROM tab1 AS cor0 GROUP BY cor0.col0",
				Expected: []sql.Row{{-2601}, {-7225}, {-8281}},
			},
			{
				Query:    "SELECT col0 FROM tab1 AS cor0 GROUP BY cor0.col0",
				Expected: []sql.Row{{51}, {85}, {91}},
			},
			{
				Query:    "SELECT - cor0.col0 FROM tab1 AS cor0 GROUP BY cor0.col0",
				Expected: []sql.Row{{-51}, {-85}, {-91}},
			},
			{
				Query:    "SELECT col0 BETWEEN 2 and 4 from tab1 group by col0",
				Expected: []sql.Row{{false}, {false}, {false}},
			},
			{
				Query:       "SELECT col0, col1 FROM tab1 GROUP by col0;",
				ExpectedErr: analyzererrors.ErrValidationGroupBy,
			},
			{
				Query:       "SELECT col0, floor(col1) FROM tab1 GROUP by col0;",
				ExpectedErr: analyzererrors.ErrValidationGroupBy,
			},
			{
				Query:       "SELECT floor(cor0.col1) * ceil(cor0.col0) AS col2 FROM tab1 AS cor0 GROUP BY cor0.col0",
				ExpectedErr: analyzererrors.ErrValidationGroupBy,
			},
		},
	},
	{
		Name: "sum() and avg() on DECIMAL type column returns the DECIMAL type result",
		SetUpScript: []string{
			"create table decimal_table (id int, val decimal(18,16));",
			"insert into decimal_table values (1,-2.5633000000000384);",
			"insert into decimal_table values (2,2.5633000000000370);",
			"insert into decimal_table values (3,0.0000000000000004);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT val FROM decimal_table;",
				Expected: []sql.Row{{"-2.5633000000000384"}, {"2.5633000000000370"}, {"0.0000000000000004"}},
			},
			{
				Query:    "SELECT sum(val) FROM decimal_table;",
				Expected: []sql.Row{{"-0.0000000000000010"}},
			},
			{
				Query:    "SELECT avg(val) FROM decimal_table;",
				Expected: []sql.Row{{"-0.00000000000000033333"}},
			},
		},
	},
	{
		Name: "sum() and avg() on non-DECIMAL type column returns the DOUBLE type result",
		SetUpScript: []string{
			"create table float_table (id int primary key, val1 double, val2 float);",
			"insert into float_table values (1,-2.5633000000000384, 2.3);",
			"insert into float_table values (2,2.5633000000000370, 2.4);",
			"insert into float_table values (3,0.0000000000000004, 5.3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT sum(id), sum(val1), sum(val2) FROM float_table ORDER BY id;",
				Expected: []sql.Row{{float64(6), -9.322676295501879e-16, 10.000000238418579}},
			},
			{
				Query:    "SELECT sum(id), sum(val1), sum(val2) FROM float_table ORDER BY id;",
				Expected: []sql.Row{{float64(6), -9.322676295501879e-16, 10.000000238418579}},
			},
			{
				Query:    "SELECT avg(id), avg(val1), avg(val2) FROM float_table ORDER BY id;",
				Expected: []sql.Row{{float64(2), -3.107558765167293e-16, 3.333333412806193}},
			},
		},
	},
	{
		Name: "having clause without groupby clause, all rows implicitly form a single aggregate group",
		SetUpScript: []string{
			"create table numbers (val int);",
			"insert into numbers values (1), (2), (3);",
			"insert into numbers values (2), (4);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select val from numbers;",
				Expected: []sql.Row{{1}, {2}, {3}, {2}, {4}},
			},
			{
				Query:    "select val as a from numbers having a = val;",
				Expected: []sql.Row{{1}, {2}, {3}, {2}, {4}},
			},
			{
				Query:    "select val as a from numbers group by val having a = val;",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
			{
				Query:    "select val as a from numbers as t1 group by t1.val having a = t1.val;",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
			{
				Query:    "select t1.val as a from numbers as t1 group by 1 having a = t1.val;",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
			{
				Query:    "select t1.val as a from numbers as t1 having a = t1.val;",
				Expected: []sql.Row{{1}, {2}, {3}, {2}, {4}},
			},
			{
				Query:    "select count(*) from numbers having count(*) = 5;",
				Expected: []sql.Row{{5}},
			},
			{
				// MySQL returns `Unknown column 'val' in 'having clause'` error for this query,
				// but GMS builds GroupBy for any aggregate function.
				Skip:  true,
				Query: "select count(*) from numbers having count(*) > val;",
				// ExpectedErrStr:   "found HAVING clause with no GROUP BY", // not the exact error we want
			},
			{
				Query:    "select count(*) from numbers group by val having count(*) < val;",
				Expected: []sql.Row{{1}, {1}},
			},
		},
	},
	{
		Name: "using having and group by clauses in subquery ",
		SetUpScript: []string{
			"CREATE TABLE t (i int, t varchar(2));",
			"insert into t values (1, 'a'), (1, 'a2'), (2, 'b'), (3, 'c'), (3, 'c2'), (4, 'd'), (5, 'e'), (5, 'e2');", // , (6, 'f'), (7, 'g'), (7, 'g2')
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select i from t group by i having count(1) = 1 order by i asc",
				Expected: []sql.Row{{2}, {4}},
			},
			{
				Query:    "select i from t group by i having count(1) != 1 order by i asc",
				Expected: []sql.Row{{1}, {3}, {5}},
			},
			{
				Query:    "select * from t where i in (select i from t group by i having count(1) = 1) order by i, t asc;",
				Expected: []sql.Row{{2, "b"}, {4, "d"}},
			},
			{
				Query:    "select * from t where i in (select i from t group by i having count(1) != 1) order by i, t asc;",
				Expected: []sql.Row{{1, "a"}, {1, "a2"}, {3, "c"}, {3, "c2"}, {5, "e"}, {5, "e2"}},
			},
			{
				Query:    "select * from t where i in (select i from t where i = 2 group by i having count(1) = 1) order by i, t asc;",
				Expected: []sql.Row{{2, "b"}},
			},
			{
				Query:    "select * from t where i in (select i from t where i = 3 group by i having count(1) != 1) order by i, t asc;",
				Expected: []sql.Row{{3, "c"}, {3, "c2"}},
			},
			{
				Query:    "select * from t where i in (select i from t where i > 2 group by i having count(1) != 1) order by i, t asc;",
				Expected: []sql.Row{{3, "c"}, {3, "c2"}, {5, "e"}, {5, "e2"}},
			},
			{
				Query:    "select * from t where i in (select i from t where i > 2 group by i having count(1) != 1 order by i desc) order by i, t asc;",
				Expected: []sql.Row{{3, "c"}, {3, "c2"}, {5, "e"}, {5, "e2"}},
			},
			{
				Query:    "select * from t where i in (select i from t where i > 2 group by i having count(1) != 1) order by i desc, t asc;",
				Expected: []sql.Row{{5, "e"}, {5, "e2"}, {3, "c"}, {3, "c2"}},
			},
		},
	},
	{
		Name: "count distinct decimals",
		SetUpScript: []string{
			"create table t (i int, j int)",
			"insert into t values (1, 11), (11, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select count(distinct i, j) from t;",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: "select count(distinct cast(i as decimal), cast(j as decimal)) from t;",
				Expected: []sql.Row{
					{2},
				},
			},
		},
	},
	{
		Name: "group by having with conflicting aliases test",
		SetUpScript: []string{
			"CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER);",
			"INSERT INTO tab2 VALUES(15,61,87);",
			"INSERT INTO tab2 VALUES(91,59,79);",
			"INSERT INTO tab2 VALUES(92,41,58);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT - col2 AS col0 FROM tab2 GROUP BY col0, col2 HAVING NOT + + col2 <= - col0;`,
				Expected: []sql.Row{
					{-87},
					{-79},
					{-58},
				},
			},
			{
				Query: `SELECT -col2 AS col0 FROM tab2 GROUP BY col0, col2 HAVING NOT col2 <= - col0;`,
				Expected: []sql.Row{
					{-87},
					{-79},
					{-58},
				},
			},
			{
				Query: `SELECT -col2 AS col0 FROM tab2 GROUP BY col0, col2 HAVING col2 > -col0;`,
				Expected: []sql.Row{
					{-87},
					{-79},
					{-58},
				},
			},
			{
				Query: `SELECT 500 * col2 AS col0 FROM tab2 GROUP BY col0, col2 HAVING col2 > -col0;`,
				Expected: []sql.Row{
					{43500},
					{39500},
					{29000},
				},
			},

			{
				Query: `select col2-100 as col0 from tab2 group by col0 having col0 > 0;`,
				Expected: []sql.Row{
					{-13},
					{-21},
					{-42},
				},
			},
			{
				Query:    `select col2-100 as col0 from tab2 group by 1 having col0 > 0;`,
				Expected: []sql.Row{},
			},
			{
				Query: `select col0, count(col0) as c from tab2 group by col0 having c > 0;`,
				Expected: []sql.Row{
					{15, 1},
					{91, 1},
					{92, 1},
				},
			},
			{
				Query: `SELECT col0 as a FROM tab2 GROUP BY a HAVING col0 = a;`,
				Expected: []sql.Row{
					{15},
					{91},
					{92},
				},
			},
			{
				Query: `SELECT col0 as a FROM tab2 GROUP BY col0 HAVING col0 = a;`,
				Expected: []sql.Row{
					{15},
					{91},
					{92},
				},
			},
			{
				Query: `SELECT col0 as a FROM tab2 GROUP BY col0, a HAVING col0 = a;`,
				Expected: []sql.Row{
					{15},
					{91},
					{92},
				},
			},
			{
				Query: `SELECT col0 as a FROM tab2 HAVING col0 = a;`,
				Expected: []sql.Row{
					{15},
					{91},
					{92},
				},
			},
			{
				Query: `select col0, (select col1 having col0 > 0) as asdf from tab2 where col0 < 1000;`,
				Expected: []sql.Row{
					{15, 61},
					{91, 59},
					{92, 41},
				},
			},
			{
				Query: `select col0, sum(col1 * col2) as val from tab2 group by col0 having sum(col1 * col2) > 0;`,
				Expected: []sql.Row{
					{15, 5307.0},
					{91, 4661.0},
					{92, 2378.0},
				},
			},
			{
				Query:       `SELECT col0+1 as a FROM tab2 HAVING col0 = a;`,
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:       `select col2-100 as asdf from tab2 group by 1 having col0 > 0;`,
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:       `SELECT -col2 AS col0 FROM tab2 HAVING col2 > -col0;`,
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:       `insert into tab2(col2) select sin(col2) from tab2 group by 1 having col2 > 1;`,
				ExpectedErr: sql.ErrColumnNotFound,
			},
		},
	},
	{
		Name:    "std, stdev, stddev_pop, variance, var_pop, var_samp tests",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int);",
			"create table tt (i int, j int);",
			"insert into tt values (0, 1), (0, 2), (0, 3);",
			"insert into tt values (1, 123), (1, 456), (1, 789);",
			"create table td (v decimal(10,2));",
			"insert into td values (1.00), (2.00), (3.00);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select std(i), stddev(i), stddev_pop(i), stddev_samp(i) from t;",
				Expected: []sql.Row{
					{nil, nil, nil, nil},
				},
			},
			{
				Query: "select variance(i), var_pop(i), var_samp(i) from t;",
				Expected: []sql.Row{
					{nil, nil, nil},
				},
			},
			{
				Query: "insert into t values (1);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select std(i), stddev(i), stddev_pop(i), stddev_samp(i) from t;",
				Expected: []sql.Row{
					{0.0, 0.0, 0.0, nil},
				},
			},
			{
				Query: "select variance(i), var_pop(i), var_samp(i) from t;",
				Expected: []sql.Row{
					{0.0, 0.0, nil},
				},
			},
			{
				Query: "insert into t values (2);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select std(i), stddev(i), stddev_pop(i), stddev_samp(i) from t;",
				Expected: []sql.Row{
					{0.5, 0.5, 0.5, 0.7071067811865476},
				},
			},
			{
				Query: "select variance(i), var_pop(i), var_samp(i) from t;",
				Expected: []sql.Row{
					{0.25, 0.25, 0.5},
				},
			},
			{
				Query: "insert into t values (3);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select std(i), stddev(i), stddev_pop(i), stddev_samp(i) from t;",
				Expected: []sql.Row{
					{0.816496580927726, 0.816496580927726, 0.816496580927726, 1.0},
				},
			},
			{
				Query: "select variance(i), var_pop(i), var_samp(i) from t;",
				Expected: []sql.Row{
					{0.6666666666666666, 0.6666666666666666, 1.0},
				},
			},
			{
				Query: "insert into t values (null), (null);",
				Expected: []sql.Row{
					{types.NewOkResult(2)},
				},
			},
			{
				Query: "select std(i), stddev(i), stddev_pop(i), stddev_samp(i) from t;",
				Expected: []sql.Row{
					{0.816496580927726, 0.816496580927726, 0.816496580927726, 1.0},
				},
			},
			{
				Query: "select variance(i), var_pop(i), var_samp(i) from t;",
				Expected: []sql.Row{
					{0.6666666666666666, 0.6666666666666666, 1.0},
				},
			},
			{
				Query: "select i, std(j), stddev_samp(j) from tt group by i;",
				Expected: []sql.Row{
					{0, 0.816496580927726, 1.0},
					{1, 271.89336144893275, 333.0},
				},
			},
			{
				Query: "select i, variance(i), var_samp(i) from tt group by i;",
				Expected: []sql.Row{
					{0, 0.0, 0.0},
					{1, 0.0, 0.0},
				},
			},
			{
				Query: "select std(i) over(), std(j) over(), stddev_samp(j) over() from tt order by i;",
				Expected: []sql.Row{
					{0.5, 297.47660972475353, 325.86929895281634},
					{0.5, 297.47660972475353, 325.86929895281634},
					{0.5, 297.47660972475353, 325.86929895281634},
					{0.5, 297.47660972475353, 325.86929895281634},
					{0.5, 297.47660972475353, 325.86929895281634},
					{0.5, 297.47660972475353, 325.86929895281634},
				},
			},
			{
				Query: "select i, std(j) over(partition by i), stddev_samp(j) over(partition by i) from tt order by i;",
				Expected: []sql.Row{
					{0, 0.816496580927726, 1.0},
					{0, 0.816496580927726, 1.0},
					{0, 0.816496580927726, 1.0},
					{1, 271.89336144893275, 333.0},
					{1, 271.89336144893275, 333.0},
					{1, 271.89336144893275, 333.0},
				},
			},
			{
				Query: "select i, variance(i) over(), var_samp(i) over() from tt order by i;",
				Expected: []sql.Row{
					{0, 0.25, 0.3},
					{0, 0.25, 0.3},
					{0, 0.25, 0.3},
					{1, 0.25, 0.3},
					{1, 0.25, 0.3},
					{1, 0.25, 0.3},
				},
			},
			{
				Query: "select i, variance(j) over(partition by i), var_samp(i) over(partition by i) from tt order by i;",
				Expected: []sql.Row{
					{0, 0.6666666666666666, 0.0},
					{0, 0.6666666666666666, 0.0},
					{0, 0.6666666666666666, 0.0},
					{1, 73926.0, 0.0},
					{1, 73926.0, 0.0},
					{1, 73926.0, 0.0},
				},
			},
			{
				Query: "insert into tt values (null, null);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select i, std(i) over(), std(j) over(), stddev_samp(j) over() from tt order by i;",
				Expected: []sql.Row{
					{nil, 0.5, 297.47660972475353, 325.86929895281634},
					{0, 0.5, 297.47660972475353, 325.86929895281634},
					{0, 0.5, 297.47660972475353, 325.86929895281634},
					{0, 0.5, 297.47660972475353, 325.86929895281634},
					{1, 0.5, 297.47660972475353, 325.86929895281634},
					{1, 0.5, 297.47660972475353, 325.86929895281634},
					{1, 0.5, 297.47660972475353, 325.86929895281634},
				},
			},
			{
				Query: "select i, std(j) over(partition by i), stddev_samp(j) over(partition by i) from tt order by i;",
				Expected: []sql.Row{
					{nil, nil, nil},
					{0, 0.816496580927726, 1.0},
					{0, 0.816496580927726, 1.0},
					{0, 0.816496580927726, 1.0},
					{1, 271.89336144893275, 333.0},
					{1, 271.89336144893275, 333.0},
					{1, 271.89336144893275, 333.0},
				},
			},
			{
				Query: "select i, variance(i) over(), var_samp(i) over() from tt order by i;",
				Expected: []sql.Row{
					{nil, 0.25, 0.3},
					{0, 0.25, 0.3},
					{0, 0.25, 0.3},
					{0, 0.25, 0.3},
					{1, 0.25, 0.3},
					{1, 0.25, 0.3},
					{1, 0.25, 0.3},
				},
			},
			{
				Query: "select i, variance(j) over(partition by i), var_samp(i) over(partition by i) from tt order by i;",
				Expected: []sql.Row{
					{nil, nil, nil},
					{0, 0.6666666666666666, 0.0},
					{0, 0.6666666666666666, 0.0},
					{0, 0.6666666666666666, 0.0},
					{1, 73926.0, 0.0},
					{1, 73926.0, 0.0},
					{1, 73926.0, 0.0},
				},
			},
			{
				Query: "select i, stddev_pop(j) over w, stddev_samp(j) over w, variance(j) over w, var_samp(i) over w from tt window w as (partition by i) order by i;",
				Expected: []sql.Row{
					{nil, nil, nil, nil, nil},
					{0, 0.816496580927726, 1.0, 0.6666666666666666, 0.0},
					{0, 0.816496580927726, 1.0, 0.6666666666666666, 0.0},
					{0, 0.816496580927726, 1.0, 0.6666666666666666, 0.0},
					{1, 271.89336144893275, 333.0, 73926.0, 0.0},
					{1, 271.89336144893275, 333.0, 73926.0, 0.0},
					{1, 271.89336144893275, 333.0, 73926.0, 0.0},
				},
			},
			{
				Query: "select std(v), stddev(v), stddev_pop(v), stddev_samp(v), variance(v), var_pop(v), var_samp(v) from td;",
				Expected: []sql.Row{
					{0.816496580927726, 0.816496580927726, 0.816496580927726, 1.0, 0.6666666666666666, 0.6666666666666666, 1.0},
				},
			},
		},
	},
	{
		// TODO: every aggregation function needs to use types.TypeAwareConversion
		// Tracking issue: https://github.com/dolthub/dolt/issues/10278
		Skip:    true,
		Name:    "aggregations with date types",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int primary key, d date, dt datetime, dt6 datetime(6), ts timestamp, ts6 timestamp(6));",
			"insert into t values (1, '2001-02-03', '2001-02-03 12:34:56', '2001-02-03 12:34:56.123456', '2001-02-03 12:34:56', '2001-02-03 12:34:56.123456');",
			"insert into t values (2, '2010-03-30', '2010-02-03 22:22:22', '2010-02-03 11:11:11.111111', '2010-03-30 22:22:22', '2010-03-30 11:11:11.111111');",
			"insert into t values (3, '2100-02-03', '2100-02-03 23:23:23', '2100-02-03 23:23:23.654321', '2001-02-03 23:23:23', '2001-02-03 23:23:23.654321');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select sum(d), sum(dt), sum(dt6), sum(ts), sum(ts6) from t;",
				Expected: []sql.Row{
					{float64(61110736), float64(61110609578001), float64(61110609466890.888888), float64(60120736578001), float64(60120736466890.888888)},
				},
			},
			{
				Query: "select var_pop(d), var_pop(dt), var_pop(dt6), var_pop(ts), var_pop(ts6) from t;",
				Expected: []sql.Row{
					{float64(199777143584.22263), float64(1.998000279462624e23), float64(1.998000479464689e23), float64(1.8050853600269382e21), float64(1.8050809093046277e21)},
				},
			},
		},
	},
	{
		Name:    "aggregate function with match",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, doc TEXT, FULLTEXT idx (doc));",
			"INSERT INTO test VALUES (2, 'g hhhh aaaab ooooo aaaa'), (1, 'bbbb ff cccc ddd eee'), (4, 'AAAA aaaa aaaac aaaa Aaaa aaaa'), (3, 'aaaA ff j kkkk llllllll');",
		},
		Assertions: []ScriptTestAssertion{
			{
				// https://github.com/dolthub/dolt/issues/9761
				Skip:        true,
				Query:       "SELECT pk, count(pk), MATCH(doc) AGAINST('aaaa') AS relevancy FROM test ORDER BY relevancy DESC;",
				ExpectedErr: sql.ErrNonAggregatedColumnWithoutGroupBy,
			},
			{
				Query:    "SET SESSION sql_mode = REPLACE(@@SESSION.sql_mode, 'ONLY_FULL_GROUP_BY', '');",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},

			{
				Query:    "SELECT pk, count(pk), MATCH(doc) AGAINST('aaaa') AS relevancy FROM test ORDER BY relevancy DESC;",
				Expected: []sql.Row{{1, 4, float64(0)}},
			},
		},
	},
	{
		Name:    "nonaggregated column in aggregated query",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table emptytable(i mediumint primary key, s varchar(20))",
			"create table mytable(i bigint primary key, s varchar(20))",
			"insert into mytable values (1, 'first'), (2, 'second'), (3, 'third');",
			"create table two_pk (pk1 tinyint, pk2 tinyint, c1 tinyint NOT NULL, primary key (pk1, pk2))",
			"insert into two_pk values (0,0,0), (0,1,10), (1,0,20), (1,1,30)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "SELECT count(*), i, concat(i, i), 123, 'abc', concat('abc', 'def') FROM emptytable;",
				ExpectedErr: sql.ErrNonAggregatedColumnWithoutGroupBy,
			},
			{
				Query:       "SELECT count(*), i, concat(i, i), 123, 'abc', concat('abc', 'def') FROM mytable where false;",
				ExpectedErr: sql.ErrNonAggregatedColumnWithoutGroupBy,
			},
			{
				Query:       "SELECT pk1, SUM(c1) FROM two_pk",
				ExpectedErr: sql.ErrNonAggregatedColumnWithoutGroupBy,
			},
			{
				Query:    "SET SESSION sql_mode = REPLACE(@@SESSION.sql_mode, 'ONLY_FULL_GROUP_BY', '');",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "SELECT count(*), i, concat(i, i), 123, 'abc', concat('abc', 'def') FROM emptytable;",
				Expected: []sql.Row{
					{0, nil, nil, 123, "abc", "abcdef"},
				},
			},
			{
				Query: "SELECT count(*), i, concat(i, i), 123, 'abc', concat('abc', 'def') FROM mytable where false;",
				Expected: []sql.Row{
					{0, nil, nil, 123, "abc", "abcdef"},
				},
			},
			{
				Query:    "SELECT pk1, SUM(c1) FROM two_pk",
				Expected: []sql.Row{{0, 60.0}},
			},
		},
	},
}
