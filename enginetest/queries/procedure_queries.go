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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var ProcedureLogicTests = []ScriptTest{
	{
		// When a loop is executed once before the first evaluation of the loop condition, we expect the stored
		// procedure to return the last result set from that first loop execution.
		Name: "REPEAT with OnceBefore returns first loop evaluation result set",
		SetUpScript: []string{
			`CREATE PROCEDURE p1()
	BEGIN
	SET @counter = 0;
	REPEAT
		SELECT 42 from dual;
		SET @counter = @counter + 1;
	UNTIL @counter >= 0
	END REPEAT;
	END`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL p1;",
				Expected: []sql.UntypedSqlRow{{42}},
			},
		},
	},
	{
		// When a loop condition evals to false, we expect the stored procedure to return the last
		// result set from the previous loop execution.
		Name: "WHILE returns previous loop evaluation result set",
		SetUpScript: []string{
			`CREATE PROCEDURE p1()
	BEGIN
	SET @counter = 0;
	WHILE @counter <= 0 DO
		SET @counter = @counter + 1;
		SELECT CAST(@counter + 41 as SIGNED) from dual;
	END WHILE;
	END`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL p1;",
				Expected: []sql.UntypedSqlRow{{42}},
			},
		},
	},

	{
		Name: "Simple SELECT",
		SetUpScript: []string{
			"CREATE PROCEDURE testabc(x DOUBLE, y DOUBLE) SELECT x*y",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL testabc(2, 3)",
				Expected: []sql.UntypedSqlRow{
					{
						6.0,
					},
				},
			},
			{
				Query: "CALL testabc(9, 9.5)",
				Expected: []sql.UntypedSqlRow{
					{
						85.5,
					},
				},
			},
		},
	},
	{
		Name: "Multiple SELECTs",
		SetUpScript: []string{
			"CREATE TABLE t1(pk VARCHAR(20) PRIMARY KEY)",
			"INSERT INTO t1 VALUES (3), (4), (50)",
			`CREATE PROCEDURE p1()
BEGIN
	SELECT * FROM t1;
	UPDATE t1 SET pk = CONCAT(pk, '0');
	SELECT * FROM t1;
	INSERT INTO t1 VALUES (1), (2);
	SELECT * FROM t1;
	REPLACE INTO t1 VALUES (1), (30);
	DELETE FROM t1 WHERE pk LIKE '%00';
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1()",
				Expected: []sql.UntypedSqlRow{
					{"1"},
					{"2"},
					{"30"},
					{"40"},
					{"500"},
				},
			},
			{
				Query: "SELECT * FROM t1 ORDER BY 1",
				Expected: []sql.UntypedSqlRow{
					{"1"},
					{"2"},
					{"30"},
					{"40"},
				},
			},
		},
	},
	{
		Name: "IF/ELSE with 1 SELECT at end",
		SetUpScript: []string{
			"SET @outparam = ''",
			`CREATE PROCEDURE p1(OUT s VARCHAR(200), N DOUBLE, m DOUBLE)
BEGIN
	SET s = '';
	IF n = m THEN SET s = 'equals';
	ELSE
		IF n > m THEN SET s = 'greater';
		ELSE SET s = 'less';
		END IF;
		SET s = CONCAT('is ', s, ' than');
	END IF;
	SET s = CONCAT(n, ' ', s, ' ', m, '.');
	SELECT s;
END;`,
			`CREATE PROCEDURE p2(s VARCHAR(200), N DOUBLE, m DOUBLE)
BEGIN
	SET s = '';
	IF n = m THEN SET s = 'equals';
	ELSE
		IF n > m THEN SET s = 'greater';
		ELSE SET s = 'less';
		END IF;
		SET s = CONCAT('is ', s, ' than');
	END IF;
	SET s = CONCAT(n, ' ', s, ' ', m, '.');
	SELECT s;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1(@outparam, 1, 2)",
				Expected: []sql.UntypedSqlRow{
					{
						"1 is less than 2.",
					},
				},
			},
			{
				Query: "SELECT @outparam",
				Expected: []sql.UntypedSqlRow{
					{
						"1 is less than 2.",
					},
				},
			},
			{
				Query: "CALL p1(@outparam, null, 2)",
				Expected: []sql.UntypedSqlRow{
					{
						nil,
					},
				},
			},
			{
				Query: "CALL p1(@outparam, 7, 4)",
				Expected: []sql.UntypedSqlRow{
					{
						"7 is greater than 4.",
					},
				},
			},
			{
				Query: "SELECT @outparam",
				Expected: []sql.UntypedSqlRow{
					{
						"7 is greater than 4.",
					},
				},
			},
			{
				Query: "CALL p1(@outparam, 5, 5)",
				Expected: []sql.UntypedSqlRow{
					{
						"5 equals 5.",
					},
				},
			},
			{
				Query: "SELECT @outparam",
				Expected: []sql.UntypedSqlRow{
					{
						"5 equals 5.",
					},
				},
			},
			{
				Query: "CALL p2(@outparam, 9, 3)",
				Expected: []sql.UntypedSqlRow{
					{
						"9 is greater than 3.",
					},
				},
			},
			{ // Not affected as p2 has an IN param rather than OUT
				Query: "SELECT @outparam",
				Expected: []sql.UntypedSqlRow{
					{
						"5 equals 5.",
					},
				},
			},
		},
	},
	{
		Name: "IF/ELSE with nested SELECT in branches",
		SetUpScript: []string{
			"CREATE TABLE t1(pk BIGINT PRIMARY KEY)",
			`CREATE PROCEDURE p1(x BIGINT)
BEGIN
	DELETE FROM t1;
	IF x < 10 THEN
		IF x = 0 THEN
			SELECT 1000;
		ELSEIF x = 1 THEN
			SELECT 1001;
		ELSE
			INSERT INTO t1 VALUES (3), (4), (5);
		END IF;
	ELSEIF x < 20 THEN
		IF x = 10 THEN
			INSERT INTO t1 VALUES (1), (2), (6), (7);
		ELSEIF x = 11 THEN
			INSERT INTO t1 VALUES (8), (9), (10), (11), (12);
			SELECT * FROM t1;
		ELSE
			SELECT 2002;
			SELECT 2003;
		END IF;
	END IF;
	INSERT INTO t1 VALUES (1), (2);
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1(0)",
				Expected: []sql.UntypedSqlRow{
					{
						int64(1000),
					},
				},
			},
			{
				Query: "CALL p1(1)",
				Expected: []sql.UntypedSqlRow{
					{
						int64(1001),
					},
				},
			},
			{
				SkipResultCheckOnServerEngine: true, // tracking issue: https://github.com/dolthub/dolt/issues/6918
				Query:                         "CALL p1(2)",
				Expected: []sql.UntypedSqlRow{
					{
						types.NewOkResult(2),
					},
				},
			},
			{
				Query:       "CALL p1(10)",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{
				Query: "CALL p1(11)",
				Expected: []sql.UntypedSqlRow{
					{int64(8)},
					{int64(9)},
					{int64(10)},
					{int64(11)},
					{int64(12)},
				},
			},
			{
				Query: "CALL p1(12)",
				Expected: []sql.UntypedSqlRow{
					{
						int64(2003),
					},
				},
			},
		},
	},
	{
		Name: "REPEAT loop over user variable",
		SetUpScript: []string{
			`CREATE PROCEDURE p1(p1 INT)
BEGIN
	SET @x = 0;
	REPEAT SET @x = @x + 1; UNTIL @x > p1 END REPEAT;
END`,
		},
		Assertions: []ScriptTestAssertion{
			// TODO: MySQL won't actually return *any* result set for these stored procedures. We have done work
			//       to filter out all but the last result set generated by the stored procedure, but we still
			//       need to filter out Result Sets that should be completely omitted.
			{
				Query:    "CALL p1(0)",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "CALL p1(1)",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "CALL p1(2)",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				// https://github.com/dolthub/dolt/issues/6230
				Query:    "CALL p1(200)",
				Expected: []sql.UntypedSqlRow{{}},
			},
		},
	},
	{
		Name: "WHILE loop over user variable",
		SetUpScript: []string{
			`CREATE PROCEDURE p1(p1 INT)
BEGIN
	SET @x = 0;
	WHILE @x <= p1 DO
		SET @x = @x + 1;
	END WHILE;
END`,
		},
		Assertions: []ScriptTestAssertion{
			// TODO: MySQL won't actually return *any* result set for these stored procedures. We have done work
			//       to filter out all but the last result set generated by the stored procedure, but we still
			//       need to filter out Result Sets that should be completely omitted.
			{
				Query:    "CALL p1(0)",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "CALL p1(1)",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "CALL p1(2)",
				Expected: []sql.UntypedSqlRow{{}},
			},
		},
	},
	{
		Name: "CASE statements",
		SetUpScript: []string{
			`CREATE PROCEDURE p1(IN a BIGINT)
BEGIN
	DECLARE b VARCHAR(200) DEFAULT "";
	tloop: LOOP
		CASE
			WHEN a < 4 THEN
				SET b = CONCAT(b, "a");
				SET a = a + 1;
			WHEN a < 8 THEN
				SET b = CONCAT(b, "b");
				SET a = a + 1;
			ELSE
				LEAVE tloop;
		END CASE;
	END LOOP;
	SELECT b;
END;`,
			`CREATE PROCEDURE p2(IN a BIGINT)
BEGIN
	DECLARE b VARCHAR(200) DEFAULT "";
	tloop: LOOP
		CASE a
			WHEN 1 THEN
				SET b = CONCAT(b, "a");
				SET a = a + 1;
			WHEN 2 THEN
				SET b = CONCAT(b, "b");
				SET a = a + 1;
			WHEN 3 THEN
				SET b = CONCAT(b, "c");
				SET a = a + 1;
			ELSE
				LEAVE tloop;
		END CASE;
	END LOOP;
	SELECT b;
END;`,
			`CREATE PROCEDURE p3(IN a BIGINT)
BEGIN
	DECLARE b VARCHAR(200) DEFAULT "";
	tloop: LOOP
		CASE a
			WHEN 1 THEN
				SET b = CONCAT(b, "a");
				SET a = a + 1;
		END CASE;
	END LOOP;
	SELECT b;
END;`,
			`CREATE PROCEDURE p4(IN a BIGINT)
BEGIN
	DECLARE b VARCHAR(200) DEFAULT "";
	tloop: LOOP
		CASE
			WHEN a = 1 THEN
				SET b = CONCAT(b, "a");
				SET a = a + 1;
		END CASE;
	END LOOP;
	SELECT b;
END;`,
			`CREATE PROCEDURE p5(IN a BIGINT)
BEGIN
	DECLARE b VARCHAR(200) DEFAULT "";
	REPEAT
		CASE
			WHEN a <= 1 THEN
				SET b = CONCAT(b, "a");
				SET a = a + 1;
		END CASE;
	UNTIL a > 1
	END REPEAT;
	SELECT b;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1(0)",
				Expected: []sql.UntypedSqlRow{
					{"aaaabbbb"},
				},
			},
			{
				Query: "CALL p1(3)",
				Expected: []sql.UntypedSqlRow{
					{"abbbb"},
				},
			},
			{
				Query: "CALL p1(6)",
				Expected: []sql.UntypedSqlRow{
					{"bb"},
				},
			},
			{
				Query: "CALL p1(9)",
				Expected: []sql.UntypedSqlRow{
					{""},
				},
			},
			{
				Query: "CALL p2(1)",
				Expected: []sql.UntypedSqlRow{
					{"abc"},
				},
			},
			{
				Query: "CALL p2(2)",
				Expected: []sql.UntypedSqlRow{
					{"bc"},
				},
			},
			{
				Query: "CALL p2(3)",
				Expected: []sql.UntypedSqlRow{
					{"c"},
				},
			},
			{
				Query: "CALL p2(4)",
				Expected: []sql.UntypedSqlRow{
					{""},
				},
			},
			{
				Query:          "CALL p3(1)",
				ExpectedErrStr: "Case not found for CASE statement (errno 1339) (sqlstate 20000)",
			},
			{
				Query:          "CALL p3(2)",
				ExpectedErrStr: "Case not found for CASE statement (errno 1339) (sqlstate 20000)",
			},
			{
				Query:          "CALL p4(1)",
				ExpectedErrStr: "Case not found for CASE statement (errno 1339) (sqlstate 20000)",
			},
			{
				Query:          "CALL p4(-1)",
				ExpectedErrStr: "Case not found for CASE statement (errno 1339) (sqlstate 20000)",
			},
			{
				Query: "CALL p5(0)",
				Expected: []sql.UntypedSqlRow{
					{"aa"},
				},
			},
			{
				Query: "CALL p5(1)",
				Expected: []sql.UntypedSqlRow{
					{"a"},
				},
			},
		},
	},
	{
		Name: "SELECT with JOIN and table aliases",
		SetUpScript: []string{
			"CREATE TABLE foo(a BIGINT PRIMARY KEY, b VARCHAR(20))",
			"INSERT INTO foo VALUES (1, 'd'), (2, 'e'), (3, 'f')",
			"CREATE TABLE bar(b VARCHAR(30) PRIMARY KEY, c BIGINT)",
			"INSERT INTO bar VALUES ('x', 3), ('y', 2), ('z', 1)",
			// Direct child is SELECT
			"CREATE PROCEDURE p1() SELECT f.a, bar.b, f.b FROM foo f INNER JOIN bar ON f.a = bar.c ORDER BY 1",
			// Direct child is BEGIN/END
			"CREATE PROCEDURE p2() BEGIN SELECT f.a, bar.b, f.b FROM foo f INNER JOIN bar ON f.a = bar.c ORDER BY 1; END;",
			// Direct child is IF
			"CREATE PROCEDURE p3() IF 0 = 0 THEN SELECT f.a, bar.b, f.b FROM foo f INNER JOIN bar ON f.a = bar.c ORDER BY 1; END IF;",
			// Direct child is BEGIN/END with preceding SELECT
			"CREATE PROCEDURE p4() BEGIN SELECT 7; SELECT f.a, bar.b, f.b FROM foo f INNER JOIN bar ON f.a = bar.c ORDER BY 1; END;",
			// Direct child is IF with preceding SELECT
			"CREATE PROCEDURE p5() IF 0 = 0 THEN SELECT 7; SELECT f.a, bar.b, f.b FROM foo f INNER JOIN bar ON f.a = bar.c ORDER BY 1; END IF;",
		},
		Assertions: []ScriptTestAssertion{
			{ // Enforces that this is the expected output from the query normally
				Query: "SELECT f.a, bar.b, f.b FROM foo f INNER JOIN bar ON f.a = bar.c ORDER BY 1",
				Expected: []sql.UntypedSqlRow{
					{int64(1), "z", "d"},
					{int64(2), "y", "e"},
					{int64(3), "x", "f"},
				},
			},
			{
				Query: "CALL p1()",
				Expected: []sql.UntypedSqlRow{
					{int64(1), "z", "d"},
					{int64(2), "y", "e"},
					{int64(3), "x", "f"},
				},
			},
			{
				Query: "CALL p2()",
				Expected: []sql.UntypedSqlRow{
					{int64(1), "z", "d"},
					{int64(2), "y", "e"},
					{int64(3), "x", "f"},
				},
			},
			{
				SkipResultCheckOnServerEngine: true, // tracking issue: https://github.com/dolthub/dolt/issues/6918
				Query:                         "CALL p3()",
				Expected: []sql.UntypedSqlRow{
					{int64(1), "z", "d"},
					{int64(2), "y", "e"},
					{int64(3), "x", "f"},
				},
			},
			{
				Query: "CALL p4()",
				Expected: []sql.UntypedSqlRow{
					{int64(1), "z", "d"},
					{int64(2), "y", "e"},
					{int64(3), "x", "f"},
				},
			},
			{
				SkipResultCheckOnServerEngine: true, // tracking issue: https://github.com/dolthub/dolt/issues/6918
				Query:                         "CALL p5()",
				Expected: []sql.UntypedSqlRow{
					{int64(1), "z", "d"},
					{int64(2), "y", "e"},
					{int64(3), "x", "f"},
				},
			},
		},
	},
	{
		Name: "Nested CALL in IF/ELSE branch",
		SetUpScript: []string{
			"CREATE TABLE t1(pk BIGINT PRIMARY KEY)",
			"INSERT INTO t1 VALUES (2), (3)",
			"CREATE PROCEDURE p1(INOUT x BIGINT) BEGIN IF X = 1 THEN CALL p2(10); ELSEIF x = 2 THEN CALL p2(100); ELSE CALL p2(X); END IF; END;",
			"CREATE PROCEDURE p2(INOUT y BIGINT) BEGIN SELECT pk * y FROM t1 ORDER BY 1; END;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1(1)",
				Expected: []sql.UntypedSqlRow{
					{int64(20)},
					{int64(30)},
				},
			},
			{
				Query: "CALL p1(2)",
				Expected: []sql.UntypedSqlRow{
					{int64(200)},
					{int64(300)},
				},
			},
			{
				Query: "CALL p1(5)",
				Expected: []sql.UntypedSqlRow{
					{int64(10)},
					{int64(15)},
				},
			},
		},
	},
	{
		Name: "INSERT INTO SELECT doesn't override SELECT",
		SetUpScript: []string{
			"CREATE TABLE t1(pk BIGINT PRIMARY KEY)",
			"CREATE TABLE t2(pk BIGINT PRIMARY KEY)",
			"INSERT INTO t1 VALUES (2), (3)",
			"INSERT INTO t2 VALUES (1)",
			`CREATE PROCEDURE p1(x BIGINT)
BEGIN
	DELETE FROM t2 WHERE pk > 1;
	INSERT INTO t2 SELECT pk FROM t1;
	SELECT * FROM t2;
	INSERT INTO t2 SELECT pk + 10 FROM t1;
	IF x = 1 THEN
		SELECT * FROM t2;
	END IF;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1(0)",
				Expected: []sql.UntypedSqlRow{
					{int64(1)},
					{int64(2)},
					{int64(3)},
				},
			},
			{
				Query: "CALL p1(1)",
				Expected: []sql.UntypedSqlRow{
					{int64(1)},
					{int64(2)},
					{int64(3)},
					{int64(12)},
					{int64(13)},
				},
			},
		},
	},
	{
		Name: "Parameters resolve inside of INSERT",
		SetUpScript: []string{
			`CREATE TABLE items (
	id INT PRIMARY KEY AUTO_INCREMENT,
	item TEXT NOT NULL
);`,
			`CREATE PROCEDURE add_item (IN txt TEXT) MODIFIES SQL DATA
INSERT INTO items (item) VALUES (txt)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				SkipResultCheckOnServerEngine: true, // call depends on stored procedure stmt for whether to use 'query' or 'exec' from go sql driver.
				Query:                         "CALL add_item('A test item');",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1, InsertID: 1}},
				},
			},
			{
				Query: "SELECT * FROM items;",
				Expected: []sql.UntypedSqlRow{
					{1, "A test item"},
				},
			},
		},
	},
	{
		Name: "Parameters resolve inside of SELECT UNION",
		SetUpScript: []string{
			"CREATE TABLE t1(pk BIGINT PRIMARY KEY, v1 BIGINT)",
			"INSERT INTO t1 VALUES (1, 2)",
			"SELECT pk, v1 FROM t1 UNION SELECT 1, 2;",
			`CREATE PROCEDURE p1(x BIGINT, y BIGINT)
BEGIN
	SELECT pk+x, v1+y FROM t1 UNION SELECT x, y;
END;`,
			`CREATE PROCEDURE p2(u BIGINT, v BIGINT) SELECT pk+u, v1+v FROM t1 UNION SELECT u, v;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1(3, 4)",
				Expected: []sql.UntypedSqlRow{
					{4, 6},
					{3, 4},
				},
			},
			{
				Query: "CALL p2(5, 6)",
				Expected: []sql.UntypedSqlRow{
					{6, 8},
					{5, 6},
				},
			},
		},
	},
	{
		Name: "Parameters resolve inside of REPLACE",
		SetUpScript: []string{
			`CREATE TABLE items (
	id INT PRIMARY KEY AUTO_INCREMENT,
	item INT NOT NULL
);`,
			`CREATE PROCEDURE add_item (IN num INT) MODIFIES SQL DATA
BEGIN
	REPLACE INTO items (item) VALUES (5), (num), (num+1);
END`,
		},
		Assertions: []ScriptTestAssertion{
			{
				SkipResultCheckOnServerEngine: true, // call depends on stored procedure stmt for whether to use 'query' or 'exec' from go sql driver.
				Query:                         "CALL add_item(6);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(3)},
				},
			},
			{
				Query: "SELECT * FROM items ORDER BY 1;",
				Expected: []sql.UntypedSqlRow{
					{1, 5},
					{2, 6},
					{3, 7},
				},
			},
		},
	},
	{
		Name: "Parameters resolve inside of INSERT INTO SELECT",
		SetUpScript: []string{
			"CREATE TABLE t1(pk BIGINT PRIMARY KEY)",
			"CREATE TABLE t2(pk BIGINT PRIMARY KEY)",
			"INSERT INTO t1 VALUES (1), (2)",
			`CREATE PROCEDURE p1(x BIGINT)
BEGIN
	TRUNCATE t2;
	INSERT INTO t2 SELECT pk+x FROM t1;
	SELECT * FROM t2;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1(0)",
				Expected: []sql.UntypedSqlRow{
					{int64(1)},
					{int64(2)},
				},
			},
			{
				Query: "CALL p1(5)",
				Expected: []sql.UntypedSqlRow{
					{int64(6)},
					{int64(7)},
				},
			},
		},
	},
	{
		Name: "Subquery on SET user variable captures parameter",
		SetUpScript: []string{
			`CREATE PROCEDURE p1(x VARCHAR(20)) BEGIN SET @randomvar = (SELECT LENGTH(x)); SELECT @randomvar; END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				SkipResultCheckOnServerEngine: true, // the user var has null type, which returns nil value over the wire.
				Query:                         "CALL p1('hi')",
				Expected: []sql.UntypedSqlRow{
					{int64(2)},
				},
			},
			{
				Query: "CALL p1('hello')",
				Expected: []sql.UntypedSqlRow{
					{int64(5)},
				},
			},
		},
	},
	{
		Name: "Simple SELECT INTO",
		SetUpScript: []string{
			"CREATE PROCEDURE testabc(IN x DOUBLE, IN y DOUBLE, OUT abc DOUBLE) SELECT x*y INTO abc",
			"CALL testabc(2, 3, @res1)",
			"CALL testabc(9, 9.5, @res2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT @res1",
				Expected: []sql.UntypedSqlRow{{float64(6)}},
			},
			{
				Query:    "SELECT @res2",
				Expected: []sql.UntypedSqlRow{{float64(85.5)}},
			},
		},
	},
	{
		Name: "Multiple variables in SELECT INTO",
		SetUpScript: []string{
			"CREATE PROCEDURE new_proc(IN x DOUBLE, IN y DOUBLE, OUT abc DOUBLE, OUT def DOUBLE) SELECT x*y, x+y INTO abc, def",
			"CALL new_proc(2, 3, @res1, @res2)",
			"CALL new_proc(9, 9.5, @res3, @res4)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT @res1, @res2",
				Expected: []sql.UntypedSqlRow{{float64(6), float64(5)}},
			},
			{
				Query:    "SELECT @res3, @res4",
				Expected: []sql.UntypedSqlRow{{float64(85.5), float64(18.5)}},
			},
		},
	},
	{
		Name: "SELECT INTO with condition",
		SetUpScript: []string{
			"CREATE TABLE inventory (item_id int primary key, shelf_id int, items varchar(100))",
			"INSERT INTO inventory VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c'), (4, 1, 'd'), (5, 4, 'e')",
			"CREATE PROCEDURE in_stock (IN p_id INT, OUT p_count INT) SELECT COUNT(*) FROM inventory WHERE shelf_id = p_id INTO p_count",
			"CALL in_stock(1, @shelf1)",
			"CALL in_stock(2, @shelf2)",
			"CALL in_stock(3, @shelf3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT @shelf1, @shelf2, @shelf3",
				Expected: []sql.UntypedSqlRow{{3, 1, 0}},
			},
		},
	},
	{
		Name: "SELECT INTO with group by, order by and limit",
		SetUpScript: []string{
			"CREATE TABLE inventory (item_id int primary key, shelf_id int, item varchar(10))",
			"INSERT INTO inventory VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c'), (4, 1, 'd'), (5, 4, 'e')",
			"CREATE PROCEDURE first_shelf (OUT p_count INT) SELECT COUNT(*) FROM inventory GROUP BY shelf_id ORDER BY shelf_id ASC LIMIT 1 INTO p_count",
			"CREATE PROCEDURE last_shelf (OUT p_count INT) SELECT COUNT(*) FROM inventory GROUP BY shelf_id ORDER BY shelf_id DESC LIMIT 1 INTO p_count",
			"CALL first_shelf(@result1)",
			"CALL last_shelf(@result2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT @result1",
				Expected: []sql.UntypedSqlRow{{3}},
			},
			{
				Query:    "SELECT @result2",
				Expected: []sql.UntypedSqlRow{{1}},
			},
		},
	},
	{
		Name: "multiple SELECT INTO in begin end block",
		SetUpScript: []string{
			"CREATE TABLE inventory (item_id int primary key, shelf_id int, item varchar(10))",
			"INSERT INTO inventory VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c'), (4, 1, 'd'), (5, 4, 'e')",
			"CREATE PROCEDURE random_info(OUT p_count1 INT, OUT p_count2 VARCHAR(10)) BEGIN " +
				"SELECT COUNT(*) FROM inventory GROUP BY shelf_id ORDER BY shelf_id ASC LIMIT 1 INTO p_count1;" +
				"SELECT item INTO p_count2 FROM inventory WHERE shelf_id = 1 ORDER BY item DESC LIMIT 1; " +
				"END",
			"CALL random_info(@s1, @s2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT @s1, @s2",
				Expected: []sql.UntypedSqlRow{{3, "d"}},
			},
		},
	},
	{
		Name: "multiple statement with single SELECT INTO in begin end block",
		SetUpScript: []string{
			"CREATE TABLE inventory (item_id int primary key, shelf_id int, item varchar(10))",
			"INSERT INTO inventory VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c'), (4, 1, 'd'), (5, 4, 'e')",
			`CREATE PROCEDURE count_and_print(IN p_shelf_id INT, OUT p_count INT) BEGIN
SELECT item FROM inventory WHERE shelf_id = p_shelf_id ORDER BY item ASC;
SELECT COUNT(*) INTO p_count FROM inventory WHERE shelf_id = p_shelf_id;
END`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL count_and_print(1, @total)",
				Expected: []sql.UntypedSqlRow{{"a"}, {"b"}, {"d"}},
			},
			{
				Query:    "SELECT @total",
				Expected: []sql.UntypedSqlRow{{3}},
			},
		},
	},
	{
		Name: "DECLARE variables, proper nesting support",
		SetUpScript: []string{
			`CREATE PROCEDURE p1(OUT x BIGINT)
BEGIN
	DECLARE a INT;
	DECLARE b MEDIUMINT;
	DECLARE c VARCHAR(20);
	SELECT 1, 2, 'a' INTO a, b, c;
	BEGIN
		DECLARE b MEDIUMINT;
		SET a = 4;
		SET b = 5;
	END;
	SET x = a + b;
	SELECT a, b, c;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1(@x);",
				Expected: []sql.UntypedSqlRow{
					{4, 2, "a"},
				},
			},
			{
				Query: "SELECT @x;",
				Expected: []sql.UntypedSqlRow{
					{6},
				},
			},
		},
	},
	{
		Name: "DECLARE multiple variables, same statement",
		SetUpScript: []string{
			`CREATE PROCEDURE p1()
BEGIN
	DECLARE a, b, c INT;
	SELECT 2, 3, 4 INTO a, b, c;
	SELECT a + b + c;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1();",
				Expected: []sql.UntypedSqlRow{
					{9},
				},
			},
		},
	},
	{
		Name: "DECLARE variable shadows parameter",
		SetUpScript: []string{
			`CREATE PROCEDURE p1(INOUT x INT)
BEGIN
	DECLARE x INT;
	SET x = 5;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SET @x = 2;",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "CALL p1(@x);",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query: "SELECT @x;",
				Expected: []sql.UntypedSqlRow{
					{2},
				},
			},
		},
	},
	{
		Name: "DECLARE CONDITION",
		SetUpScript: []string{
			`CREATE PROCEDURE p1(x INT)
BEGIN
	DECLARE specialty CONDITION FOR SQLSTATE '45000';
	DECLARE specialty2 CONDITION FOR SQLSTATE '02000';
	IF x = 0 THEN
		SIGNAL SQLSTATE '01000';
	ELSEIF x = 1 THEN
		SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'A custom error occurred 1';
	ELSEIF x = 2 THEN
		SIGNAL specialty
			SET MESSAGE_TEXT = 'A custom error occurred 2', MYSQL_ERRNO = 1002;
	ELSEIF x = 3 THEN
		SIGNAL specialty;
	ELSEIF x = 4 THEN
		SIGNAL specialty2;
	ELSE
		SIGNAL SQLSTATE '01000'
			SET MESSAGE_TEXT = 'A warning occurred', MYSQL_ERRNO = 1000;
		SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'An error occurred', MYSQL_ERRNO = 1001;
	END IF;
	BEGIN
		DECLARE specialty3 CONDITION FOR SQLSTATE '45000';
	END;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "CALL p1(0)",
				ExpectedErrStr: "warnings not yet implemented",
			},
			{
				Query:          "CALL p1(1)",
				ExpectedErrStr: "A custom error occurred 1 (errno 1644) (sqlstate 45000)",
			},
			{
				Query:          "CALL p1(2)",
				ExpectedErrStr: "A custom error occurred 2 (errno 1002) (sqlstate 45000)",
			},
			{
				Query:          "CALL p1(3)",
				ExpectedErrStr: "Unhandled user-defined exception condition (errno 1644) (sqlstate 45000)",
			},
			{
				Query:          "CALL p1(4)",
				ExpectedErrStr: "Unhandled user-defined not found condition (errno 1643) (sqlstate 02000)",
			},
		},
	},
	{
		Name: "DECLARE CONDITION nesting priority",
		SetUpScript: []string{
			`CREATE PROCEDURE p1(x INT)
BEGIN
	DECLARE cond_name CONDITION FOR SQLSTATE '02000';
	BEGIN
		DECLARE cond_name CONDITION FOR SQLSTATE '45000';
		IF x = 0 THEN
			SIGNAL cond_name;
		END IF;
	END;
	SIGNAL cond_name;
END;`,
			`CREATE PROCEDURE p2(x INT)
BEGIN
	DECLARE cond_name CONDITION FOR SQLSTATE '45000';
	BEGIN
		DECLARE cond_name CONDITION FOR SQLSTATE '02000';
		IF x = 0 THEN
			SIGNAL cond_name;
		END IF;
	END;
	SIGNAL cond_name;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "CALL p1(0)",
				ExpectedErrStr: "Unhandled user-defined exception condition (errno 1644) (sqlstate 45000)",
			},
			{
				Query:          "CALL p1(1)",
				ExpectedErrStr: "Unhandled user-defined not found condition (errno 1643) (sqlstate 02000)",
			},
			{
				Query:          "CALL p2(0)",
				ExpectedErrStr: "Unhandled user-defined not found condition (errno 1643) (sqlstate 02000)",
			},
			{
				Query:          "CALL p2(1)",
				ExpectedErrStr: "Unhandled user-defined exception condition (errno 1644) (sqlstate 45000)",
			},
		},
	},
	{
		Name: "FETCH multiple rows",
		SetUpScript: []string{
			`CREATE TABLE t1 (pk BIGINT PRIMARY KEY);`,
			`CREATE PROCEDURE p1()
BEGIN
	DECLARE a, b INT;
	DECLARE cur1 CURSOR FOR SELECT pk FROM t1;
	DELETE FROM t1;
    INSERT INTO t1 VALUES (1), (2);
    OPEN cur1;
    FETCH cur1 INTO a;
    FETCH cur1 INTO b;
    CLOSE cur1;
    SELECT a, b;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1();",
				Expected: []sql.UntypedSqlRow{
					{1, 2},
				},
			},
		},
	},
	{
		Name: "FETCH with multiple opens and closes",
		SetUpScript: []string{
			`CREATE TABLE t1 (pk BIGINT PRIMARY KEY);`,
			`CREATE PROCEDURE p1()
BEGIN
	DECLARE a, b INT;
	DECLARE cur1 CURSOR FOR SELECT pk FROM t1;
	DELETE FROM t1;
    INSERT INTO t1 VALUES (1);
    OPEN cur1;
    FETCH cur1 INTO a;
    CLOSE cur1;
	UPDATE t1 SET pk = 2;
    OPEN cur1;
    FETCH cur1 INTO b;
    CLOSE cur1;
    SELECT a, b;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1();",
				Expected: []sql.UntypedSqlRow{
					{1, 2},
				},
			},
		},
	},
	{
		Name: "issue 7458: proc params as limit values",
		SetUpScript: []string{
			"create table t (i int primary key);",
			"insert into t values (0), (1), (2), (3)",
			"CREATE PROCEDURE limited(the_limit int, the_offset bigint) SELECT * FROM t LIMIT the_limit OFFSET the_offset",
			"CREATE PROCEDURE limited_uns(the_limit int unsigned, the_offset bigint unsigned) SELECT * FROM t LIMIT the_limit OFFSET the_offset",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "call limited(1,0)",
				Expected: []sql.UntypedSqlRow{{0}},
			},
			{
				Query:    "call limited(2,0)",
				Expected: []sql.UntypedSqlRow{{0}, {1}},
			},
			{
				Query:    "call limited(2,2)",
				Expected: []sql.UntypedSqlRow{{2}, {3}},
			},
			{
				Query:    "call limited_uns(2,2)",
				Expected: []sql.UntypedSqlRow{{2}, {3}},
			},
			{
				Query:          "CREATE PROCEDURE limited_inv(the_limit CHAR(3), the_offset INT) SELECT * FROM t LIMIT the_limit OFFSET the_offset",
				ExpectedErrStr: "the variable 'the_limit' has a non-integer based type: char(3)",
			},
			{
				Query:          "CREATE PROCEDURE limited_inv(the_limit float, the_offset INT) SELECT * FROM t LIMIT the_limit OFFSET the_offset",
				ExpectedErrStr: "the variable 'the_limit' has a non-integer based type: float",
			},
		},
	},
	{
		Name: "FETCH captures state at OPEN",
		SetUpScript: []string{
			`CREATE TABLE t1 (pk BIGINT PRIMARY KEY);`,
			`CREATE PROCEDURE p1()
BEGIN
	DECLARE a, b INT;
	DECLARE cur1 CURSOR FOR SELECT pk FROM t1;
	DELETE FROM t1;
    INSERT INTO t1 VALUES (1);
    OPEN cur1;
	UPDATE t1 SET pk = 2;
    FETCH cur1 INTO a;
    CLOSE cur1;
    OPEN cur1;
    FETCH cur1 INTO b;
    CLOSE cur1;
    SELECT a, b;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1();",
				Expected: []sql.UntypedSqlRow{
					{1, 2},
				},
			},
		},
	},
	{
		Name: "FETCH implicitly closes",
		SetUpScript: []string{
			`CREATE TABLE t1 (pk BIGINT PRIMARY KEY);`,
			`CREATE PROCEDURE p1()
BEGIN
	DECLARE a INT;
	DECLARE cur1 CURSOR FOR SELECT pk FROM t1;
	DELETE FROM t1;
    INSERT INTO t1 VALUES (4);
    OPEN cur1;
    FETCH cur1 INTO a;
    SELECT a;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1();",
				Expected: []sql.UntypedSqlRow{
					{4},
				},
			},
		},
	},
	{
		Name: "SQLEXCEPTION declare handler",
		SetUpScript: []string{
			`DROP TABLE IF EXISTS t1;`,
			`CREATE TABLE t1 (pk BIGINT PRIMARY KEY);`,
			`CREATE PROCEDURE eof()
BEGIN
	DECLARE a, b INT DEFAULT 1;
    DECLARE cur1 CURSOR FOR SELECT * FROM t1;
    OPEN cur1;
    BEGIN
		DECLARE EXIT HANDLER FOR SQLEXCEPTION SET a = 7;
		tloop: LOOP
			FETCH cur1 INTO b;
            IF a > 1000 THEN
				LEAVE tloop;
            END IF;
		END LOOP;
    END;
    CLOSE cur1;
    SELECT a;
END;`,
			`CREATE PROCEDURE duplicate_key()
BEGIN
	DECLARE a, b INT DEFAULT 1;
    BEGIN
		DECLARE EXIT HANDLER FOR SQLEXCEPTION SET a = 7;
		INSERT INTO t1 values (0);
    END;
    SELECT a;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL eof();",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "CALL duplicate_key();",
				Expected: []sql.UntypedSqlRow{{1}},
			},
			{
				Query:    "CALL duplicate_key();",
				Expected: []sql.UntypedSqlRow{{7}},
			},
		},
	},
	{
		Name: "DECLARE CONTINUE HANDLER",
		SetUpScript: []string{
			"CREATE TABLE t1(id CHAR(16) primary key, data INT)",
			"CREATE TABLE t2(i INT)",
			"CREATE TABLE t3(id CHAR(16) primary key, data INT)",
			`CREATE PROCEDURE curdemo()
BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE a CHAR(16);
  DECLARE b, c INT;
  DECLARE cur1 CURSOR FOR SELECT id,data FROM t1;
  DECLARE cur2 CURSOR FOR SELECT i FROM t2;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  OPEN cur1;
  OPEN cur2;

  read_loop: LOOP
    FETCH cur1 INTO a, b;
    FETCH cur2 INTO c;
    IF done THEN
      LEAVE read_loop;
    END IF;
    IF b < c THEN
      INSERT INTO t3 VALUES (a,b);
    ELSE
      INSERT INTO t3 VALUES (a,c);
    END IF;
  END LOOP;

  CLOSE cur1;
  CLOSE cur2;
  SELECT "success";
END`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL curdemo()",
				Expected: []sql.UntypedSqlRow{{"success"}},
			},
			{
				Query:    "SELECT * from t3",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "INSERT INTO t1 values ('a', 10), ('b', 20)",
			},
			{
				Query: "INSERT INTO t2 values (15), (15)",
			},
			{
				Query:    "CALL curdemo()",
				Expected: []sql.UntypedSqlRow{{"success"}},
			},
			{
				Query:    "SELECT * from t3",
				Expected: []sql.UntypedSqlRow{{"a", 10}, {"b", 15}},
			},
		},
	},
	{
		Name: "DECLARE HANDLERs exit according to the block they were declared in",
		SetUpScript: []string{
			`DROP TABLE IF EXISTS t1;`,
			`CREATE TABLE t1 (pk BIGINT PRIMARY KEY);`,
			`CREATE PROCEDURE outer_declare()
BEGIN
	DECLARE a, b INT DEFAULT 1;
    DECLARE cur1 CURSOR FOR SELECT * FROM t1;
	DECLARE EXIT HANDLER FOR NOT FOUND SET a = 1001;
    OPEN cur1;
    BEGIN
		tloop: LOOP
			FETCH cur1 INTO b;
            IF a > 1000 THEN
				LEAVE tloop;
            END IF;
		END LOOP;
    END;
    CLOSE cur1;
    SELECT a;
END;`,
			`CREATE PROCEDURE inner_declare()
BEGIN
	DECLARE a, b INT DEFAULT 1;
    DECLARE cur1 CURSOR FOR SELECT * FROM t1;
	DECLARE EXIT HANDLER FOR NOT FOUND SET a = a + 1;
    OPEN cur1;
    BEGIN
		DECLARE EXIT HANDLER FOR NOT FOUND SET a = 1001;
		tloop: LOOP
			FETCH cur1 INTO b;
            IF a > 1000 THEN
				LEAVE tloop;
            END IF;
		END LOOP;
    END;
    CLOSE cur1;
    SELECT a;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL outer_declare();",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "CALL inner_declare();",
				Expected: []sql.UntypedSqlRow{
					{1001},
				},
			},
		},
	},
	{
		Name: "Labeled BEGIN...END",
		SetUpScript: []string{
			`CREATE PROCEDURE p1()
BEGIN
	DECLARE a INT DEFAULT 1;
	tblock: BEGIN
		LOOP
			SET a = a + 3;
			LEAVE tblock;
		END LOOP;
	END;
	SELECT a;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1();",
				Expected: []sql.UntypedSqlRow{
					{4},
				},
			},
			{
				Query:       `CREATE PROCEDURE p2() BEGIN tblock: BEGIN ITERATE tblock; END; END;`,
				ExpectedErr: sql.ErrLoopLabelNotFound,
			},
		},
	},
	{
		Name: "REPEAT runs loop before first evaluation",
		SetUpScript: []string{
			`CREATE PROCEDURE p1()
BEGIN
	DECLARE a INT DEFAULT 10;
	REPEAT
		SET a = a * 5;
	UNTIL a > 0
	END REPEAT;
    SELECT a;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1();",
				Expected: []sql.UntypedSqlRow{
					{50},
				},
			},
		},
	},
	{
		Name: "WHILE runs evaluation before first loop",
		SetUpScript: []string{
			`CREATE PROCEDURE p1()
BEGIN
	DECLARE a INT DEFAULT 10;
	WHILE a < 10 DO
		SET a = a * 10;
	END WHILE;
    SELECT a;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1();",
				Expected: []sql.UntypedSqlRow{
					{10},
				},
			},
		},
	},
	{
		Name: "ITERATE and LEAVE LOOP",
		SetUpScript: []string{
			`CREATE TABLE t1 (pk BIGINT PRIMARY KEY);`,
			`INSERT INTO t1 VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9)`,
			`CREATE PROCEDURE p1()
BEGIN
	DECLARE a, b INT DEFAULT 1;
    DECLARE cur1 CURSOR FOR SELECT * FROM t1;
	DECLARE EXIT HANDLER FOR NOT FOUND BEGIN END;
    OPEN cur1;
    BEGIN
		tloop: LOOP
			FETCH cur1 INTO b;
			SET a = (a + b) * 10;
            IF a < 1000 THEN
				ITERATE tloop;
			ELSE
				LEAVE tloop;
            END IF;
		END LOOP;
    END;
    CLOSE cur1;
    SELECT a;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1();",
				Expected: []sql.UntypedSqlRow{
					{2230},
				},
			},
		},
	},
	{
		Name: "ITERATE and LEAVE REPEAT",
		SetUpScript: []string{
			`CREATE TABLE t1 (pk BIGINT PRIMARY KEY);`,
			`INSERT INTO t1 VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9)`,
			`CREATE PROCEDURE p1()
BEGIN
	DECLARE a, b INT DEFAULT 1;
    DECLARE cur1 CURSOR FOR SELECT * FROM t1;
	DECLARE EXIT HANDLER FOR NOT FOUND BEGIN END;
    OPEN cur1;
    BEGIN
		tloop: REPEAT
			FETCH cur1 INTO b;
			SET a = (a + b) * 10;
            IF a < 1000 THEN
				ITERATE tloop;
			ELSE
				LEAVE tloop;
            END IF;
		UNTIL false
		END REPEAT;
    END;
    CLOSE cur1;
    SELECT a;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1();",
				Expected: []sql.UntypedSqlRow{
					{2230},
				},
			},
		},
	},
	{
		Name: "ITERATE and LEAVE WHILE",
		SetUpScript: []string{
			`CREATE TABLE t1 (pk BIGINT PRIMARY KEY);`,
			`INSERT INTO t1 VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9)`,
			`CREATE PROCEDURE p1()
BEGIN
	DECLARE a, b INT DEFAULT 1;
    DECLARE cur1 CURSOR FOR SELECT * FROM t1;
	DECLARE EXIT HANDLER FOR NOT FOUND BEGIN END;
    OPEN cur1;
    BEGIN
		tloop: WHILE true DO
			FETCH cur1 INTO b;
			SET a = (a + b) * 10;
            IF a < 1000 THEN
				ITERATE tloop;
			ELSE
				LEAVE tloop;
            END IF;
		END WHILE;
    END;
    CLOSE cur1;
    SELECT a;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1();",
				Expected: []sql.UntypedSqlRow{
					{2230},
				},
			},
		},
	},
	{
		Name: "Handle setting an uninitialized user variable",
		SetUpScript: []string{
			`CREATE PROCEDURE p1(INOUT param VARCHAR(10))
BEGIN
	SELECT param;
	SET param = '5';
END`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1(@uservar4);",
				Expected: []sql.UntypedSqlRow{
					{nil},
				},
			},
			{
				Query: "SELECT @uservar4;",
				Expected: []sql.UntypedSqlRow{
					{"5"},
				},
			},
		},
	},
	{
		Name: "Dolt Issue #4980",
		SetUpScript: []string{
			`CREATE TABLE person_cal_entries (id VARCHAR(36) PRIMARY KEY, cal_entry_id_fk VARCHAR(36), person_id_fk VARCHAR(36));`,
			`CREATE TABLE personnel (id VARCHAR(36) PRIMARY KEY, event_id VARCHAR(36));`,
			`CREATE TABLE season_participants (person_id_fk VARCHAR(36), season_id_fk VARCHAR(36));`,
			`CREATE TABLE cal_entries (id VARCHAR(36) PRIMARY KEY, season_id_fk VARCHAR(36));`,
			`INSERT INTO personnel VALUES ('6140e23e-7b9b-11ed-a1eb-0242ac120002', 'c546abc4-7b9b-11ed-a1eb-0242ac120002');`,
			`INSERT INTO season_participants VALUES ('6140e23e-7b9b-11ed-a1eb-0242ac120002', '46d7041e-7b9b-11ed-a1eb-0242ac120002');`,
			`INSERT INTO cal_entries VALUES ('cb8ba301-6c27-4bf8-b99b-617082d72621', '46d7041e-7b9b-11ed-a1eb-0242ac120002');`,
			`CREATE PROCEDURE create_cal_entries_for_event(IN event_id VARCHAR(36))
BEGIN
    INSERT INTO person_cal_entries (id, cal_entry_id_fk, person_id_fk)
    SELECT 'd17cb898-7b9b-11ed-a1eb-0242ac120002' as id, event_id as cal_entry_id_fk, id as person_id_fk
    FROM personnel
    WHERE id IN (
        SELECT person_id_fk
        FROM season_participants
        WHERE season_id_fk = (
            SELECT season_id_fk
            FROM cal_entries
            WHERE id = event_id
        )
    );
END`,
		},
		Assertions: []ScriptTestAssertion{
			{
				SkipResultCheckOnServerEngine: true, // call depends on stored procedure stmt for whether to use 'query' or 'exec' from go sql driver.
				Query:                         "call create_cal_entries_for_event('cb8ba301-6c27-4bf8-b99b-617082d72621');",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "SELECT * FROM person_cal_entries;",
				Expected: []sql.UntypedSqlRow{
					{"d17cb898-7b9b-11ed-a1eb-0242ac120002", "cb8ba301-6c27-4bf8-b99b-617082d72621", "6140e23e-7b9b-11ed-a1eb-0242ac120002"},
				},
			},
		},
	},
	{
		Name: "Conditional expression where body has its own columns",
		SetUpScript: []string{
			"CREATE TABLE test (id INT);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
CREATE PROCEDURE populate(IN val INT)
BEGIN
	IF (SELECT COUNT(*) FROM test where id = val) = 0 THEN
        INSERT INTO test (id) VALUES (val);
    END IF;
END;`,
				Expected: []sql.UntypedSqlRow{{types.OkResult{}}},
			},
			{
				Query: "CALL populate(1);",
				Expected: []sql.UntypedSqlRow{{types.OkResult{
					RowsAffected: 1,
				}}},
				SkipResultCheckOnServerEngine: true,
			},
			{
				Query: "SELECT * FROM test;",
				Expected: []sql.UntypedSqlRow{
					{1},
				},
			},
		},
	},
	{
		Name: "Nested subquery in conditional expression where body has its own columns",
		SetUpScript: []string{
			"CREATE TABLE test (id INT);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
CREATE PROCEDURE populate(IN val INT)
BEGIN
	IF (SELECT COUNT(*) FROM test where (select t2.id from test t2 where t2.id = test.id) = val) = 0 THEN
        INSERT INTO test (id) VALUES (val);
    END IF;
END;`,
				Expected: []sql.UntypedSqlRow{{types.OkResult{}}},
			},
			{
				Query: "CALL populate(1);",
				Expected: []sql.UntypedSqlRow{{types.OkResult{
					RowsAffected: 1,
				}}},
				SkipResultCheckOnServerEngine: true,
			},
			{
				Query: "SELECT * FROM test;",
				Expected: []sql.UntypedSqlRow{
					{1},
				},
			},
		},
	},
	{
		Name: "Conditional expression with else doesn't have body columns in its scope",
		SetUpScript: []string{
			"CREATE TABLE test (id INT);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
CREATE PROCEDURE populate(IN val INT)
BEGIN
	IF (SELECT COUNT(*) FROM test where id = val) = 0 THEN
        INSERT INTO test (id) VALUES (val);
    ELSE
		SELECT 0;
    END IF;
END;`,
				Expected: []sql.UntypedSqlRow{{types.OkResult{}}},
			},
			{
				Query: "CALL populate(1);",
				Expected: []sql.UntypedSqlRow{{types.OkResult{
					RowsAffected: 1,
				}}},
				SkipResultCheckOnServerEngine: true,
			},
			{
				Query: "SELECT * FROM test;",
				Expected: []sql.UntypedSqlRow{
					{1},
				},
			},
		},
	},
	{
		Name: "HANDLERs ignore variables declared after them",
		SetUpScript: []string{
			`CREATE TABLE t1 (pk BIGINT PRIMARY KEY);`,
			`CREATE PROCEDURE p1()
BEGIN
	DECLARE dvar BIGINT DEFAULT 1;
	DECLARE cur1 CURSOR FOR SELECT * FROM t1;
    OPEN cur1;
	BEGIN
		DECLARE EXIT HANDLER FOR NOT FOUND SET dvar = 10;
		BEGIN
			DECLARE dvar BIGINT DEFAULT 2;
			BEGIN
				DECLARE dvar BIGINT DEFAULT 3;
				LOOP
					FETCH cur1 INTO dvar; # Handler is triggered here, but should only set the first "dvar"
				END LOOP;
            END;
		END;
    END;
    SELECT dvar;
END`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1();",
				Expected: []sql.UntypedSqlRow{
					{10},
				},
			},
		},
	},
	{
		Name:        "Duplicate parameter names",
		Query:       "CREATE PROCEDURE p1(abc DATETIME, abc DOUBLE) SELECT abc",
		ExpectedErr: sql.ErrDeclareVariableDuplicate,
	},
	{
		Name:        "Duplicate parameter names mixed casing",
		Query:       "CREATE PROCEDURE p1(abc DATETIME, ABC DOUBLE) SELECT abc",
		ExpectedErr: sql.ErrDeclareVariableDuplicate,
	},
	{
		Name:        "Invalid parameter type",
		Query:       "CREATE PROCEDURE p1(x FAKETYPE) SELECT x",
		ExpectedErr: sql.ErrSyntaxError,
	},
	{ // This statement is not allowed in stored procedures, and is caught by the vitess parser.
		Name:        "Invalid USE statement",
		Query:       `CREATE PROCEDURE p1() USE mydb`,
		ExpectedErr: sql.ErrSyntaxError,
	},
	{ // These statements are not allowed in stored procedures, and are caught by the vitess parser.
		Name: "Invalid LOCK/UNLOCK statements",
		SetUpScript: []string{
			"CREATE TABLE t1(pk BIGINT PRIMARY KEY)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE PROCEDURE p1(x BIGINT) LOCK TABLES t1 READ",
				ExpectedErr: sql.ErrSyntaxError,
			},
			{
				Query:       "CREATE PROCEDURE p1(x BIGINT) UNLOCK TABLES",
				ExpectedErr: sql.ErrSyntaxError,
			},
		},
	},
	{
		Name: "DECLARE CONDITION wrong positions",
		Assertions: []ScriptTestAssertion{
			{
				Query: `CREATE PROCEDURE p1(x INT)
BEGIN
	SELECT x;
	DECLARE cond_name CONDITION FOR SQLSTATE '45000';
END;`,
				ExpectedErr: sql.ErrDeclareConditionOrderInvalid,
			},
			{
				Query: `CREATE PROCEDURE p1(x INT)
BEGIN
	BEGIN
		SELECT x;
		DECLARE cond_name CONDITION FOR SQLSTATE '45000';
	END;
END;`,
				ExpectedErr: sql.ErrDeclareConditionOrderInvalid,
			},
			{
				Query: `CREATE PROCEDURE p1(x INT)
BEGIN
	IF x = 0 THEN
		DECLARE cond_name CONDITION FOR SQLSTATE '45000';
	END IF;
END;`,
				ExpectedErr: sql.ErrDeclareConditionOrderInvalid,
			},
			{
				Query: `CREATE PROCEDURE p1(x INT)
BEGIN
	IF x = 0 THEN
		SELECT x;
	ELSE
		DECLARE cond_name CONDITION FOR SQLSTATE '45000';
	END IF;
END;`,
				ExpectedErr: sql.ErrDeclareConditionOrderInvalid,
			},
		},
	},
	{
		Name: "DECLARE CONDITION duplicate name",
		Query: `CREATE PROCEDURE p1()
BEGIN
	DECLARE cond_name CONDITION FOR SQLSTATE '45000';
	DECLARE cond_name CONDITION FOR SQLSTATE '45000';
END;`,
		ExpectedErr: sql.ErrDeclareConditionDuplicate,
	},
	{ //TODO: change this test when we implement DECLARE CONDITION for MySQL error codes
		Name: "SIGNAL references condition name for MySQL error code",
		Query: `CREATE PROCEDURE p1(x INT)
BEGIN
	DECLARE mysql_err_code CONDITION FOR 1000;
	SIGNAL mysql_err_code;
END;`,
		ExpectedErr: sql.ErrUnsupportedSyntax,
	},
	{
		Name: "SIGNAL non-existent condition name",
		Query: `CREATE PROCEDURE p1(x INT)
BEGIN
	DECLARE abcdefg CONDITION FOR SQLSTATE '45000';
	SIGNAL abcdef;
END;`,
		ExpectedErr: sql.ErrDeclareConditionNotFound,
	},
	{
		Name: "Duplicate procedure name",
		SetUpScript: []string{
			"CREATE PROCEDURE test_proc(x DOUBLE, y DOUBLE) SELECT x*y",
		},
		Query:       "CREATE PROCEDURE test_proc(z VARCHAR(20)) SELECT z",
		ExpectedErr: sql.ErrStoredProcedureAlreadyExists,
	},
	{
		Name: "Broken procedure shouldn't break other procedures",
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, other INT);",
			"INSERT INTO t VALUES (1, 1), (2, 2), (3, 3);",
			"CREATE PROCEDURE fragile() select other from t;",
			"CREATE PROCEDURE stable() select pk from t;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL stable();",
				Expected: []sql.UntypedSqlRow{{1}, {2}, {3}},
			},
			{
				Query:    "CALL fragile();",
				Expected: []sql.UntypedSqlRow{{1}, {2}, {3}},
			},
			{
				Query:            "SHOW PROCEDURE STATUS LIKE 'stable'",
				SkipResultsCheck: true, // ensure that there's no error
			},
			{
				Query:            "SHOW PROCEDURE STATUS LIKE 'fragile'",
				SkipResultsCheck: true, // ensure that there's no error
			},
			{
				Query:    "alter table t drop other;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "CALL stable();",
				Expected: []sql.UntypedSqlRow{{1}, {2}, {3}},
			},
			{
				Query:          "CALL fragile();",
				ExpectedErrStr: "column \"other\" could not be found in any table in scope",
			},
			{
				Query:            "SHOW PROCEDURE STATUS LIKE 'stable'",
				SkipResultsCheck: true, // ensure that there's no error
			},
			{
				Query:            "SHOW PROCEDURE STATUS LIKE 'fragile'",
				SkipResultsCheck: true, // ensure that there's no error
			},
			{
				Query:    "ALTER TABLE t ADD COLUMN other INT",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "CALL stable();",
				Expected: []sql.UntypedSqlRow{{1}, {2}, {3}},
			},
			{
				Query:    "CALL fragile();",
				Expected: []sql.UntypedSqlRow{{nil}, {nil}, {nil}},
			},
			{
				Query:    "INSERT INTO t VALUES (4, 4), (5, 5), (6, 6);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
			},
			{
				Query:    "CALL stable();",
				Expected: []sql.UntypedSqlRow{{1}, {2}, {3}, {4}, {5}, {6}},
			},
			{
				Query:    "CALL fragile();",
				Expected: []sql.UntypedSqlRow{{nil}, {nil}, {nil}, {4}, {5}, {6}},
			},
		},
	},
	{
		Name: "DECLARE name duplicate same type",
		Assertions: []ScriptTestAssertion{
			{
				Query: `CREATE PROCEDURE p1()
BEGIN
	DECLARE x INT;
	DECLARE x INT;
	SELECT 1;
END;`,
				ExpectedErr: sql.ErrDeclareVariableDuplicate,
			},
		},
	},
	{
		Name: "DECLARE name duplicate different type",
		Assertions: []ScriptTestAssertion{
			{
				Query: `CREATE PROCEDURE p1()
BEGIN
	DECLARE x INT;
	DECLARE x VARCHAR(20);
	SELECT 1;
END;`,
				ExpectedErr: sql.ErrDeclareVariableDuplicate,
			},
		},
	},
	{
		Name: "Variable, condition, and cursor in invalid order",
		Assertions: []ScriptTestAssertion{
			{
				Query: `CREATE PROCEDURE p1()
BEGIN
	DECLARE var_name INT;
	DECLARE cur_name CURSOR FOR SELECT 1;
	DECLARE cond_name CONDITION FOR SQLSTATE '45000';
	SELECT 1;
END;`,
				ExpectedErr: sql.ErrDeclareConditionOrderInvalid,
			},
			{
				Query: `CREATE PROCEDURE p2()
BEGIN
	DECLARE cond_name CONDITION FOR SQLSTATE '45000';
	DECLARE cur_name CURSOR FOR SELECT 1;
	DECLARE var_name INT;
	SELECT 1;
END;`,
				ExpectedErr: sql.ErrDeclareVariableOrderInvalid,
			},
			{
				Query: `CREATE PROCEDURE p3()
BEGIN
	DECLARE cond_name CONDITION FOR SQLSTATE '45000';
	DECLARE var_name INT;
	SELECT 1;
	DECLARE cur_name CURSOR FOR SELECT 1;
END;`,
				ExpectedErr: sql.ErrDeclareCursorOrderInvalid,
			},
		},
	},
	{
		Name: "FETCH non-existent cursor",
		Assertions: []ScriptTestAssertion{
			{
				Query: `CREATE PROCEDURE p1()
BEGIN
	DECLARE a INT;
	FETCH no_cursor INTO a;
END;`,
				ExpectedErr: sql.ErrCursorNotFound,
			},
		},
	},
	{
		Name: "OPEN non-existent cursor",
		Assertions: []ScriptTestAssertion{
			{
				Query: `CREATE PROCEDURE p1()
BEGIN
	OPEN no_cursor;
END;`,
				ExpectedErr: sql.ErrCursorNotFound,
			},
		},
	},
	{
		Name: "CLOSE non-existent cursor",
		Assertions: []ScriptTestAssertion{
			{
				Query: `CREATE PROCEDURE p1()
BEGIN
	CLOSE no_cursor;
END;`,
				ExpectedErr: sql.ErrCursorNotFound,
			},
		},
	},
	{
		Name: "CLOSE without OPEN",
		SetUpScript: []string{
			`CREATE PROCEDURE p1()
BEGIN
	DECLARE cur1 CURSOR FOR SELECT 1;
    CLOSE cur1;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CALL p1();",
				ExpectedErr: sql.ErrCursorNotOpen,
			},
		},
	},
	{
		Name: "OPEN repeatedly",
		SetUpScript: []string{
			`CREATE PROCEDURE p1()
BEGIN
	DECLARE cur1 CURSOR FOR SELECT 1;
    OPEN cur1;
    OPEN cur1;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CALL p1();",
				ExpectedErr: sql.ErrCursorAlreadyOpen,
			},
		},
	},
	{
		Name: "CLOSE repeatedly",
		SetUpScript: []string{
			`CREATE PROCEDURE p1()
BEGIN
	DECLARE cur1 CURSOR FOR SELECT 1;
    OPEN cur1;
    CLOSE cur1;
    CLOSE cur1;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CALL p1();",
				ExpectedErr: sql.ErrCursorNotOpen,
			},
		},
	},
	{
		Name: "With CTE using variable",
		SetUpScript: []string{
			`CREATE PROCEDURE p1()
BEGIN
	DECLARE v1 INT DEFAULT 1234;
	WITH cte as (SELECT v1)
	SELECT * FROM cte;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1();",
				Expected: []sql.UntypedSqlRow{
					{1234},
				},
			},
		},
	},
	{
		Name: "With CTE using parameter",
		SetUpScript: []string{
			`CREATE PROCEDURE p1(v1 int)
BEGIN
	WITH cte as (SELECT v1)
	SELECT * FROM cte;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1(1234);",
				Expected: []sql.UntypedSqlRow{
					{1234},
				},
			},
		},
	},
	{
		Name: "Dolt Issue #4480",
		SetUpScript: []string{
			"create table p1 (row_id int primary key, pred int, actual int)",
			"create table p2 (row_id int primary key, pred int, actual int)",
			"insert into p1 values (0, 0, 0), (1, 0, 1), (2, 1, 0), (3, 1, 1)",
			"insert into p2 values (0, 0, 0), (1, 0, 1), (2, 1, 0), (3, 1, 1)",
			`CREATE PROCEDURE computeSummary(c VARCHAR(200)) 
BEGIN
	with t as (
		select
			case
				when p1.pred = p2.actual then 1
			else 0
			end as correct,
			p1.actual
			from p1
			join p2
			on p1.row_id = p2.row_id
	)
	select
		sum(correct)/count(*),
		count(*) as row_num
		from t;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL computeSummary('i am not used');",
				Expected: []sql.UntypedSqlRow{
					{float64(0.5), 4},
				},
			},
		},
	},
}

var ProcedureCallTests = []ScriptTest{
	{
		Name: "OUT param with SET",
		SetUpScript: []string{
			"SET @outparam = 5",
			"CREATE PROCEDURE testabc(OUT x BIGINT) SET x = 9",
			"CALL testabc(@outparam)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT @outparam",
				Expected: []sql.UntypedSqlRow{
					{
						int64(9),
					},
				},
			},
		},
	},
	{
		Name: "OUT param without SET",
		SetUpScript: []string{
			"SET @outparam = 5",
			"CREATE PROCEDURE testabc(OUT x BIGINT) SELECT x",
			"CALL testabc(@outparam)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT @outparam",
				Expected: []sql.UntypedSqlRow{
					{
						nil,
					},
				},
			},
		},
	},
	{
		Name: "INOUT param with SET",
		SetUpScript: []string{
			"SET @outparam = 5",
			"CREATE PROCEDURE testabc(INOUT x BIGINT) BEGIN SET x = x + 1; SET x = x + 3; END;",
			"CALL testabc(@outparam)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT @outparam",
				Expected: []sql.UntypedSqlRow{
					{
						int64(9),
					},
				},
			},
		},
	},
	{
		Name: "INOUT param without SET",
		SetUpScript: []string{
			"SET @outparam = 5",
			"CREATE PROCEDURE testabc(INOUT x BIGINT) SELECT x",
			"CALL testabc(@outparam)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT @outparam",
				Expected: []sql.UntypedSqlRow{
					{
						int64(5),
					},
				},
			},
		},
	},
	{
		Name: "Nested CALL with INOUT param",
		SetUpScript: []string{
			"SET @outparam = 5",
			"CREATE PROCEDURE p3(INOUT z INT) BEGIN SET z = z * 111; END;",
			"CREATE PROCEDURE p2(INOUT y DOUBLE) BEGIN SET y = y + 4; CALL p3(y); END;",
			"CREATE PROCEDURE p1(INOUT x BIGINT) BEGIN SET x = 3; CALL p2(x); END;",
			"CALL p1(@outparam)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT @outparam",
				Expected: []sql.UntypedSqlRow{
					{
						int64(777),
					},
				},
			},
		},
	},
	{
		Name: "OUT param without SET",
		SetUpScript: []string{
			"SET @outparam = 5",
			"CREATE PROCEDURE testabc(OUT x BIGINT) SELECT x",
			"CALL testabc(@outparam)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT @outparam",
				Expected: []sql.UntypedSqlRow{
					{
						nil,
					},
				},
			},
		},
	},
	{
		Name: "Incompatible type for parameter",
		SetUpScript: []string{
			"CREATE PROCEDURE p1(x DATETIME) SELECT x",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CALL p1('hi')",
				ExpectedErr: types.ErrConvertingToTime,
			},
		},
	},
	{
		Name: "Incorrect parameter count",
		SetUpScript: []string{
			"CREATE PROCEDURE p1(x BIGINT, y BIGINT) SELECT x + y",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CALL p1(1)",
				ExpectedErr: sql.ErrCallIncorrectParameterCount,
			},
			{
				Query:       "CALL p1(1, 2, 3)",
				ExpectedErr: sql.ErrCallIncorrectParameterCount,
			},
		},
	},
	{
		Name: "use procedure parameter in filter expressions and multiple statements",
		SetUpScript: []string{
			"CREATE TABLE inventory (store_id int, product varchar(5))",
			"INSERT INTO inventory VALUES (1, 'a'), (1, 'b'), (1, 'c'), (1, 'd'), (2, 'e'), (2, 'f'), (1, 'g'), (1, 'h'), (3, 'i')",
			"CREATE PROCEDURE proc1 (IN p_store_id INT) SELECT COUNT(*) FROM inventory WHERE store_id = p_store_id;",
			"CREATE PROCEDURE proc2 (IN p_store_id INT, OUT p_film_count INT) READS SQL DATA BEGIN SELECT COUNT(*) as counted FROM inventory WHERE store_id = p_store_id; SET p_film_count = 44; END ;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL proc1(1)",
				Expected: []sql.UntypedSqlRow{
					{
						int64(6),
					},
				},
			},
			{
				Query: "CALL proc1(2)",
				Expected: []sql.UntypedSqlRow{
					{
						int64(2),
					},
				},
			}, {
				Query: "CALL proc1(4)",
				Expected: []sql.UntypedSqlRow{
					{
						int64(0),
					},
				},
			}, {
				Query: "CALL proc2(3, @foo)",
				Expected: []sql.UntypedSqlRow{
					{
						int64(1),
					},
				},
			}, {
				Query: "SELECT @foo",
				Expected: []sql.UntypedSqlRow{
					{
						int64(44),
					},
				},
			},
		},
	},
	{
		Name: "Call procedures by their qualified name",
		SetUpScript: []string{
			"CREATE DATABASE otherdb",
			"CREATE PROCEDURE mydb.p1() SELECT 42",
			"CREATE PROCEDURE otherdb.p1() SELECT 43",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL p1()",
				Expected: []sql.UntypedSqlRow{{42}},
			},
			{
				Query:    "CALL mydb.p1()",
				Expected: []sql.UntypedSqlRow{{42}},
			},
			{
				Query:    "CALL otherdb.p1()",
				Expected: []sql.UntypedSqlRow{{43}},
			},
			{
				Query:    "USE otherdb",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "CALL p1()",
				Expected: []sql.UntypedSqlRow{{43}},
			},
		},
	},
	{
		Name: "String literals with escaped chars",
		SetUpScript: []string{
			`CREATE PROCEDURE joe(IN str VARCHAR(15)) SELECT CONCAT('joe''s bar:', str);`,
			`CREATE PROCEDURE jill(IN str VARCHAR(15)) SELECT CONCAT('jill\'s bar:', str);`,
			`CREATE PROCEDURE stan(IN str VARCHAR(15)) SELECT CONCAT("stan\'s bar:", str);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL joe('open')",
				Expected: []sql.UntypedSqlRow{{"joe's bar:open"}},
			},
			{
				Query:    "CALL jill('closed')",
				Expected: []sql.UntypedSqlRow{{"jill's bar:closed"}},
			},
			{
				Query:    "CALL stan('quarantined')",
				Expected: []sql.UntypedSqlRow{{"stan's bar:quarantined"}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/pull/7947/
		Name: "Call a procedure that needs subqueries resolved in an if condition",
		SetUpScript: []string{
			`CREATE PROCEDURE populate_if_empty()
				BEGIN
					IF (SELECT 0) = 0 THEN
						SELECT 'hi';
					END IF;
				END`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL populate_if_empty();",
				Expected: []sql.UntypedSqlRow{{"hi"}},
			},
		},
	},
}

var ProcedureDropTests = []ScriptTest{
	{
		Name: "DROP procedures",
		SetUpScript: []string{
			"CREATE PROCEDURE p1() SELECT 5",
			"CREATE PROCEDURE p2() SELECT 6",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1",
				Expected: []sql.UntypedSqlRow{
					{
						int64(5),
					},
				},
			},
			{
				Query: "CALL p2",
				Expected: []sql.UntypedSqlRow{
					{
						int64(6),
					},
				},
			},
			{
				Query:    "DROP PROCEDURE p1",
				Expected: []sql.UntypedSqlRow{{types.OkResult{}}},
			},
			{
				Query:       "CALL p1",
				ExpectedErr: sql.ErrStoredProcedureDoesNotExist,
			},
			{
				Query:    "DROP PROCEDURE IF EXISTS p2",
				Expected: []sql.UntypedSqlRow{{types.OkResult{}}},
			},
			{
				Query:       "CALL p2",
				ExpectedErr: sql.ErrStoredProcedureDoesNotExist,
			},
			{
				Query:       "DROP PROCEDURE p3",
				ExpectedErr: sql.ErrStoredProcedureDoesNotExist,
			},
			{
				Query:    "DROP PROCEDURE IF EXISTS p4",
				Expected: []sql.UntypedSqlRow{{types.OkResult{}}},
			},
		},
	},
}

var ProcedureShowStatus = []ScriptTest{
	{
		Name: "SHOW procedures",
		SetUpScript: []string{
			"CREATE PROCEDURE p1() COMMENT 'hi' DETERMINISTIC SELECT 6",
			"CREATE definer=`user` PROCEDURE p2() SQL SECURITY INVOKER SELECT 7",
			"CREATE PROCEDURE p21() SQL SECURITY DEFINER SELECT 8",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW PROCEDURE STATUS",
				Expected: []sql.UntypedSqlRow{
					{
						"mydb",                // Db
						"p1",                  // Name
						"PROCEDURE",           // Type
						"",                    // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"DEFINER",             // Security_type
						"hi",                  // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
					{
						"mydb",                // Db
						"p2",                  // Name
						"PROCEDURE",           // Type
						"user@%",              // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"INVOKER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
					{
						"mydb",                // Db
						"p21",                 // Name
						"PROCEDURE",           // Type
						"",                    // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"DEFINER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
				},
			},
			{
				Query: "SHOW PROCEDURE STATUS LIKE 'p2%'",
				Expected: []sql.UntypedSqlRow{
					{
						"mydb",                // Db
						"p2",                  // Name
						"PROCEDURE",           // Type
						"user@%",              // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"INVOKER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
					{
						"mydb",                // Db
						"p21",                 // Name
						"PROCEDURE",           // Type
						"",                    // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"DEFINER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
				},
			},
			{
				Query:    "SHOW PROCEDURE STATUS LIKE 'p4'",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SHOW PROCEDURE STATUS WHERE Db = 'mydb'",
				Expected: []sql.UntypedSqlRow{
					{
						"mydb",                // Db
						"p1",                  // Name
						"PROCEDURE",           // Type
						"",                    // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"DEFINER",             // Security_type
						"hi",                  // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
					{
						"mydb",                // Db
						"p2",                  // Name
						"PROCEDURE",           // Type
						"user@%",              // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"INVOKER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
					{
						"mydb",                // Db
						"p21",                 // Name
						"PROCEDURE",           // Type
						"",                    // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"DEFINER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
				},
			},
			{
				Query: "SHOW PROCEDURE STATUS WHERE Name LIKE '%1'",
				Expected: []sql.UntypedSqlRow{
					{
						"mydb",                // Db
						"p1",                  // Name
						"PROCEDURE",           // Type
						"",                    // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"DEFINER",             // Security_type
						"hi",                  // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
					{
						"mydb",                // Db
						"p21",                 // Name
						"PROCEDURE",           // Type
						"",                    // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"DEFINER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
				},
			},
			{
				Query: "SHOW PROCEDURE STATUS WHERE Security_type = 'INVOKER'",
				Expected: []sql.UntypedSqlRow{
					{
						"mydb",                // Db
						"p2",                  // Name
						"PROCEDURE",           // Type
						"user@%",              // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"INVOKER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
				},
			},
			{
				Query: "SHOW PROCEDURE STATUS",
				Expected: []sql.UntypedSqlRow{
					{
						"mydb",                // Db
						"p1",                  // Name
						"PROCEDURE",           // Type
						"",                    // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"DEFINER",             // Security_type
						"hi",                  // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
					{
						"mydb",                // Db
						"p2",                  // Name
						"PROCEDURE",           // Type
						"user@%",              // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"INVOKER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
					{
						"mydb",                // Db
						"p21",                 // Name
						"PROCEDURE",           // Type
						"",                    // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"DEFINER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_bin",    // collation_connection
						"utf8mb4_0900_bin",    // Database Collation
					},
				},
			},
		},
	},
}

var ProcedureShowCreate = []ScriptTest{
	{
		Name: "SHOW procedures",
		SetUpScript: []string{
			"CREATE PROCEDURE p1() COMMENT 'hi' DETERMINISTIC SELECT 6",
			"CREATE definer=`user` PROCEDURE p2() SQL SECURITY INVOKER SELECT 7",
			"CREATE PROCEDURE p21() SQL SECURITY DEFINER SELECT 8",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW CREATE PROCEDURE p1",
				Expected: []sql.UntypedSqlRow{
					{
						"p1", // Procedure
						"",   // sql_mode
						"CREATE PROCEDURE p1() COMMENT 'hi' DETERMINISTIC SELECT 6", // Create Procedure
						"utf8mb4",          // character_set_client
						"utf8mb4_0900_bin", // collation_connection
						"utf8mb4_0900_bin", // Database Collation
					},
				},
			},
			{
				Query: "SHOW CREATE PROCEDURE p2",
				Expected: []sql.UntypedSqlRow{
					{
						"p2", // Procedure
						"",   // sql_mode
						"CREATE definer=`user` PROCEDURE p2() SQL SECURITY INVOKER SELECT 7", // Create Procedure
						"utf8mb4",          // character_set_client
						"utf8mb4_0900_bin", // collation_connection
						"utf8mb4_0900_bin", // Database Collation
					},
				},
			},
			{
				Query: "SHOW CREATE PROCEDURE p21",
				Expected: []sql.UntypedSqlRow{
					{
						"p21", // Procedure
						"",    // sql_mode
						"CREATE PROCEDURE p21() SQL SECURITY DEFINER SELECT 8", // Create Procedure
						"utf8mb4",          // character_set_client
						"utf8mb4_0900_bin", // collation_connection
						"utf8mb4_0900_bin", // Database Collation
					},
				},
			},
		},
	},
	{
		Name:        "SHOW non-existent procedures",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "SHOW CREATE PROCEDURE p1",
				ExpectedErr: sql.ErrStoredProcedureDoesNotExist,
			},
		},
	},
}

var ProcedureCreateInSubroutineTests = []ScriptTest{
	//TODO: Match MySQL behavior (https://github.com/dolthub/dolt/issues/8053)
	{
		Name: "procedure must not contain CREATE PROCEDURE",
		Assertions: []ScriptTestAssertion{
			{
				Query: "CREATE PROCEDURE foo() CREATE PROCEDURE bar() SELECT 0;",
				// MySQL output: "Can't create a PROCEDURE from within another stored routine",
				ExpectedErrStr: "creating procedures in stored procedures is currently unsupported and will be added in a future release",
			},
		},
	},
	{
		Name: "event must not contain CREATE PROCEDURE",
		Assertions: []ScriptTestAssertion{
			{
				// Skipped because MySQL errors here but we don't.
				Query:          "CREATE EVENT foo ON SCHEDULE EVERY 1 YEAR DO CREATE PROCEDURE bar() SELECT 1;",
				ExpectedErrStr: "Can't create a PROCEDURE from within another stored routine",
				Skip:           true,
			},
		},
	},
	{
		Name: "trigger must not contain CREATE PROCEDURE",
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Skipped because MySQL errors here but we don't.
				Query:          "CREATE TRIGGER foo AFTER UPDATE ON t FOR EACH ROW BEGIN CREATE PROCEDURE bar() SELECT 1; END",
				ExpectedErrStr: "Can't create a PROCEDURE from within another stored routine",
				Skip:           true,
			},
		},
	},
}

var NoDbProcedureTests = []ScriptTestAssertion{
	{
		Query:    "SHOW databases;",
		Expected: []sql.UntypedSqlRow{{"information_schema"}, {"mydb"}, {"mysql"}},
	},
	{
		Query:    "SELECT database();",
		Expected: []sql.UntypedSqlRow{{nil}},
	},
	{
		Query:    "CREATE PROCEDURE mydb.p5() SELECT 42;",
		Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
	},
	{
		Query:            "SHOW CREATE PROCEDURE mydb.p5;",
		SkipResultsCheck: true,
	},
	{
		Query:       "SHOW CREATE PROCEDURE p5;",
		ExpectedErr: sql.ErrNoDatabaseSelected,
	},
}
