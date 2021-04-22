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

package enginetest

import (
	"time"

	"github.com/dolthub/go-mysql-server/sql/parse"

	"github.com/dolthub/go-mysql-server/sql"
)

var ProcedureLogicTests = []ScriptTest{
	{
		Name: "Simple SELECT",
		SetUpScript: []string{
			"CREATE PROCEDURE testabc(x DOUBLE, y DOUBLE) SELECT x*y",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL testabc(2, 3)",
				Expected: []sql.Row{
					{
						float64(6),
					},
				},
			},
			{
				Query: "CALL testabc(9, 9.5)",
				Expected: []sql.Row{
					{
						float64(85.5),
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
				Expected: []sql.Row{
					{"1"},
					{"2"},
					{"30"},
					{"40"},
					{"500"},
				},
			},
			{
				Query: "SELECT * FROM t1 ORDER BY 1",
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{
						"1 is less than 2.",
					},
				},
			},
			{
				Query: "SELECT @outparam",
				Expected: []sql.Row{
					{
						"1 is less than 2.",
					},
				},
			},
			{
				Query: "CALL p1(@outparam, null, 2)",
				Expected: []sql.Row{
					{
						nil,
					},
				},
			},
			{
				Query: "CALL p1(@outparam, 7, 4)",
				Expected: []sql.Row{
					{
						"7 is greater than 4.",
					},
				},
			},
			{
				Query: "SELECT @outparam",
				Expected: []sql.Row{
					{
						"7 is greater than 4.",
					},
				},
			},
			{
				Query: "CALL p1(@outparam, 5, 5)",
				Expected: []sql.Row{
					{
						"5 equals 5.",
					},
				},
			},
			{
				Query: "SELECT @outparam",
				Expected: []sql.Row{
					{
						"5 equals 5.",
					},
				},
			},
			{
				Query: "CALL p2(@outparam, 9, 3)",
				Expected: []sql.Row{
					{
						"9 is greater than 3.",
					},
				},
			},
			{ // Not affected as p2 has an IN param rather than OUT
				Query: "SELECT @outparam",
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{
						int64(1000),
					},
				},
			},
			{
				Query: "CALL p1(1)",
				Expected: []sql.Row{
					{
						int64(1001),
					},
				},
			},
			{
				Query: "CALL p1(2)",
				Expected: []sql.Row{
					{
						sql.NewOkResult(2),
					},
				},
			},
			{
				Query:       "CALL p1(10)",
				ExpectedErrStr: "duplicate primary key given: [1]",
			},
			{
				Query: "CALL p1(11)",
				Expected: []sql.Row{
					{int64(8)},
					{int64(9)},
					{int64(10)},
					{int64(11)},
					{int64(12)},
				},
			},
			{
				Query: "CALL p1(12)",
				Expected: []sql.Row{
					{
						int64(2003),
					},
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
				Expected: []sql.Row{
					{int64(1), "z", "d"},
					{int64(2), "y", "e"},
					{int64(3), "x", "f"},
				},
			},
			{
				Query: "CALL p1()",
				Expected: []sql.Row{
					{int64(1), "z", "d"},
					{int64(2), "y", "e"},
					{int64(3), "x", "f"},
				},
			},
			{
				Query: "CALL p2()",
				Expected: []sql.Row{
					{int64(1), "z", "d"},
					{int64(2), "y", "e"},
					{int64(3), "x", "f"},
				},
			},
			{
				Query: "CALL p3()",
				Expected: []sql.Row{
					{int64(1), "z", "d"},
					{int64(2), "y", "e"},
					{int64(3), "x", "f"},
				},
			},
			{
				Query: "CALL p4()",
				Expected: []sql.Row{
					{int64(1), "z", "d"},
					{int64(2), "y", "e"},
					{int64(3), "x", "f"},
				},
			},
			{
				Query: "CALL p5()",
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{int64(20)},
					{int64(30)},
				},
			},
			{
				Query: "CALL p1(2)",
				Expected: []sql.Row{
					{int64(200)},
					{int64(300)},
				},
			},
			{
				Query: "CALL p1(5)",
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{int64(1)},
					{int64(2)},
					{int64(3)},
				},
			},
			{
				Query: "CALL p1(1)",
				Expected: []sql.Row{
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
				Query: "CALL add_item('A test item');",
				Expected: []sql.Row{
					{sql.NewOkResult(1)},
				},
			},
			{
				Query: "SELECT * FROM items;",
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{"4", "6"},
					{"3", "4"},
				},
			},
			{
				Query: "CALL p2(5, 6)",
				Expected: []sql.Row{
					{"6", "8"},
					{"5", "6"},
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
				Query: "CALL add_item(6);",
				Expected: []sql.Row{
					{sql.NewOkResult(3)},
				},
			},
			{
				Query: "SELECT * FROM items ORDER BY 1;",
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{int64(1)},
					{int64(2)},
				},
			},
			{
				Query: "CALL p1(5)",
				Expected: []sql.Row{
					{int64(6)},
					{int64(7)},
				},
			},
		},
	},
	{
		Name: "Subquery on SET user variable captures parameter",
		SetUpScript: []string{
			`CREATE PROCEDURE p1(x VARCHAR(20))
BEGIN
	SET @randomvar = (SELECT LENGTH(x));
	SELECT @randomvar;
END;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1('hi')",
				Expected: []sql.Row{
					{int64(2)},
				},
			},
			{
				Query: "CALL p1('hello')",
				Expected: []sql.Row{
					{int64(5)},
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
		Name:        "Duplicate parameter names",
		Query:       "CREATE PROCEDURE p1(abc DATETIME, abc DOUBLE) SELECT abc",
		ExpectedErr: sql.ErrProcedureDuplicateParameterName,
	},
	{
		Name:        "Duplicate parameter names mixed casing",
		Query:       "CREATE PROCEDURE p1(abc DATETIME, ABC DOUBLE) SELECT abc",
		ExpectedErr: sql.ErrProcedureDuplicateParameterName,
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
				ExpectedErr: sql.ErrDeclareOrderInvalid,
			},
			{
				Query: `CREATE PROCEDURE p1(x INT)
BEGIN
	BEGIN
		SELECT x;
		DECLARE cond_name CONDITION FOR SQLSTATE '45000';
	END;
END;`,
				ExpectedErr: sql.ErrDeclareOrderInvalid,
			},
			{
				Query: `CREATE PROCEDURE p1(x INT)
BEGIN
	IF x = 0 THEN
		DECLARE cond_name CONDITION FOR SQLSTATE '45000';
	END IF;
END;`,
				ExpectedErr: sql.ErrDeclareOrderInvalid,
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
				ExpectedErr: sql.ErrDeclareOrderInvalid,
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
		ExpectedErr: parse.ErrUnsupportedSyntax,
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				ExpectedErr: sql.ErrConvertingToTime,
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
				Expected: []sql.Row{
					{
						int64(5),
					},
				},
			},
			{
				Query: "CALL p2",
				Expected: []sql.Row{
					{
						int64(6),
					},
				},
			},
			{
				Query:    "DROP PROCEDURE p1",
				Expected: []sql.Row{},
			},
			{
				Query:       "CALL p1",
				ExpectedErr: sql.ErrStoredProcedureDoesNotExist,
			},
			{
				Query:    "DROP PROCEDURE IF EXISTS p2",
				Expected: []sql.Row{},
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
				Expected: []sql.Row{},
			},
		},
	},
}

var ProcedureShowStatus = []ScriptTest{
	{
		Name: "SHOW procedures",
		SetUpScript: []string{
			"CREATE PROCEDURE p1() COMMENT 'hi' DETERMINISTIC SELECT 6",
			"CREATE definer=user PROCEDURE p2() SQL SECURITY INVOKER SELECT 7",
			"CREATE PROCEDURE p21() SQL SECURITY DEFINER SELECT 8",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW PROCEDURE STATUS",
				Expected: []sql.Row{
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
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
					},
					{
						"mydb",                // Db
						"p2",                  // Name
						"PROCEDURE",           // Type
						"user",                // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"INVOKER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
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
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
					},
				},
			},
			{
				Query: "SHOW PROCEDURE STATUS LIKE 'p2%'",
				Expected: []sql.Row{
					{
						"mydb",                // Db
						"p2",                  // Name
						"PROCEDURE",           // Type
						"user",                // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"INVOKER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
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
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
					},
				},
			},
			{
				Query:    "SHOW PROCEDURE STATUS LIKE 'p4'",
				Expected: []sql.Row{},
			},
			{
				Query: "SHOW PROCEDURE STATUS WHERE Db = 'mydb'",
				Expected: []sql.Row{
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
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
					},
					{
						"mydb",                // Db
						"p2",                  // Name
						"PROCEDURE",           // Type
						"user",                // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"INVOKER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
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
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
					},
				},
			},
			{
				Query: "SHOW PROCEDURE STATUS WHERE Name LIKE '%1'",
				Expected: []sql.Row{
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
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
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
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
					},
				},
			},
			{
				Query: "SHOW PROCEDURE STATUS WHERE Security_type = 'INVOKER'",
				Expected: []sql.Row{
					{
						"mydb",                // Db
						"p2",                  // Name
						"PROCEDURE",           // Type
						"user",                // Definer
						time.Unix(0, 0).UTC(), // Modified
						time.Unix(0, 0).UTC(), // Created
						"INVOKER",             // Security_type
						"",                    // Comment
						"utf8mb4",             // character_set_client
						"utf8mb4_0900_ai_ci",  // collation_connection
						"utf8mb4_0900_ai_ci",  // Database Collation
					},
				},
			},
		},
	},
}
