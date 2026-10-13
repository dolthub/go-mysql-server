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
	"math"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var VariableQueries = []ScriptTest{
	{
		Name:        "use string name for foreign_key checks",
		SetUpScript: []string{},
		Query:       "set @@foreign_key_checks = off;",
		Expected:    []sql.Row{{types.NewOkResult(0)}},
	},
	{
		Name: "set system variables",
		SetUpScript: []string{
			"set @@auto_increment_increment = 100, sql_select_limit = 1",
		},
		Query: "SELECT @@auto_increment_increment, @@sql_select_limit",
		Expected: []sql.Row{
			{100, 1},
		},
	},
	{
		Name:  "select join_complexity_limit",
		Query: "SELECT @@join_complexity_limit",
		Expected: []sql.Row{
			{uint64(12)},
		},
	},
	{
		Name: "set join_complexity_limit",
		SetUpScript: []string{
			"set @@join_complexity_limit = 2",
		},
		Query: "SELECT @@join_complexity_limit",
		Expected: []sql.Row{
			{uint64(2)},
		},
	},
	{
		Name: "variable scope is included in returned column name when explicitly provided",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select @@max_allowed_packet;",
				Expected: []sql.Row{{1073741824}},
				ExpectedColumns: sql.Schema{
					{
						Name: "@@max_allowed_packet",
						Type: types.Uint64,
					},
				},
			},
			{
				Query:    "select @@session.max_allowed_packet;",
				Expected: []sql.Row{{1073741824}},
				ExpectedColumns: sql.Schema{
					{
						Name: "@@session.max_allowed_packet",
						Type: types.Uint64,
					},
				},
			},
			{
				Query:    "select @@global.max_allowed_packet;",
				Expected: []sql.Row{{1073741824}},
				ExpectedColumns: sql.Schema{
					{
						Name: "@@global.max_allowed_packet",
						Type: types.Uint64,
					},
				},
			},
			{
				Query:    "select @@GLoBAL.max_allowed_packet;",
				Expected: []sql.Row{{1073741824}},
				ExpectedColumns: sql.Schema{
					{
						Name: "@@GLoBAL.max_allowed_packet",
						Type: types.Uint64,
					},
				},
			},
		},
	},
	{
		Name: "@@server_id",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select @@server_id;",
				Expected: []sql.Row{{uint32(1)}},
			},
			{
				Query:    "set @@server_id=123;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "set @@GLOBAL.server_id=123;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "set @@GLOBAL.server_id=0;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "set system variables and user variables",
		SetUpScript: []string{
			"SET @myvar = @@autocommit",
			"SET autocommit = @myvar",
			"SET @myvar2 = @myvar - 1, @myvar3 = @@autocommit - 1",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select @myvar, @@autocommit, @myvar2, @myvar3",
				Expected: []sql.Row{
					{1, 1, 0, 0},
				},
			},
		},
	},
	{
		Name: "set system variables mixed case",
		SetUpScript: []string{
			"set @@auto_increment_INCREMENT = 100, sql_select_LIMIT = 1",
		},
		Query: "SELECT @@auto_increment_increment, @@sql_select_limit",
		Expected: []sql.Row{
			{100, 1},
		},
	},
	{
		Name: "set system variable defaults",
		SetUpScript: []string{
			"set @@auto_increment_increment = 100, sql_select_limit = 1",
			"set @@auto_increment_increment = default, sql_select_limit = default",
		},
		Query: "SELECT @@auto_increment_increment, @@sql_select_limit",
		Expected: []sql.Row{
			{1, math.MaxInt32},
		},
	},
	{
		Name: "set system variable ON / OFF",
		SetUpScript: []string{
			"set @@autocommit = ON, sql_mode = \"\"",
		},
		Query: "SELECT @@autocommit, @@session.sql_mode",
		Expected: []sql.Row{
			{1, ""},
		},
	},
	{
		Name: "set system variable ON / OFF",
		SetUpScript: []string{
			"set @@autocommit = ON, session sql_mode = \"\"",
		},
		Query: "SELECT @@autocommit, @@session.sql_mode",
		Expected: []sql.Row{
			{1, ""},
		},
	},
	{
		Name: "set system variable sql_mode to ANSI for session",
		SetUpScript: []string{
			"set SESSION sql_mode = 'ANSI'",
		},
		Query: "SELECT @@session.sql_mode",
		Expected: []sql.Row{
			{"ANSI"},
		},
	},
	{
		Name: "set system variable true / false quoted",
		SetUpScript: []string{
			`set @@autocommit = "true", default_table_encryption = "false"`,
		},
		Query: "SELECT @@autocommit, @@session.default_table_encryption",
		Expected: []sql.Row{
			{1, 0},
		},
	},
	{
		Name: "set system variable true / false",
		SetUpScript: []string{
			`set @@autocommit = true, default_table_encryption = false`,
		},
		Query: "SELECT @@autocommit, @@session.default_table_encryption",
		Expected: []sql.Row{
			{1, 0},
		},
	},
	{
		Name: "set system variable with expressions",
		SetUpScript: []string{
			`set lc_messages = '123', @@auto_increment_increment = 1`,
			`set lc_messages = concat(@@lc_messages, '456'), @@auto_increment_increment = @@auto_increment_increment + 3`,
		},
		Query: "SELECT @@lc_messages, @@auto_increment_increment",
		Expected: []sql.Row{
			{"123456", 4},
		},
	},
	{
		Name: "set system variable to another system variable",
		SetUpScript: []string{
			`set @@auto_increment_increment = 123`,
			`set @@sql_select_limit = @@auto_increment_increment`,
		},
		Query: "SELECT @@sql_select_limit",
		Expected: []sql.Row{
			{123},
		},
	},
	{
		Name: "set names",
		SetUpScript: []string{
			`set names utf8mb4`,
		},
		Query: "SELECT @@character_set_client, @@character_set_connection, @@character_set_results",
		Expected: []sql.Row{
			{"utf8mb4", "utf8mb4", "utf8mb4"},
		},
	},
	// TODO: we should validate the character set here
	{
		Name: "set names quoted",
		SetUpScript: []string{
			`set NAMES "utf8mb3"`,
		},
		Query: "SELECT @@character_set_client, @@character_set_connection, @@character_set_results",
		Expected: []sql.Row{
			{"utf8mb3", "utf8mb3", "utf8mb3"},
		},
	},
	{
		Name: "set character set",
		SetUpScript: []string{
			`set character set utf8`,
		},
		Query: "SELECT @@character_set_client, @@character_set_connection, @@character_set_results",
		Expected: []sql.Row{
			{"utf8", "utf8mb4", "utf8"},
		},
	},
	{
		Name: "set charset",
		SetUpScript: []string{
			`set charset utf8`,
		},
		Query: "SELECT @@character_set_client, @@character_set_connection, @@character_set_results",
		Expected: []sql.Row{
			{"utf8", "utf8mb4", "utf8"},
		},
	},
	{
		Name: "set charset quoted",
		SetUpScript: []string{
			`set charset 'utf8'`,
		},
		Query: "SELECT @@character_set_client, @@character_set_connection, @@character_set_results",
		Expected: []sql.Row{
			{"utf8", "utf8mb4", "utf8"},
		},
	},
	{
		Name: "set multiple variables including 'names'",
		SetUpScript: []string{
			"set SESSION sql_mode = 'ANSI'",
			`SET sql_mode=(SELECT CONCAT(@@sql_mode, ',PIPES_AS_CONCAT,NO_ENGINE_SUBSTITUTION')), time_zone='+00:00', NAMES utf8mb3 COLLATE utf8mb3_bin;`,
		},
		Query: "SELECT @@sql_mode, @@time_zone, @@character_set_client, @@character_set_connection, @@character_set_results",
		Expected: []sql.Row{
			{"PIPES_AS_CONCAT,ANSI,NO_ENGINE_SUBSTITUTION", "+00:00", "utf8mb3", "utf8mb3", "utf8mb3"},
		},
	},
	{
		Name: "set multiple variables including 'charset'",
		SetUpScript: []string{
			`SET sql_mode=ALLOW_INVALID_DATES, time_zone='+00:00', CHARSET 'utf8'`,
		},
		Query: "SELECT @@sql_mode, @@time_zone, @@character_set_client, @@character_set_connection, @@character_set_results",
		Expected: []sql.Row{
			{"ALLOW_INVALID_DATES", "+00:00", "utf8", "utf8mb4", "utf8"},
		},
	},
	{
		Name: "set system variable to bareword",
		SetUpScript: []string{
			`set @@sql_mode = ALLOW_INVALID_DATES`,
		},
		Query: "SELECT @@sql_mode",
		Expected: []sql.Row{
			{"ALLOW_INVALID_DATES"},
		},
	},
	{
		Name: "set system variable to bareword, unqualified",
		SetUpScript: []string{
			`set sql_mode = ALLOW_INVALID_DATES`,
		},
		Query: "SELECT @@sql_mode",
		Expected: []sql.Row{
			{"ALLOW_INVALID_DATES"},
		},
	},
	{
		Name: "set system variable to no_auto_create_user, which has been deprecated",
		SetUpScript: []string{
			`set sql_mode = NO_AUTO_CREATE_USER`,
		},
		Query: "SELECT @@sql_mode",
		Expected: []sql.Row{
			{"NO_AUTO_CREATE_USER"},
		},
	},
	{
		Name: "set sql_mode variable from mysqldump",
		SetUpScript: []string{
			`SET sql_mode = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,TRADITIONAL,NO_ENGINE_SUBSTITUTION'`,
		},
		Query: "SELECT @@sql_mode",
		Expected: []sql.Row{
			{"STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,TRADITIONAL,NO_ENGINE_SUBSTITUTION"},
		},
	},
	{
		Name: "set sql_mode variable ignores empty strings",
		SetUpScript: []string{
			`SET sql_mode = ',,,,STRICT_TRANS_TABLES,,,,,NO_AUTO_VALUE_ON_ZERO,,,,NO_ENGINE_SUBSTITUTION,,,,,,'`,
		},
		Query: "SELECT @@sql_mode",
		Expected: []sql.Row{
			{"NO_AUTO_VALUE_ON_ZERO,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"},
		},
	},
	{
		Name: "show variables renders enums after set",
		SetUpScript: []string{
			`set @@sql_mode='ONLY_FULL_GROUP_BY';`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SHOW VARIABLES LIKE '%sql_mode%'`,
				Expected: []sql.Row{
					{"sql_mode", "ONLY_FULL_GROUP_BY"},
				},
			},
		},
	},
	{
		Name:        "innodb autoinc lock mode",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: `select @@innodb_autoinc_lock_mode;`,
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: `select @@global.innodb_autoinc_lock_mode;`,
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query:       `select @@session.innodb_autoinc_lock_mode;`,
				ExpectedErr: sql.ErrSystemVariableGlobalOnly,
			},
			{
				Query:       `set @@innodb_autoinc_lock_mode = 1;`,
				ExpectedErr: sql.ErrSystemVariableReadOnly,
			},
		},
	},
	// User variables
	{
		Name: "set user var",
		SetUpScript: []string{
			`set @myvar = "hello"`,
		},
		Query: "SELECT @myvar",
		Expected: []sql.Row{
			{"hello"},
		},
	},
	{
		Name: "set user var, integer type",
		SetUpScript: []string{
			`set @myvar = 123`,
		},
		Query: "SELECT @myvar",
		Expected: []sql.Row{
			{123},
		},
	},
	{
		Name: "set user var, floating point",
		SetUpScript: []string{
			`set @myvar = 123.4`,
		},
		Query: "SELECT @myvar",
		Expected: []sql.Row{
			{"123.4"},
		},
	},
	{
		Name: "set user var and sys var in same statement",
		SetUpScript: []string{
			`set @myvar = 123.4, @@auto_increment_increment = 1234`,
		},
		Query: "SELECT @myvar, @@auto_increment_increment",
		Expected: []sql.Row{
			{"123.4", 1234},
		},
	},
	{
		Name: "set sys var to user var",
		SetUpScript: []string{
			`set @myvar = 1234`,
			`set auto_increment_increment = @myvar`,
		},
		Query: "SELECT @myvar, @@auto_increment_increment",
		Expected: []sql.Row{
			{1234, 1234},
		},
	},
	{
		Name: "local is session",
		SetUpScript: []string{
			`set @@LOCAL.cte_max_recursion_depth = 1234`,
		},
		Query: "SELECT @@SESSION.cte_max_recursion_depth",
		Expected: []sql.Row{
			{1234},
		},
	},
	{
		Name: "user and system var with same name",
		SetUpScript: []string{
			`set @cte_max_recursion_depth = 55`,
			`set cte_max_recursion_depth = 77`,
		},
		Query: "SELECT @cte_max_recursion_depth, @@cte_max_recursion_depth",
		Expected: []sql.Row{
			{55, 77},
		},
	},
	{
		Name: "uninitialized user vars",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT @doesNotExist;",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:    "SELECT @doesNotExist is NULL;",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "SELECT @doesNotExist='';",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:    "SELECT @doesNotExist < 123;",
				Expected: []sql.Row{{nil}},
			},
		},
	},

	{
		Name: "eval string user var",
		SetUpScript: []string{
			"set @stringVar = 'abc'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT @stringVar='abc'",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "SELECT @stringVar='abcd';",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "SELECT @stringVar=123;",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "SELECT @stringVar is null;",
				Expected: []sql.Row{{false}},
			},
		},
	},
	{
		Name: "set transaction",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "set transaction isolation level serializable, read only",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select @@transaction_isolation, @@transaction_read_only",
				Expected: []sql.Row{{"SERIALIZABLE", 1}},
			},
			{
				Query:    "set transaction read write, isolation level read uncommitted",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select @@transaction_isolation, @@transaction_read_only",
				Expected: []sql.Row{{"READ-UNCOMMITTED", 0}},
			},
			{
				Query:    "set transaction isolation level read committed",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select @@transaction_isolation",
				Expected: []sql.Row{{"READ-COMMITTED"}},
			},
			{
				Query:    "set transaction isolation level repeatable read",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select @@transaction_isolation",
				Expected: []sql.Row{{"REPEATABLE-READ"}},
			},
			{
				Query:    "set session transaction isolation level serializable, read only",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select @@transaction_isolation, @@transaction_read_only",
				Expected: []sql.Row{{"SERIALIZABLE", 1}},
			},
			{
				Query:    "set global transaction read write, isolation level read uncommitted",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select @@transaction_isolation, @@transaction_read_only",
				Expected: []sql.Row{{"SERIALIZABLE", 1}},
			},
			{
				Query:    "select @@global.transaction_isolation, @@global.transaction_read_only",
				Expected: []sql.Row{{"READ-UNCOMMITTED", 0}},
			},
		},
	},
	{
		Name: "locked warnings stay after query",
		SetUpScript: []string{
			"set @@lock_warnings = 1",
			"select 1/0,1/0",
			"select 1/1",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show warnings",
				Expected: []sql.Row{
					{"Warning", 1365, "Division by 0"},
					{"Warning", 1365, "Division by 0"}},
			},
			{
				Query:            "select 1/0",
				SkipResultsCheck: true,
			},
			{
				Query: "show warnings",
				Expected: []sql.Row{
					{"Warning", 1365, "Division by 0"},
					{"Warning", 1365, "Division by 0"},
					{"Warning", 1365, "Division by 0"},
				},
			},
		},
	},
	{
		Name: "unlocked warnings clear after query",
		SetUpScript: []string{
			"set @@lock_warnings = 0",
			"select 1/0,1/0",
			"select 1/1",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "show warnings",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "warnings persist after locking between queries",
		SetUpScript: []string{
			"select 1/0",
			"set @@lock_warnings = 1",
			"select 1/1",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show warnings",
				Expected: []sql.Row{
					{"Warning", 1365, "Division by 0"},
				},
			},
		},
	},
	//TODO: do not override tables with user-var-like names...but why would you do this??
	//{
	//	Name: "user var table name no conflict",
	//	SetUpScript: []string{
	//		"create table test (pk bigint primary key, `@v1` bigint)",
	//		`insert into test values (1, 123)`,
	//		`set @v1 = 1234`,
	//	},
	//	Query: "SELECT @v1, `@v1` from test",
	//	Expected: []sql.Row{
	//		{1234, 123},
	//	},
	//},
}

var VariableErrorTests = []QueryErrorTest{
	{
		Query:       "select @@GLOBAL.unknown",
		ExpectedErr: sql.ErrUnknownSystemVariable,
	},
	{
		Query:       "set @@does_not_exist = 100",
		ExpectedErr: sql.ErrUnknownSystemVariable,
	},
	{
		Query:       "set @myvar = bareword",
		ExpectedErr: sql.ErrColumnNotFound,
	},
	{
		Query:       "set @@sql_mode = true",
		ExpectedErr: sql.ErrInvalidSystemVariableValue,
	},
	{
		Query:       `set @@sql_mode = "NOT_AN_OPTION"`,
		ExpectedErr: sql.ErrInvalidSetValue,
	},
	{
		Query:       `set global core_file = true`,
		ExpectedErr: sql.ErrSystemVariableReadOnly,
	},
	{
		Query:       `set global require_row_format = on`,
		ExpectedErr: sql.ErrSystemVariableSessionOnly,
	},
	{
		Query:       `set session default_password_lifetime = 5`,
		ExpectedErr: sql.ErrSystemVariableGlobalOnly,
	},
	{
		Query:       `set @custom_var = default`,
		ExpectedErr: sql.ErrUserVariableNoDefault,
	},
	{
		Query:       `set session @@bulk_insert_buffer_size = 5`,
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       `set global @@bulk_insert_buffer_size = 5`,
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       `set session @@session.bulk_insert_buffer_size = 5`,
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       `set session @@global.bulk_insert_buffer_size = 5`,
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       `set global @@session.bulk_insert_buffer_size = 5`,
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       `set global @@global.bulk_insert_buffer_size = 5`,
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       `set session @myvar = 5`,
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       `set global @myvar = 5`,
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       `set @@session.@@bulk_insert_buffer_size = 5`,
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       `set @@global.@@bulk_insert_buffer_size = 5`,
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       `set @@session.@bulk_insert_buffer_size = 5`,
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       `set @@global.@bulk_insert_buffer_size = 5`,
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       `set @@session.@myvar = 5`,
		ExpectedErr: sql.ErrSyntaxError,
	},
	{
		Query:       `set @@global.@myvar = 5`,
		ExpectedErr: sql.ErrSyntaxError,
	},
}

// VariablesScriptTests contains self-contained variables script tests.
var VariablesScriptTests = []ScriptTest{
	{
		Name:    "Test cases on select into statement",
		Dialect: "mysql",
		SetUpScript: []string{
			"SELECT * FROM (VALUES ROW(22,44,88)) AS t INTO @x,@y,@z",
			"CREATE TABLE tab1 (id int primary key, v1 int)",
			"INSERT INTO tab1 VALUES (1, 1), (2, 3), (3, 6)",
			"SELECT id FROM tab1 ORDER BY id DESC LIMIT 1 INTO @myVar",
			"CREATE TABLE tab2 (i2 int primary key, s text)",
			"INSERT INTO tab2 VALUES (1, 'b'), (2, 'm'), (3, 'g')",
			"SELECT m.id, t.s FROM tab1 m JOIN tab2 t on m.id = t.i2 ORDER BY t.s DESC LIMIT 1 INTO @myId, @myText",
			// TODO: union statement does not handle order by and limit clauses
			// "SELECT id FROM tab1 UNION select s FROM tab2 LIMIT 1 INTO @myUnion",
			"SELECT id FROM tab1 WHERE id > 3 UNION select s FROM tab2 WHERE s < 'f' INTO @mustSingleVar",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:                         `SELECT 1 INTO @abc`,
				SkipResultCheckOnServerEngine: true,
				Expected:                      []sql.Row{{types.NewOkResult(1)}},
				ExpectedColumns:               nil,
			},
			{
				Query:    `SELECT @abc`,
				Expected: []sql.Row{{int8(1)}},
			},
			{
				Query:    `SELECT @z, @x, @y`,
				Expected: []sql.Row{{88, 22, 44}},
			},
			{
				Query:    `SELECT @myVar, @mustSingleVar`,
				Expected: []sql.Row{{3, "b"}},
			},
			{
				Query:    `SELECT @myId, @myText, @myUnion`,
				Expected: []sql.Row{{2, "m", nil}},
			},
			{
				Query:       `SELECT id FROM tab1 ORDER BY id DESC INTO @myvar`,
				ExpectedErr: sql.ErrMoreThanOneRow,
			},
			{
				Query:       `SELECT id INTO DUMPFILE 'baddump.out' FROM tab1 ORDER BY id DESC LIMIT 15`,
				ExpectedErr: sql.ErrMoreThanOneRow,
			},
			{
				Query:       `select 1, 2, 3 into @my1, @my2`,
				ExpectedErr: sql.ErrColumnNumberDoesNotMatch,
			},
			{
				Query:          `SELECT id, v1 INTO @myFirstVar FROM tab1 ORDER BY id DESC LIMIT 1 INTO @mySecondVar`,
				ExpectedErrStr: "Multiple INTO clauses in one query block at position 84 near '@mySecondVar'",
			},
			{
				Query:          `SELECT id FROM tab1 WHERE id > 3 UNION select s INTO @mustSingleVar FROM tab2 WHERE s < 'f' ORDER BY s DESC`,
				ExpectedErrStr: "INTO clause is not allowed at position 98 near 'ORDER'",
			},
		},
	},
	{
		Name: "validate_password_strength and validate_password.length",
		SetUpScript: []string{
			"set @orig = @@global.validate_password.length",
			"set @@global.validate_password.length = 0",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select validate_password_strength('')",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select validate_password_strength('123')",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select validate_password_strength('1234')",
				Expected: []sql.Row{
					{50},
				},
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.length = 1000",
			},
			{
				Query: "select validate_password_strength('ABCabc123!@!#')",
				Expected: []sql.Row{
					{25},
				},
			},
			{
				Query:          "set @@session.validate_password.length = 123",
				ExpectedErrStr: "Variable 'validate_password.length' is a GLOBAL variable and should be set with SET GLOBAL",
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.length = @orig",
			},
		},
	},
	{
		Name: "validate_password_strength and validate_password.number_count",
		SetUpScript: []string{
			"set @orig = @@global.validate_password.number_count",
			"set @@global.validate_password.number_count = 0",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select validate_password_strength('ABCabc!@#')",
				Expected: []sql.Row{
					{100},
				},
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.number_count = 1000",
			},
			{
				Query: "select validate_password_strength('ABCabc!!!!123456789012345678901234567890')",
				Expected: []sql.Row{
					{50},
				},
			},
			{
				Query:          "set @@session.validate_password.number_count = 123",
				ExpectedErrStr: "Variable 'validate_password.number_count' is a GLOBAL variable and should be set with SET GLOBAL",
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.number_count = @orig",
			},
		},
	},
	{
		Name: "validate_password_strength and validate_password.mixed_case_count",
		SetUpScript: []string{
			"set @orig = @@global.validate_password.mixed_case_count",
			"set @@global.validate_password.mixed_case_count = 0",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select validate_password_strength('abcabc!@#123')",
				Expected: []sql.Row{
					{100},
				},
			},
			{
				Query: "select validate_password_strength('ABCABC!@#123')",
				Expected: []sql.Row{
					{100},
				},
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.mixed_case_count = 1000",
			},
			{
				Query: "select validate_password_strength('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456!?!?!?')",
				Expected: []sql.Row{
					{50},
				},
			},
			{
				Query:          "set @@session.validate_password.mixed_case_count = 123",
				ExpectedErrStr: "Variable 'validate_password.mixed_case_count' is a GLOBAL variable and should be set with SET GLOBAL",
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.mixed_case_count = @orig",
			},
		},
	},
	{
		Name: "validate_password_strength and validate_password.special_char_count",
		SetUpScript: []string{
			"set @orig = @@global.validate_password.special_char_count",
			"set @@global.validate_password.special_char_count = 0",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select validate_password_strength('abcABC123')",
				Expected: []sql.Row{
					{100},
				},
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.special_char_count = 1000",
			},
			{
				Query: "select validate_password_strength('abcABC123!@#$%^&*()                            ')",
				Expected: []sql.Row{
					{50},
				},
			},
			{
				Query:          "set @@session.validate_password.special_char_count = 123",
				ExpectedErrStr: "Variable 'validate_password.special_char_count' is a GLOBAL variable and should be set with SET GLOBAL",
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.special_char_count = @orig",
			},
		},
	},
	{
		Name:        "MySQL default and strict SQL_MODE behavior",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				// Disabling `NO_ZERO_IN_DATE` throws additional warning
				Query: "set @@sql_mode = '" +
					"STRICT_TRANS_TABLES," +
					"NO_ZERO_DATE," +
					"ERROR_FOR_DIVISION_BY_ZERO'",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
				ExpectedWarningsCount:           2,
				ExpectedWarning:                 3135,
				ExpectedWarningMessageSubstring: "Removing NO_ZERO_IN_DATE mode is not supported",
			},
			{
				Query: "select @@sql_mode",
				Expected: []sql.Row{
					{"STRICT_TRANS_TABLES,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO"},
				},
			},
			{
				// Disabling `STRICT_TRANS_TABLES` throws strict mode warning
				Query: "set @@sql_mode = '" +
					"NO_ZERO_IN_DATE," +
					"NO_ZERO_DATE," +
					"ERROR_FOR_DIVISION_BY_ZERO'",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       3135,
				ExpectedWarningMessageSubstring: "'NO_ZERO_DATE', 'NO_ZERO_IN_DATE' and 'ERROR_FOR_DIVISION_BY_ZERO' " +
					"sql modes should be used with strict mode. " +
					"They will be merged with strict mode in a future release",
			},
			{
				Query: "select @@sql_mode",
				Expected: []sql.Row{
					{"NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO"},
				},
			},
			{
				// Disabling `NO_ZERO_DATE` throws strict mode warning
				Query: "set @@sql_mode = '" +
					"STRICT_TRANS_TABLES," +
					"NO_ZERO_IN_DATE," +
					"ERROR_FOR_DIVISION_BY_ZERO'",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       3135,
				ExpectedWarningMessageSubstring: "'NO_ZERO_DATE', 'NO_ZERO_IN_DATE' and 'ERROR_FOR_DIVISION_BY_ZERO' " +
					"sql modes should be used with strict mode. " +
					"They will be merged with strict mode in a future release",
			},
			{
				Query: "select @@sql_mode",
				Expected: []sql.Row{
					{"STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO"},
				},
			},
			{
				// Disabling `ERROR_FOR_DIVISION_BY_ZERO` throws strict mode warning
				Query: "set @@sql_mode = '" +
					"STRICT_TRANS_TABLES," +
					"NO_ZERO_IN_DATE," +
					"NO_ZERO_DATE'",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       3135,
				ExpectedWarningMessageSubstring: "'NO_ZERO_DATE', 'NO_ZERO_IN_DATE' and 'ERROR_FOR_DIVISION_BY_ZERO' " +
					"sql modes should be used with strict mode. " +
					"They will be merged with strict mode in a future release",
			},
			{
				Query: "select @@sql_mode",
				Expected: []sql.Row{
					{"STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE"},
				},
			},
		},
	},
}
