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
)

var VariableQueries = []ScriptTest{
	{
		Name:        "use string name for foreign_key checks",
		SetUpScript: []string{},
		Query:       "select @@GLOBAL.unknown",
		ExpectedErr: sql.ErrUnknownSystemVariable,
	},
	{
		Name:        "use string name for foreign_key checks",
		SetUpScript: []string{},
		Query:       "set @@foreign_key_checks = off;",
		Expected:    []sql.Row{{}},
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
		Name: "@@server_id",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select @@server_id;",
				Expected: []sql.Row{{uint32(0)}},
			},
			{
				Query:    "set @@server_id=123;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "set @@GLOBAL.server_id=123;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "set @@GLOBAL.server_id=0;",
				Expected: []sql.Row{{}},
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
			{"NO_ENGINE_SUBSTITUTION,PIPES_AS_CONCAT,ANSI", "+00:00", "utf8mb3", "utf8mb3", "utf8mb3"},
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
		Name: "set sql_mode variable from mysqldump",
		SetUpScript: []string{
			`SET sql_mode = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,TRADITIONAL,NO_ENGINE_SUBSTITUTION'`,
		},
		Query: "SELECT @@sql_mode",
		Expected: []sql.Row{
			{"ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,STRICT_ALL_TABLES,STRICT_TRANS_TABLES,TRADITIONAL"},
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
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select @@transaction_isolation, @@transaction_read_only",
				Expected: []sql.Row{{"SERIALIZABLE", 1}},
			},
			{
				Query:    "set transaction read write, isolation level read uncommitted",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select @@transaction_isolation, @@transaction_read_only",
				Expected: []sql.Row{{"READ-UNCOMMITTED", 0}},
			},
			{
				Query:    "set transaction isolation level read committed",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select @@transaction_isolation",
				Expected: []sql.Row{{"READ-COMMITTED"}},
			},
			{
				Query:    "set transaction isolation level repeatable read",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select @@transaction_isolation",
				Expected: []sql.Row{{"REPEATABLE-READ"}},
			},
			{
				Query:    "set session transaction isolation level serializable, read only",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "select @@transaction_isolation, @@transaction_read_only",
				Expected: []sql.Row{{"SERIALIZABLE", 1}},
			},
			{
				Query:    "set global transaction read write, isolation level read uncommitted",
				Expected: []sql.Row{{}},
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
