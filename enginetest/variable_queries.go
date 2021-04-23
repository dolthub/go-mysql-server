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
	"math"

	"github.com/dolthub/go-mysql-server/sql"
)

var VariableQueries = []ScriptTest{
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
			`set lc_messages = "123", @@auto_increment_increment = 1`,
			`set lc_messages = concat(@@lc_messages, "456"), @@auto_increment_increment = @@auto_increment_increment + 3`,
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
			`set NAMES "charset"`,
		},
		Query: "SELECT @@character_set_client, @@character_set_connection, @@character_set_results",
		Expected: []sql.Row{
			{"charset", "charset", "charset"},
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
			{123.4},
		},
	},
	{
		Name: "set user var and sys var in same statement",
		SetUpScript: []string{
			`set @myvar = 123.4, @@auto_increment_increment = 1234`,
		},
		Query: "SELECT @myvar, @@auto_increment_increment",
		Expected: []sql.Row{
			{123.4, 1234},
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
