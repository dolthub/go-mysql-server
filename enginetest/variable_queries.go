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
			"set @@autocommit = ON, sql_mode = OFF",
		},
		Query: "SELECT @@autocommit, @@session.sql_mode",
		Expected: []sql.Row{
			{1, 0},
		},
	},
	{
		Name: "set system variable true / false quoted",
		SetUpScript: []string{
			`set @@autocommit = "true", sql_mode = "false"`,
		},
		Query: "SELECT @@autocommit, @@session.sql_mode",
		Expected: []sql.Row{
			{1, 0},
		},
	},
	{
		Name: "set system variable true / false",
		SetUpScript: []string{
			`set @@autocommit = true, sql_mode = false`,
		},
		Query: "SELECT @@autocommit, @@session.sql_mode",
		Expected: []sql.Row{
			{1, 0},
		},
	},
	{
		Name: "set system variable with expressions",
		SetUpScript: []string{
			`set sql_mode = "123", @@auto_increment_increment = 1`,
			`set sql_mode = concat(@@sql_mode, "456"), @@auto_increment_increment = @@auto_increment_increment + 3`,
		},
		Query: "SELECT @@sql_mode, @@auto_increment_increment",
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
			`set @@sql_mode = some_mode`,
		},
		Query: "SELECT @@sql_mode",
		Expected: []sql.Row{
			{"some_mode"},
		},
	},
	{
		Name: "set system variable to bareword, unqualified",
		SetUpScript: []string{
			`set sql_mode = some_mode`,
		},
		Query: "SELECT @@sql_mode",
		Expected: []sql.Row{
			{"some_mode"},
		},
	},
	// TODO: for compatibility, we allow unknown system variables to be set as well. For full MySQL emulation, we need to
	//  list every system variable MySQL supports and reject all others.
	{
		Name: "set unknown system variable",
		SetUpScript: []string{
			`set dne = "hello"`,
		},
		Query: "SELECT @@dne",
		Expected: []sql.Row{
			{"hello"},
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
}

var VariableErrorTests = []QueryErrorTest{
	// TODO: for compatibility, we allow unknown system variables to be set as well. For full MySQL emulation, we need to
	//  list every system variable MySQL supports and reject all others.
	// {
	// 	Query:       "set @@does_not_exist = 100",
	// 	ExpectedErr: sql.ErrUnknownSystemVariable,
	// },
	{
		Query:       "set @myvar = bareword",
		ExpectedErr: sql.ErrColumnNotFound,
	},
}
