// Copyright 2022 Dolthub, Inc.
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

var ExternalProcedureTests = []ScriptTest{
	{
		Name: "Call external stored procedure that does not exist",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CALL procedure_does_not_exist('foo');",
				ExpectedErr: sql.ErrStoredProcedureDoesNotExist,
			},
		},
	},
	{
		Name: "INOUT on first param, IN on second param",
		SetUpScript: []string{
			"SET @outparam = 5;",
			"CALL memory_inout_add(@outparam, 11);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT @outparam;",
				Expected: []sql.Row{{16}},
			},
		},
	},
	{
		Name: "Handle setting uninitialized user variables",
		SetUpScript: []string{
			"CALL memory_inout_set_unitialized(@uservar12, @uservar13, @uservar14, @uservar15);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT @uservar12;",
				Expected: []sql.Row{{5}},
			},
			{
				Query:    "SELECT @uservar13;",
				Expected: []sql.Row{{uint(5)}},
			},
			{
				Query:    "SELECT @uservar14;",
				Expected: []sql.Row{{"5"}},
			},
			{
				Query:    "SELECT @uservar15;",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "Called from standard stored procedure",
		SetUpScript: []string{
			"CREATE PROCEDURE p1(x BIGINT) BEGIN CALL memory_inout_add(x, x); SELECT x; END;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL p1(11);",
				Expected: []sql.Row{{22}},
			},
		},
	},
	{
		Name: "Overloaded Name",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL memory_overloaded_mult(1);",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "CALL memory_overloaded_mult(2, 3);",
				Expected: []sql.Row{{6}},
			},
			{
				Query:    "CALL memory_overloaded_mult(4, 5, 6);",
				Expected: []sql.Row{{120}},
			},
		},
	},
	{
		Name: "Passing in all supported types",
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL memory_overloaded_type_test(1, 100, 10000, 1000000, 100000000, 3, 300," +
					"10, 1000, 100000, 10000000, 1000000000, 30, 3000);",
				Expected: []sql.Row{{1111114444}},
			},
			{
				Query: "CALL memory_overloaded_type_test(false, 'hi', 'A', '2020-02-20 12:00:00', 123.456," +
					"true, 'bye', 'B', '2022-02-02 12:00:00', 654.32);",
				Expected: []sql.Row{{`aa:false,ba:true,ab:"hi",bb:"bye",ac:[65],bc:[66],ad:2020-02-20,bd:2022-02-02,ae:123.456,be:654.32`}},
			},
			{
				Query: "CALL memory_type_test3(1, 100, 10000, 1000000, 100000000, 3, 300," +
					"10, 1000, 100000, 10000000, 1000000000, 30, 3000);",
				Expected: []sql.Row{{uint64(1111114444)}},
			},
		},
	},
	{
		Name: "BOOL and []BYTE INOUT conversions",
		SetUpScript: []string{
			"SET @outparam1 = 1;",
			"SET @outparam2 = 0;",
			"SET @outparam3 = 'A';",
			"SET @outparam4 = 'B';",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT @outparam1, @outparam2, @outparam3, @outparam4;",
				Expected: []sql.Row{{1, 0, "A", "B"}},
			},
			{
				Query:    "CALL memory_inout_bool_byte(@outparam1, @outparam2, @outparam3, @outparam4);",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT @outparam1, @outparam2, @outparam3, @outparam4;",
				Expected: []sql.Row{{1, 1, "A", []byte("C")}},
			},
			{
				Query:    "CALL memory_inout_bool_byte(@outparam1, @outparam2, @outparam3, @outparam4);",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT @outparam1, @outparam2, @outparam3, @outparam4;",
				Expected: []sql.Row{{1, 0, "A", []byte("D")}},
			},
		},
	},
	{
		Name: "Errors returned",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CALL memory_error_table_not_found();",
				ExpectedErr: sql.ErrTableNotFound,
			},
		},
	},
	{
		Name: "Variadic parameter",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL memory_variadic_add();",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "CALL memory_variadic_add(1);",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "CALL memory_variadic_add(1, 2);",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "CALL memory_variadic_add(1, 2, 3);",
				Expected: []sql.Row{{6}},
			},
			{
				Query:    "CALL memory_variadic_add(1, 2, 3, 4);",
				Expected: []sql.Row{{10}},
			},
		},
	},
	{
		Name: "Variadic byte slices",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL memory_variadic_byte_slice();",
				Expected: []sql.Row{{""}},
			},
			{
				Query:    "CALL memory_variadic_byte_slice('A');",
				Expected: []sql.Row{{"A"}},
			},
			{
				Query:    "CALL memory_variadic_byte_slice('A', 'B');",
				Expected: []sql.Row{{"AB"}},
			},
		},
	},
	{
		Name: "Variadic overloading",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CALL memory_variadic_overload();",
				ExpectedErr: sql.ErrCallIncorrectParameterCount,
			},
			{
				Query:       "CALL memory_variadic_overload('A');",
				ExpectedErr: sql.ErrCallIncorrectParameterCount,
			},
			{
				Query:    "CALL memory_variadic_overload('A', 'B');",
				Expected: []sql.Row{{"A-B"}},
			},
			{
				Query:       "CALL memory_variadic_overload('A', 'B', 'C');",
				ExpectedErr: sql.ErrInvalidValue,
			},
			{
				Query:    "CALL memory_variadic_overload('A', 'B', 5);",
				Expected: []sql.Row{{"A,B,[5]"}},
			},
		},
	},
	{
		Name: "show create procedure for external stored procedures",
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create procedure memory_variadic_overload;",
				Expected: []sql.Row{{
					"memory_variadic_overload",
					"",
					"CREATE PROCEDURE memory_variadic_overload() SELECT 'External stored procedure';",
					"utf8mb4",
					"utf8mb4_0900_bin",
					"utf8mb4_0900_bin",
				}},
			},
		},
	},
	{
		// An external procedure marked NotAllowedInSubStatement ends the transaction, so it's rejected wherever COMMIT is.
		Name: "CALL from a trigger can't call an external procedure that ends the transaction",
		SetUpScript: []string{
			"create table lg (id int primary key);",
			"create procedure p_ext(v int) begin insert into lg values (v); call memory_ends_transaction(v); end;",
			"create procedure p_ext_unexecuted(v int) begin if v < 0 then call memory_ends_transaction(v); end if; insert into lg values (v); end;",
			"create procedure p_ext_prepared(v int) begin prepare s from 'call memory_ends_transaction(1)'; execute s; end;",
			"create procedure p_ext_allowed(v int) begin insert into lg values (v + 10); call memory_inout_add_readwrite(v, 1); end;",
			"create table t_ext (id int primary key);",
			"create trigger trg_ext after insert on t_ext for each row call p_ext(new.id);",
			"create table t_ext_unexecuted (id int primary key);",
			"create trigger trg_ext_unexecuted after insert on t_ext_unexecuted for each row call p_ext_unexecuted(new.id);",
			"create table t_ext_prepared (id int primary key);",
			"create trigger trg_ext_prepared after insert on t_ext_prepared for each row call p_ext_prepared(new.id);",
			"create table t_ext_allowed (id int primary key);",
			"create trigger trg_ext_allowed after insert on t_ext_allowed for each row call p_ext_allowed(new.id);",
			"create table t_ext_direct (id int primary key);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "call memory_ends_transaction(1, 2);",
				Expected: []sql.Row{{int64(3)}},
			},
			{
				Query:       "create trigger trg_ext_direct after insert on t_ext_direct for each row call memory_ends_transaction(new.id);",
				ExpectedErr: sql.ErrCommitNotAllowedInSubStatement,
			},
			{
				Query:       "create trigger trg_ext_direct after insert on t_ext_direct for each row begin if new.id < 0 then call memory_ends_transaction(new.id); end if; end;",
				ExpectedErr: sql.ErrCommitNotAllowedInSubStatement,
			},
			{
				Query:       "insert into t_ext values (1), (2);",
				ExpectedErr: sql.ErrCommitNotAllowedInSubStatement,
			},
			{
				Query:       "insert into t_ext_unexecuted values (1);",
				ExpectedErr: sql.ErrCommitNotAllowedInSubStatement,
			},
			{
				Query:       "insert into t_ext_prepared values (1);",
				ExpectedErr: sql.ErrCommitNotAllowedInSubStatement,
			},
			{
				// The procedures were rejected before they ran.
				Query:    "select count(*) from lg;",
				Expected: []sql.Row{{int64(0)}},
			},
			{
				// Other external procedures are still allowed.
				Query:    "insert into t_ext_allowed values (1), (2);",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "select group_concat(id order by id) from lg;",
				Expected: []sql.Row{{"11,12"}},
			},
			{
				// A top-level CALL isn't part of another statement.
				Query:    "call p_ext(3);",
				Expected: []sql.Row{{int64(3)}},
			},
		},
	},
}
