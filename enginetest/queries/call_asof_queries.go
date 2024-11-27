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

import "github.com/dolthub/go-mysql-server/sql"

var CallAsofScripts = []ScriptTest{
	{
		Name: "AS OF propagates to nested CALLs",
		SetUpScript: []string{
			"CREATE PROCEDURE p1() BEGIN CALL p2(); END",
			"CREATE PROCEDURE p1a() BEGIN CALL p2() AS OF '2019-01-01'; END",
			"CREATE PROCEDURE p1b() BEGIN CALL p2a(); END",
			"CREATE PROCEDURE p2() BEGIN SELECT * FROM myhistorytable; END",
			"CREATE PROCEDURE p2a() BEGIN SELECT * FROM myhistorytable AS OF '2019-01-02'; END",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CALL p1();",
				Expected: []sql.UntypedSqlRow{
					{int64(1), "first row, 3", "1"},
					{int64(2), "second row, 3", "2"},
					{int64(3), "third row, 3", "3"},
				},
			},
			{
				Query: "CALL p1a();",
				Expected: []sql.UntypedSqlRow{
					{int64(1), "first row, 1"},
					{int64(2), "second row, 1"},
					{int64(3), "third row, 1"},
				},
			},
			{
				Query: "CALL p1b();",
				Expected: []sql.UntypedSqlRow{
					{int64(1), "first row, 2"},
					{int64(2), "second row, 2"},
					{int64(3), "third row, 2"},
				},
			},
			{
				Query: "CALL p2();",
				Expected: []sql.UntypedSqlRow{
					{int64(1), "first row, 3", "1"},
					{int64(2), "second row, 3", "2"},
					{int64(3), "third row, 3", "3"},
				},
			},
			{
				Query: "CALL p2a();",
				Expected: []sql.UntypedSqlRow{
					{int64(1), "first row, 2"},
					{int64(2), "second row, 2"},
					{int64(3), "third row, 2"},
				},
			},
			{
				Query: "CALL p1() AS OF '2019-01-01';",
				Expected: []sql.UntypedSqlRow{
					{int64(1), "first row, 1"},
					{int64(2), "second row, 1"},
					{int64(3), "third row, 1"},
				},
			},
			{
				Query: "CALL p1a() AS OF '2019-01-03';",
				Expected: []sql.UntypedSqlRow{
					{int64(1), "first row, 1"},
					{int64(2), "second row, 1"},
					{int64(3), "third row, 1"},
				},
			},
			{
				Query: "CALL p1b() AS OF '2019-01-03';",
				Expected: []sql.UntypedSqlRow{
					{int64(1), "first row, 2"},
					{int64(2), "second row, 2"},
					{int64(3), "third row, 2"},
				},
			},
			{
				Query: "CALL p2() AS OF '2019-01-01';",
				Expected: []sql.UntypedSqlRow{
					{int64(1), "first row, 1"},
					{int64(2), "second row, 1"},
					{int64(3), "third row, 1"},
				},
			},
			{
				Query: "CALL p2a() AS OF '2019-01-03';",
				Expected: []sql.UntypedSqlRow{
					{int64(1), "first row, 2"},
					{int64(2), "second row, 2"},
					{int64(3), "third row, 2"},
				},
			},
		},
	},
}
