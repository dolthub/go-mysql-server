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

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var GeneratedColumnTests = []ScriptTest{
	{
		Name: "stored generated column",
		SetUpScript: []string{
			"create table t1 (a int primary key, b int as (a + 1) stored)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "show create table t1",
				// TODO: double parens here is a bug
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
					"  `a` int NOT NULL,\n" +
					"  `b` int GENERATED ALWAYS AS ((a + 1)) STORED,\n" +
					"  PRIMARY KEY (`a`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t1 values (1,2)",
				ExpectedErr: sql.ErrGeneratedColumnValue,
			},
			{
				Query:    "insert into t1(a,b) values (1,2)",
				ExpectedErr: sql.ErrGeneratedColumnValue,
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{},
			},
			{
				Query:    "insert into t1(a) values (1), (2), (3)",
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.Row{{1, 2}, {2, 3}, {3, 4}},
			},
		},
	},
}