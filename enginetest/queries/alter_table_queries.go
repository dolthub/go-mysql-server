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

var AlterTableScripts = []ScriptTest{
	{
		Name: "Error queries",
		SetUpScript: []string{
			"create table one_pk_two_idx (pk bigint primary key, v1 bigint, v2 bigint, index idx1 (v1), index idx2 (v2));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE one_pk_two_idx MODIFY COLUMN v1 BIGINT DEFAULT (pk) AFTER v3",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "ALTER TABLE one_pk_two_idx ADD COLUMN v4 BIGINT DEFAULT (pk) AFTER v3",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "ALTER TABLE one_pk_two_idx ADD COLUMN v3 BIGINT DEFAULT 5, RENAME COLUMN v3 to v4",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "ALTER TABLE one_pk_two_idx ADD COLUMN v3 BIGINT DEFAULT 5, modify column v3 bigint default null",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/6206
		Name: "alter table containing column default value expressions",
		SetUpScript: []string{
			"create table t (pk int primary key, col1 timestamp default current_timestamp(), col2 varchar(1000), index idx1 (pk, col1));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "alter table t alter column col2 DROP DEFAULT;",
				Expected: []sql.Row{},
			},
			{
				Query:    "show create table t;",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` timestamp(6) DEFAULT (CURRENT_TIMESTAMP()),\n  `col2` varchar(1000),\n  PRIMARY KEY (`pk`),\n  KEY `idx1` (`pk`,`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "alter table t alter column col2 SET DEFAULT 'FOO!';",
				Expected: []sql.Row{},
			},
			{
				Query:    "show create table t;",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n  `pk` int NOT NULL,\n  `col1` timestamp(6) DEFAULT (CURRENT_TIMESTAMP()),\n  `col2` varchar(1000) DEFAULT 'FOO!',\n  PRIMARY KEY (`pk`),\n  KEY `idx1` (`pk`,`col1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "alter table t drop index idx1;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "drop column drops check constraint",
		SetUpScript: []string{
			"create table t34 (i bigint primary key, s varchar(20))",
			"ALTER TABLE t34 ADD COLUMN j int",
			"ALTER TABLE t34 ADD CONSTRAINT test_check CHECK (j < 12345)",
			"ALTER TABLE t34 DROP COLUMN j",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t34",
				Expected: []sql.Row{{"t34", "CREATE TABLE `t34` (\n" +
						"  `i` bigint NOT NULL,\n" +
						"  `s` varchar(20),\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
}
