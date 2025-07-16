// Copyright 2025 Dolthub, Inc.
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

var AutoIncrementOverflowTests = []QueryTest{
	{
		Query: "CREATE TABLE test_tinyint (id TINYINT AUTO_INCREMENT PRIMARY KEY, value VARCHAR(50))",
		Expected: []sql.Row{
			{types.NewOkResult(0)},
		},
	},
	{
		Query: "INSERT INTO test_tinyint (id, value) VALUES (127, 'test127')",
		Expected: []sql.Row{
			{types.NewOkResult(1)},
		},
	},
	{
		Query: "SHOW CREATE TABLE test_tinyint",
		Expected: []sql.Row{
			{"test_tinyint", "CREATE TABLE `test_tinyint` (\n  `id` tinyint NOT NULL AUTO_INCREMENT,\n  `value` varchar(50),\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB AUTO_INCREMENT=127 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
		},
	},
	{
		Query: "CREATE TABLE test_smallint (id SMALLINT AUTO_INCREMENT PRIMARY KEY, value VARCHAR(50))",
		Expected: []sql.Row{
			{types.NewOkResult(0)},
		},
	},
	{
		Query: "INSERT INTO test_smallint (id, value) VALUES (32767, 'test32767')",
		Expected: []sql.Row{
			{types.NewOkResult(1)},
		},
	},
	{
		Query: "SHOW CREATE TABLE test_smallint",
		Expected: []sql.Row{
			{"test_smallint", "CREATE TABLE `test_smallint` (\n  `id` smallint NOT NULL AUTO_INCREMENT,\n  `value` varchar(50),\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB AUTO_INCREMENT=32767 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
		},
	},
	{
		Query: "CREATE TABLE test_int (id INT AUTO_INCREMENT PRIMARY KEY, value VARCHAR(50))",
		Expected: []sql.Row{
			{types.NewOkResult(0)},
		},
	},
	{
		Query: "INSERT INTO test_int (id, value) VALUES (2147483647, 'test2147483647')",
		Expected: []sql.Row{
			{types.NewOkResult(1)},
		},
	},
	{
		Query: "SHOW CREATE TABLE test_int",
		Expected: []sql.Row{
			{"test_int", "CREATE TABLE `test_int` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  `value` varchar(50),\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB AUTO_INCREMENT=2147483647 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
		},
	},
	{
		Query: "CREATE TABLE test_bigint (id BIGINT AUTO_INCREMENT PRIMARY KEY, value VARCHAR(50))",
		Expected: []sql.Row{
			{types.NewOkResult(0)},
		},
	},
	{
		Query: "INSERT INTO test_bigint (id, value) VALUES (9223372036854775807, 'test9223372036854775807')",
		Expected: []sql.Row{
			{types.NewOkResult(1)},
		},
	},
	{
		Query: "SHOW CREATE TABLE test_bigint",
		Expected: []sql.Row{
			{"test_bigint", "CREATE TABLE `test_bigint` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `value` varchar(50),\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB AUTO_INCREMENT=9223372036854775807 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
		},
	},
}

var AutoIncrementOverflowErrorTests = []QueryErrorTest{
	{
		Query:       "INSERT INTO test_tinyint (value) VALUES ('test_overflow')",
		ExpectedErr: sql.ErrPrimaryKeyViolation,
	},
	{
		Query:       "INSERT INTO test_smallint (value) VALUES ('test_overflow')",
		ExpectedErr: sql.ErrPrimaryKeyViolation,
	},
	{
		Query:       "INSERT INTO test_int (value) VALUES ('test_overflow')",
		ExpectedErr: sql.ErrPrimaryKeyViolation,
	},
	{
		Query:       "INSERT INTO test_bigint (value) VALUES ('test_overflow')",
		ExpectedErr: sql.ErrPrimaryKeyViolation,
	},
}
