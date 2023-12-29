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
)

// CollationCoercionTest is used to test the resulting collation and coercion of a SQL expression
type CollationCoercionTest struct {
	Parameters   string
	Collation    sql.CollationID
	Coercibility int64
	Error        bool
}

// CollationCoercionSetup is the setup that is run before every CollationCoercionTest
var CollationCoercionSetup = []string{
	`SET CHARACTER SET "utf8mb4";`,
	`SET collation_connection = "utf8mb4_0900_bin";`,
	`SET character_set_results = "binary";`,
	`CREATE TABLE temp_tbl (v1 VARCHAR(200) COLLATE utf8mb4_0900_bin,
    v2 VARCHAR(200) COLLATE utf8mb4_0900_as_cs DEFAULT 'z',
    v3 VARCHAR(200) COLLATE utf8mb4_0900_ai_ci,
    v4 VARCHAR(200) COLLATE utf8mb3_bin,
    v5 VARCHAR(200) COLLATE utf8mb3_general_ci,
    v6 VARCHAR(200) COLLATE latin1_bin,
    v7 VARCHAR(200) COLLATE latin1_general_ci,
    v8 VARBINARY(200));`,
	`INSERT INTO temp_tbl VALUES ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h');`,
}

var CollationCoercionTests = []CollationCoercionTest{
	{
		Parameters:   `'26:27:28'`,
		Collation:    sql.Collation_utf8mb4_0900_bin,
		Coercibility: 4,
	},
	{
		Parameters:   `'str' COLLATE utf8mb4_bin`,
		Collation:    sql.Collation_utf8mb4_bin,
		Coercibility: 0,
	},
	{
		Parameters:   `1001`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `2002.5`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `CONVERT('2020-02-20 20:20:20', DATETIME)`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `CONVERT('2020-02-20', DATE)`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `CONVERT('23:24:25', TIME)`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `CONVERT('34', BINARY)`,
		Collation:    sql.Collation_binary,
		Coercibility: 2,
	},
	{
		Parameters:   `CONVERT('34', SIGNED)`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `CONVERT('34', UNSIGNED)`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `CONVERT('[1]', JSON)`,
		Collation:    sql.Collation_utf8mb4_bin,
		Coercibility: 2,
	},
	{
		Parameters:   `CURDATE()`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `CURRENT_USER()`,
		Collation:    sql.Collation_utf8_general_ci,
		Coercibility: 3,
	},
	{
		Parameters:   `FALSE`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `TRUE`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `NOW()`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `NULL`,
		Collation:    sql.Collation_binary,
		Coercibility: 6,
	},
	{
		Parameters:   `UUID()`,
		Collation:    sql.Collation_utf8_general_ci,
		Coercibility: 4,
	},
	{
		Parameters:   `v1`,
		Collation:    sql.Collation_utf8mb4_0900_bin,
		Coercibility: 2,
	},
	{
		Parameters:   `v1 COLLATE utf8mb4_0900_bin`,
		Collation:    sql.Collation_utf8mb4_0900_bin,
		Coercibility: 0,
	},
	{
		Parameters:   `v2`,
		Collation:    sql.Collation_utf8mb4_0900_as_cs,
		Coercibility: 2,
	},
	{
		Parameters:   `v2 COLLATE utf8mb4_0900_as_cs`,
		Collation:    sql.Collation_utf8mb4_0900_as_cs,
		Coercibility: 0,
	},
	{
		Parameters:   `v3`,
		Collation:    sql.Collation_utf8mb4_0900_ai_ci,
		Coercibility: 2,
	},
	{
		Parameters:   `v3 COLLATE utf8mb4_0900_ai_ci`,
		Collation:    sql.Collation_utf8mb4_0900_ai_ci,
		Coercibility: 0,
	},
	{
		Parameters:   `v4`,
		Collation:    sql.Collation_utf8_bin,
		Coercibility: 2,
	},
	{
		Parameters:   `v4 COLLATE utf8mb3_bin`,
		Collation:    sql.Collation_utf8_bin,
		Coercibility: 0,
	},
	{
		Parameters:   `v5`,
		Collation:    sql.Collation_utf8_general_ci,
		Coercibility: 2,
	},
	{
		Parameters:   `v5 COLLATE utf8mb3_general_ci`,
		Collation:    sql.Collation_utf8_general_ci,
		Coercibility: 0,
	},
	{
		Parameters:   `v6`,
		Collation:    sql.Collation_latin1_bin,
		Coercibility: 2,
	},
	{
		Parameters:   `v6 COLLATE latin1_bin`,
		Collation:    sql.Collation_latin1_bin,
		Coercibility: 0,
	},
	{
		Parameters:   `v7`,
		Collation:    sql.Collation_latin1_general_ci,
		Coercibility: 2,
	},
	{
		Parameters:   `v7 COLLATE latin1_general_ci`,
		Collation:    sql.Collation_latin1_general_ci,
		Coercibility: 0,
	},
	{
		Parameters:   `v8`,
		Collation:    sql.Collation_binary,
		Coercibility: 2,
	},
	{
		Parameters:   `v8 COLLATE 'binary'`,
		Collation:    sql.Collation_binary,
		Coercibility: 0,
	},
	{
		Parameters:   `!('26:27:28')`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `!('str' COLLATE utf8mb4_bin)`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `-(CONVERT('2020-02-20', DATE))`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `-(CONVERT('34', BINARY))`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `-(v6)`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `NOT('{"a": 1, "b": {"c": 30}}')`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `CURRENT_TIME() != '12:34:56'`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `v4 != '26:27:28'`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `FALSE * '26:27:28'`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `FALSE * '26:27:28'`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `v5 + '26:27:28'`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `v5 COLLATE utf8mb3_general_ci + '26:27:28'`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `v6 + '26:27:28'`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `v6 COLLATE latin1_bin + '26:27:28'`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `'str' - CURRENT_TIME()`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `v2 / 1001`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `v2 COLLATE utf8mb4_0900_as_cs / 1001`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `1001 < CONVERT('34', CHAR)`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `2002.5 < CONVERT('34', CHAR)`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `1001 <= RAND()`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `2002.5 <= RAND()`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `CONVERT('23:24:25', TIME) DIV NOW()`,
		Collation:    sql.Collation_binary,
		Coercibility: 5,
	},
	{
		Parameters:   `NULLIF('str' COLLATE utf8mb4_bin, '26:27:28')`,
		Collation:    sql.Collation_utf8mb4_bin,
		Coercibility: 0,
	},
	{
		Parameters:   `NULLIF('{"a": 1, "b": {"c": 30}}', '26:27:28')`,
		Collation:    sql.Collation_utf8mb4_0900_bin,
		Coercibility: 4,
	},
	{
		Parameters:   `REPEAT(v1, 1001)`,
		Collation:    sql.Collation_utf8mb4_0900_bin,
		Coercibility: 2,
	},
	{
		Parameters:   `SUBSTR(v6, CONVERT('2020-02-20 20:20:20', DATETIME))`,
		Collation:    sql.Collation_latin1_bin,
		Coercibility: 2,
	},
	{
		Parameters:   `SUBSTR(v6 COLLATE latin1_bin, CONVERT('2020-02-20 20:20:20', DATETIME))`,
		Collation:    sql.Collation_latin1_bin,
		Coercibility: 0,
	},
}
