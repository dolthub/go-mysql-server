// Copyright 2026 Dolthub, Inc.
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
	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ConversionsScriptTests contains self-contained conversions script tests.
var ConversionsScriptTests = []ScriptTest{
	{
		Dialect: "mysql",
		Name:    "string to number comparison correctly truncates",
		Assertions: []ScriptTestAssertion{
			{
				Query:                           "SELECT 'A' = 0;",
				Expected:                        []sql.Row{{true}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'A'",
			},
			{
				Query:                           "SELECT 'A' != 0;",
				Expected:                        []sql.Row{{false}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'A'",
			},
			{
				Query:                           "SELECT 'A' <> 0;",
				Expected:                        []sql.Row{{false}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'A'",
			},
			{
				Query:                           "SELECT 'A' < 0;",
				Expected:                        []sql.Row{{false}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'A'",
			},
			{
				Query:                           "SELECT 'A' <= 0;",
				Expected:                        []sql.Row{{true}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'A'",
			},
			{
				Query:                           "SELECT 'A' > 0;",
				Expected:                        []sql.Row{{false}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'A'",
			},
			{
				Query:                           "SELECT 'A' >= 0;",
				Expected:                        []sql.Row{{true}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'A'",
			},
			{
				Query:    "SELECT '' = 0;",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "select 'abc' = false",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "select '1abc' = true",
				Expected: []sql.Row{{true}},
			},
			{
				// Truncates to 123
				Query:    "select '123abc' = false",
				Expected: []sql.Row{{false}},
			},
			{
				// Truncates to 123
				Query:    "select '123abc' = true",
				Expected: []sql.Row{{false}},
			},
			{
				Query:                           "SELECT '123A' = 123;",
				Expected:                        []sql.Row{{true}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: '123A'",
			},
			{
				Query:                           "SELECT 'A123' = 0;",
				Expected:                        []sql.Row{{true}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'A123'",
			},
			{
				Query:    "SELECT '123.456' = 123;",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "SELECT '123.456' = 123.456;",
				Expected: []sql.Row{{true}},
			},
			{
				// TODO: 123.456 is converted to a DECIMAL by Builder.ConvertVal, when it should be a DOUBLE
				SkipResultCheckOnServerEngine:   true, // TODO: warnings do not make it to server engine
				Query:                           "SELECT '123.456ABC' = 123.456;",
				Expected:                        []sql.Row{{true}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect decimal(65,30) value: '123.456ABC'",
			},
			{
				Query:    "SELECT '123.456e2' = 12345.6;",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "SELECT '123.456e-2' = 1.23456;",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "SELECT '1.9a' = 1.9;",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "SELECT 1 where '1.9a' = 1.9;",
				Expected: []sql.Row{{1}},
			},
			{
				// Valid float strings used as arguments to functions are truncated not rounded
				Query:                 "SELECT LENGTH(SPACE('1.9'));",
				Expected:              []sql.Row{{1}},
				ExpectedWarningsCount: 1, // TODO: MySQL throws two warnings for some reason
				ExpectedWarning:       mysql.ERTruncatedWrongValue,
			},
			{
				// TODO: 123.456 is converted to a DECIMAL by Builder.ConvertVal, when it should be a DOUBLE
				Query:                           "SELECT -'+123.456ABC' = -123.456",
				Expected:                        []sql.Row{{true}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect decimal(65,30) value: '+123.456ABC'",
			},
			{
				Query:                           "SELECT '0xBEEF' = 0;",
				Expected:                        []sql.Row{{true}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: '0xBEEF'",
			},
			{
				// 'A' is truncated to 0
				Query:                           "SELECT 'A' IN (0)",
				Expected:                        []sql.Row{{true}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'A'",
			},
			{
				// 'A' is truncated to 0
				Query:                           "SELECT 'A' NOT IN (0)",
				Expected:                        []sql.Row{{false}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'A'",
			},
			{
				Query:    "SELECT '' in (0);",
				Expected: []sql.Row{{true}},
			},
			{
				Query:                           "SELECT '123A' in (123);",
				Expected:                        []sql.Row{{true}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: '123A'",
			},
			{
				Query:                           "SELECT 123 in ('123A');",
				Expected:                        []sql.Row{{true}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: '123A'",
			},
			{
				Query:                           "SELECT 'A123' in (0);",
				Expected:                        []sql.Row{{true}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'A123'",
			},
			{
				Query:                           "SELECT '123abc' in ('string', 1, 2, 123);",
				Expected:                        []sql.Row{{true}},
				ExpectedWarningsCount:           3, // MySQL only throws 1 warning
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value",
			},
			{
				Query:                           "SELECT 123 in ('string', 1, 2, '123abc');",
				Expected:                        []sql.Row{{true}},
				ExpectedWarningsCount:           2,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value",
			},
			{
				Query:                           "SELECT '123A' in (123);",
				Expected:                        []sql.Row{{true}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: '123A'",
			},
			{
				Query:    "SELECT '123.456' in (123);",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "SELECT '123.456' in (123.456);",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "SELECT 123.456 in (123.456);",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "SELECT 123.45 in (123.4);",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "SELECT 123.45 in (123.5);",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "SELECT '123.45a' in (123.5);",
				Expected: []sql.Row{{false}},
			},
			{
				Query:    "SELECT '123.45a' in (123.4);",
				Expected: []sql.Row{{false}},
			},
			{
				SkipResultCheckOnServerEngine:   true, // TODO: warnings do not make it to server engine
				Query:                           "SELECT '123.456ABC' in (123.456);",
				Expected:                        []sql.Row{{true}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect decimal(65,30) value: '123.456ABC'",
			},
			{
				Query:    "SELECT '123.456e2' in (12345.6);",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "SELECT '123.456e-2' in (1.23456);",
				Expected: []sql.Row{{true}},
			},
			{
				Query:                           "SELECT '0xBEEF' = 0;",
				Expected:                        []sql.Row{{true}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: '0xBEEF'",
			},
			{
				Query:                           `select 'a' + 4;`,
				Expected:                        []sql.Row{{4.0}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           `select '20a' + 4;`,
				Expected:                        []sql.Row{{24.0}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: '20a'",
			},
			{
				Query:                           `select '10.a' + 4;`,
				Expected:                        []sql.Row{{14.0}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: '10.a'",
			},
			{
				Query:                           `select '.20a' + 4;`,
				Expected:                        []sql.Row{{4.2}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: '.20a'",
			},
			{
				Query:                           `select 4 + 'a';`,
				Expected:                        []sql.Row{{4.0}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                 `select 'a' + 'a';`,
				Expected:              []sql.Row{{0.0}},
				ExpectedWarningsCount: 2,
				ExpectedWarning:       mysql.ERTruncatedWrongValue,
			},
			{
				Query:                           `select 'a' - 4;`,
				Expected:                        []sql.Row{{-4.0}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           `select 4 - 'a';`,
				Expected:                        []sql.Row{{4.0}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           `select 4 - '2a';`,
				Expected:                        []sql.Row{{2.0}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: '2a'",
			},
			{
				Query:                 `select 'a' - 'a';`,
				Expected:              []sql.Row{{0.0}},
				ExpectedWarningsCount: 2,
				ExpectedWarning:       mysql.ERTruncatedWrongValue,
			},
			{
				Query:                           `select 'a' * 4;`,
				Expected:                        []sql.Row{{0.0}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           `select 4 * 'a';`,
				Expected:                        []sql.Row{{0.0}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                 `select 'a' * 'a';`,
				Expected:              []sql.Row{{0.0}},
				ExpectedWarningsCount: 2,
				ExpectedWarning:       mysql.ERTruncatedWrongValue,
			},
			{
				Query:                           "select 1 * '2.50a';",
				Expected:                        []sql.Row{{2.5}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: '2.50a'",
			},
			{
				Query:                           "select 1 * '2.a50a';",
				Expected:                        []sql.Row{{2.0}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: '2.a50a'",
			},
			{
				Query:                           `select 'a' / 4;`,
				Expected:                        []sql.Row{{0.0}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                 `select 4 / 'a';`,
				Expected:              []sql.Row{{nil}},
				ExpectedWarningsCount: 2, // Truncated incorrect value and Divide by Zero
			},
			{
				Query:                 `select 'a' / 'a';`,
				Expected:              []sql.Row{{nil}},
				ExpectedWarningsCount: 2, // Truncated incorrect value and Divide by Zero
			},
			{
				Query:                 "select 1 / '2.50a';",
				Expected:              []sql.Row{{0.4}},
				ExpectedWarningsCount: 2, // Truncated incorrect value and Divide by Zero
			},
			{
				Query:                 "select 1 / '2.a50a';",
				Expected:              []sql.Row{{0.5}},
				ExpectedWarningsCount: 2, // Truncated incorrect value and Divide by Zero
			},
			{
				Query:                           "select 'a' & 'a';",
				Expected:                        []sql.Row{{uint64(0)}},
				ExpectedWarningsCount:           2,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           "select 'a' & 4;",
				Expected:                        []sql.Row{{uint64(0)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           "select 4 & 'a';",
				Expected:                        []sql.Row{{uint64(0)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           "select 'a' | 'a';",
				Expected:                        []sql.Row{{uint64(0)}},
				ExpectedWarningsCount:           2,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           "select 'a' | 4;",
				Expected:                        []sql.Row{{uint64(4)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           "select 'a' | -1;",
				Expected:                        []sql.Row{{uint64(18446744073709551615)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           "select 4 | 'a';",
				Expected:                        []sql.Row{{uint64(4)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           "select 'a' ^ 'a';",
				Expected:                        []sql.Row{{uint64(0)}},
				ExpectedWarningsCount:           2,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           "select 'a' ^ 4;",
				Expected:                        []sql.Row{{uint64(4)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           "select 'a' ^ -1;",
				Expected:                        []sql.Row{{uint64(18446744073709551615)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           "select 4 ^ 'a';",
				Expected:                        []sql.Row{{uint64(4)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           "select 'a' >> 'a';",
				Expected:                        []sql.Row{{uint64(0)}},
				ExpectedWarningsCount:           2,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           "select 'a' >> 4;",
				Expected:                        []sql.Row{{uint64(0)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           "select 4 >> 'a';",
				Expected:                        []sql.Row{{uint64(4)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           "select -1 >> 'a';",
				Expected:                        []sql.Row{{uint64(18446744073709551615)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           "select 'a' << 'a';",
				Expected:                        []sql.Row{{uint64(0)}},
				ExpectedWarningsCount:           2,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           "select 'a' << 4;",
				Expected:                        []sql.Row{{uint64(0)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           "select '2a' << 4;",
				Expected:                        []sql.Row{{uint64(32)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: '2a'",
			},
			{
				Query:                           "select 4 << 'a';",
				Expected:                        []sql.Row{{uint64(4)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                           "select -1 << 'a';",
				Expected:                        []sql.Row{{uint64(18446744073709551615)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                 "select 'a' div 'a';",
				Expected:              []sql.Row{{nil}},
				ExpectedWarningsCount: 3, // Truncated incorrect value (2c) and Divide by Zero
			},
			{
				Query:                           "select 'a' div 4;",
				Expected:                        []sql.Row{{0}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect DECIMAL value: 'a'",
			},
			{
				Query:                 "select 4 div 'a';",
				Expected:              []sql.Row{{nil}},
				ExpectedWarningsCount: 2, // Truncated incorrect value and Divide by Zero
			},
			{
				Query:    "select 1.2 div '1';",
				Expected: []sql.Row{{1}},
			},
			{
				Query:                 "select 1.2 div 'a1';",
				Expected:              []sql.Row{{nil}},
				ExpectedWarningsCount: 2, // Truncated incorrect value and Divide by Zero
			},
			{
				Query:                           "select '12a' div '3' ;",
				Expected:                        []sql.Row{{4}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect DECIMAL value: '12a'",
			},
			{
				Query:                 "select 'a' mod 'a';",
				Expected:              []sql.Row{{nil}},
				ExpectedWarningsCount: 2, // Truncated incorrect value and Divide by Zero
			},
			{
				Query:                           "select 'a' mod 4;",
				Expected:                        []sql.Row{{float64(0)}},
				ExpectedWarningsCount:           1,
				ExpectedWarning:                 mysql.ERTruncatedWrongValue,
				ExpectedWarningMessageSubstring: "Truncated incorrect double value: 'a'",
			},
			{
				Query:                 "select 4 mod 'a';",
				Expected:              []sql.Row{{nil}},
				ExpectedWarningsCount: 2, // Truncated incorrect value and Divide by Zero
			},
			{
				Query:    "SELECT '127' | '128', '128' << 2;",
				Expected: []sql.Row{{uint64(255), uint64(512)}},
			},
			{
				Query:    "SELECT X'7F' | X'80', X'80' << 2;",
				Expected: []sql.Row{{uint64(255), uint64(512)}},
			},
			{
				Query:    "SELECT X'40' | X'01', b'11110001' & b'01001111';",
				Expected: []sql.Row{{uint64(65), uint64(65)}},
			},
			{
				Skip:     true,
				Query:    "SELECT 0x1 = 1;",
				Expected: []sql.Row{{true}},
			},
		},
	},
	{
		// Related Issues:
		//   https://github.com/dolthub/dolt/issues/9733
		//   https://github.com/dolthub/dolt/issues/9739
		Dialect: "mysql",
		Name:    "strings cast to numbers",
		SetUpScript: []string{
			"create table test01(pk varchar(20) primary key);",
			`insert into test01 values ('  3 12 4'),
                          ('  3.2 12 4'),('-3.1234'),('-3.1a'),('-5+8'),('+3.1234'),
                          ('11d'),('11wha?'),('11'),('12'),('1a1'),('a1a1'),('11-5'),
                          ('3. 12 4'),('5.932887e+07'),('5.932887e+07abc'),('5.932887e7'),('5.932887e7abc');`,
			"create table test02(pk int primary key);",
			"insert into test02 values(11),(12),(13),(14),(15);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select pk, cast(pk as float) from test01",
				Expected: []sql.Row{
					{"  3 12 4", float32(3)},
					{"  3.2 12 4", float32(3.2)},
					{"-3.1234", float32(-3.1234)},
					{"-3.1a", float32(-3.1)},
					{"-5+8", float32(-5)},
					{"+3.1234", float32(3.1234)},
					{"11", float32(11)},
					{"11-5", float32(11)},
					{"11d", float32(11)},
					{"11wha?", float32(11)},
					{"12", float32(12)},
					{"1a1", float32(1)},
					{"3. 12 4", float32(3)},
					{"5.932887e+07", float32(5.932887e+07)},
					{"5.932887e+07abc", float32(5.932887e+07)},
					{"5.932887e7", float32(5.932887e+07)},
					{"5.932887e7abc", float32(5.932887e+07)},
					{"a1a1", float32(0)},
				},
				ExpectedWarningsCount: 12,
				ExpectedWarning:       mysql.ERTruncatedWrongValue,
			},
			{
				Dialect: "mysql",
				Query:   "select pk, cast(pk as double) from test01",
				Expected: []sql.Row{
					{"  3 12 4", 3.0},
					{"  3.2 12 4", 3.2},
					{"-3.1234", -3.1234},
					{"-3.1a", -3.1},
					{"-5+8", -5.0},
					{"+3.1234", 3.1234},
					{"11", 11.0},
					{"11-5", 11.0},
					{"11d", 11.0},
					{"11wha?", 11.0},
					{"12", 12.0},
					{"1a1", 1.0},
					{"3. 12 4", 3.0},
					{"5.932887e+07", 5.932887e+07},
					{"5.932887e+07abc", 5.932887e+07},
					{"5.932887e7", 5.932887e+07},
					{"5.932887e7abc", 5.932887e+07},
					{"a1a1", 0.0},
				},
				ExpectedWarningsCount: 12,
				ExpectedWarning:       mysql.ERTruncatedWrongValue,
			},
			{
				Query: "select pk, cast(pk as signed) from test01",
				Expected: []sql.Row{
					{"  3 12 4", 3},
					{"  3.2 12 4", 3},
					{"-3.1234", -3},
					{"-3.1a", -3},
					{"-5+8", -5},
					{"+3.1234", 3},
					{"11", 11},
					{"11-5", 11},
					{"11d", 11},
					{"11wha?", 11},
					{"12", 12},
					{"1a1", 1},
					{"3. 12 4", 3},
					{"5.932887e+07", 5},
					{"5.932887e+07abc", 5},
					{"5.932887e7", 5},
					{"5.932887e7abc", 5},
					{"a1a1", 0},
				},
				ExpectedWarningsCount: 16,
				ExpectedWarning:       mysql.ERTruncatedWrongValue,
			},
			{
				Query: "select pk, cast(pk as unsigned) from test01",
				Expected: []sql.Row{
					{"  3 12 4", uint64(3)},
					{"  3.2 12 4", uint64(3)},
					{"-3.1234", uint64(18446744073709551613)},
					{"-3.1a", uint64(18446744073709551613)},
					{"-5+8", uint64(18446744073709551611)},
					{"+3.1234", uint64(3)},
					{"11", uint64(11)},
					{"11-5", uint64(11)},
					{"11d", uint64(11)},
					{"11wha?", uint64(11)},
					{"12", uint64(12)},
					{"1a1", uint64(1)},
					{"3. 12 4", uint64(3)},
					{"5.932887e+07", uint64(5)},
					{"5.932887e+07abc", uint64(5)},
					{"5.932887e7", uint64(5)},
					{"5.932887e7abc", uint64(5)},
					{"a1a1", uint64(0)},
				},
				ExpectedWarningsCount: 19,
				// Can't check multiple different warnings
			},
			{
				Query: "select pk, cast(pk as decimal(12,3)) from test01",
				Expected: []sql.Row{
					{"  3 12 4", "3.000"},
					{"  3.2 12 4", "3.200"},
					{"-3.1234", "-3.123"},
					{"-3.1a", "-3.100"},
					{"-5+8", "-5.000"},
					{"+3.1234", "3.123"},
					{"11", "11.000"},
					{"11-5", "11.000"},
					{"11d", "11.000"},
					{"11wha?", "11.000"},
					{"12", "12.000"},
					{"1a1", "1.000"},
					{"3. 12 4", "3.000"},
					{"5.932887e+07", "59328870.000"},
					{"5.932887e+07abc", "59328870.000"},
					{"5.932887e7", "59328870.000"},
					{"5.932887e7abc", "59328870.000"},
					{"a1a1", "0.000"},
				},
				// TODO: should be 13. Missing warning for "Incorrect DECIMAL value: '0' for column '' at row -1" (1366)
				ExpectedWarningsCount: 12,
				ExpectedWarning:       mysql.ERTruncatedWrongValue,
			},
			{
				Query:                 "select * from test01 where pk in ('11')",
				Expected:              []sql.Row{{"11"}},
				ExpectedWarningsCount: 0,
			},
			{
				// https://github.com/dolthub/dolt/issues/9739
				Dialect: "mysql",
				Query:   "select * from test01 where pk in (11)",
				Expected: []sql.Row{
					{"11"},
					{"11-5"},
					{"11d"},
					{"11wha?"},
				},
				ExpectedWarningsCount: 12,
				ExpectedWarning:       mysql.ERTruncatedWrongValue,
			},
			{
				// https://github.com/dolthub/dolt/issues/9739
				Skip:    true, // this passes in gms but not dolt
				Dialect: "mysql",
				Query:   "select * from test01 where pk=3",
				Expected: []sql.Row{
					{"  3 12 4"},
					{"3. 12 4"},
				},
				ExpectedWarningsCount: 12,
				ExpectedWarning:       mysql.ERTruncatedWrongValue,
			},
			{
				// https://github.com/dolthub/dolt/issues/9739
				Dialect: "mysql",
				Query:   "select * from test01 where pk>=3 and pk < 4",
				Expected: []sql.Row{
					{"  3 12 4"},
					{"  3.2 12 4"},
					{"+3.1234"},
					{"3. 12 4"},
				},
				ExpectedWarningsCount: 20,
				ExpectedWarning:       mysql.ERTruncatedWrongValue,
			},
			{
				Dialect:               "mysql",
				Query:                 "select * from test02 where pk in ('11asdf');",
				Expected:              []sql.Row{{11}},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       mysql.ERTruncatedWrongValue,
			},
			{
				Dialect:               "mysql",
				Query:                 "select * from test02 where pk='11.12asdf';",
				Expected:              []sql.Row{},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       mysql.ERTruncatedWrongValue,
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9821
		Name: "strings with boolean operators",
		Assertions: []ScriptTestAssertion{
			{
				Dialect:               "mysql",
				Query:                 `select '3bxu' and true`,
				Expected:              []sql.Row{{true}},
				ExpectedWarning:       mysql.ERTruncatedWrongValue,
				ExpectedWarningsCount: 1,
			},
			{
				Dialect:               "mysql",
				Query:                 "select '3bxu' or false",
				Expected:              []sql.Row{{true}},
				ExpectedWarning:       mysql.ERTruncatedWrongValue,
				ExpectedWarningsCount: 1,
			},
			{
				Dialect:               "mysql",
				Query:                 "select '3bxu' xor false",
				Expected:              []sql.Row{{true}},
				ExpectedWarning:       mysql.ERTruncatedWrongValue,
				ExpectedWarningsCount: 1,
			},
			{
				Query:                 "select '' or false",
				Expected:              []sql.Row{{false}},
				ExpectedWarningsCount: 0,
			},
			{
				Query:                 "select '0' or false",
				Expected:              []sql.Row{{false}},
				ExpectedWarningsCount: 0,
			},
			{
				Query:                 "select '00' or false",
				Expected:              []sql.Row{{false}},
				ExpectedWarningsCount: 0,
			},
			{
				Dialect:               "mysql",
				Query:                 "select '00asdf' or false",
				Expected:              []sql.Row{{false}},
				ExpectedWarning:       mysql.ERTruncatedWrongValue,
				ExpectedWarningsCount: 1,
			},
			{
				Dialect:               "mysql",
				Query:                 "select 'asdf' or false",
				Expected:              []sql.Row{{false}},
				ExpectedWarning:       mysql.ERTruncatedWrongValue,
				ExpectedWarningsCount: 1,
			},
		},
	},
	{
		Dialect: "mysql",
		Name:    "complicated string to numeric conversion",
		SetUpScript: []string{
			"CREATE TABLE t0(c INT);",
			"INSERT INTO t0 VALUES (1);",
			"CREATE TABLE t1(c VARCHAR(500));",
			"INSERT INTO t1 VALUES ('1a');",
			"CREATE TABLE t2(c0 INT , c1 BOOLEAN , c2 BOOLEAN , c3 INT , placeholder0 INT , placeholder1 VARCHAR(500) , placeholder2 VARCHAR(500) , PRIMARY KEY(placeholder0));",
			"CREATE TABLE t3(c0 INT , c1 VARCHAR(500) , c2 BOOLEAN , c3 VARCHAR(500) , placeholder0 BOOLEAN , placeholder1 INT , placeholder2 VARCHAR(500));",
			"INSERT INTO t3 VALUES (7, '0y4', TRUE, '5y', TRUE, 5, 'p9c');",
			"INSERT INTO t3 VALUES (1, '4', TRUE, '4H', FALSE, 9, 'Zy4');",
			"INSERT INTO t3 VALUES (10, '1a', FALSE, 'pYE', FALSE, 3, '0awX');",
			"INSERT INTO t3 VALUES (8, 'J', TRUE, 'LE', TRUE, 9, 'YEqQ');",
			"INSERT INTO t2 VALUES (10, FALSE, TRUE, 2, 2, 'nfxF', 'xvC');",
			"INSERT INTO t2 VALUES (10, TRUE, TRUE, 10, 1, 'rlQT', 'W');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t0, t1 WHERE (t1.c IN (true));",
				Expected: []sql.Row{
					{1, "1a"},
				},
			},
			{
				Query: "SELECT * FROM t3 INNER JOIN t2 ON ((((t3.c0) = ((EXTRACT(YEAR FROM DATE_ADD(DATE '2000-01-01', INTERVAL ( BIT_LENGTH(( MOD(t2.c3 + ( t2.c3 + ( BIT_COUNT(t2.c3) ) * 3 - CAST(( NOT (t2.c0 XOR t2.c2) ) AS SIGNED) ) * 2, 100 + t2.c3) ) ^ t2.c3) ) DAY)) % (t2.c3 + 1))))) >= (((t3.c2) < ((((((('Bs./')OR('wZ')) IN ((('1066274936')OR('')))))OR((((t3.c1 IN (true)))<>(((t3.c0)OR(( COALESCE(NULLIF(t3.c3, ''), t3.c1) ))))))))))));",
				Expected: []sql.Row{
					{7, "0y4", 1, "5y", 1, 5, "p9c", 10, 1, 1, 10, 1, "rlQT", "W"},
					{1, "4", 1, "4H", 0, 9, "Zy4", 10, 1, 1, 10, 1, "rlQT", "W"},
					{10, "1a", 0, "pYE", 0, 3, "0awX", 10, 1, 1, 10, 1, "rlQT", "W"},
					{8, "J", 1, "LE", 1, 9, "YEqQ", 10, 1, 1, 10, 1, "rlQT", "W"},
					{7, "0y4", 1, "5y", 1, 5, "p9c", 10, 0, 1, 2, 2, "nfxF", "xvC"},
					{1, "4", 1, "4H", 0, 9, "Zy4", 10, 0, 1, 2, 2, "nfxF", "xvC"},
					{10, "1a", 0, "pYE", 0, 3, "0awX", 10, 0, 1, 2, 2, "nfxF", "xvC"},
					{8, "J", 1, "LE", 1, 9, "YEqQ", 10, 0, 1, 2, 2, "nfxF", "xvC"},
				},
			},
			{
				Query: "SELECT * FROM t3 CROSS JOIN t2 WHERE ((((t3.c0) = ((EXTRACT(YEAR FROM DATE_ADD(DATE '2000-01-01', INTERVAL ( BIT_LENGTH(( MOD(t2.c3 + ( t2.c3 + ( BIT_COUNT(t2.c3) ) * 3 - CAST(( NOT (t2.c0 XOR t2.c2) ) AS SIGNED) ) * 2, 100 + t2.c3) ) ^ t2.c3) ) DAY)) % (t2.c3 + 1))))) >= (((t3.c2) < ((((((('Bs./')OR('wZ')) IN ((('1066274936')OR('')))))OR((((t3.c1 IN (true)))<>(((t3.c0)OR(( COALESCE(NULLIF(t3.c3, ''), t3.c1) ))))))))))));",
				Expected: []sql.Row{
					{7, "0y4", 1, "5y", 1, 5, "p9c", 10, 1, 1, 10, 1, "rlQT", "W"},
					{1, "4", 1, "4H", 0, 9, "Zy4", 10, 1, 1, 10, 1, "rlQT", "W"},
					{10, "1a", 0, "pYE", 0, 3, "0awX", 10, 1, 1, 10, 1, "rlQT", "W"},
					{8, "J", 1, "LE", 1, 9, "YEqQ", 10, 1, 1, 10, 1, "rlQT", "W"},
					{7, "0y4", 1, "5y", 1, 5, "p9c", 10, 0, 1, 2, 2, "nfxF", "xvC"},
					{1, "4", 1, "4H", 0, 9, "Zy4", 10, 0, 1, 2, 2, "nfxF", "xvC"},
					{10, "1a", 0, "pYE", 0, 3, "0awX", 10, 0, 1, 2, 2, "nfxF", "xvC"},
					{8, "J", 1, "LE", 1, 9, "YEqQ", 10, 0, 1, 2, 2, "nfxF", "xvC"},
				},
			},
		},
	},
	{
		Name: "Handle hex number to binary conversion",
		SetUpScript: []string{
			"CREATE TABLE hex_nums1 (pk BIGINT PRIMARY KEY, v1 INT, v2 BIGINT UNSIGNED, v3 DOUBLE, v4 BINARY(32));",
			"INSERT INTO hex_nums1 values (1, 0x7ED0599B, 0x765a8ce4ce74b187, 0xF753AD20B0C4, 0x148aa875c3cdb9af8919493926a3d7c6862fec7f330152f400c0aecb4467508a);",
			"CREATE TABLE hex_nums2 (pk BIGINT PRIMARY KEY, v1 VARBINARY(255), v2 BLOB);",
			"INSERT INTO hex_nums2 values (1, 0x765a8ce4ce74b187, 0x148aa875c3cdb9af8919493926a3d7c6862fec7f330152f400c0aecb4467508a);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT v1, v2, v3, hex(v4) FROM hex_nums1;",
				Expected: []sql.Row{{2127583643, uint64(8528283758723641735), float64(271938758947012), "148AA875C3CDB9AF8919493926A3D7C6862FEC7F330152F400C0AECB4467508A"}},
			},
			{
				Query:    "SELECT hex(v1), hex(v2), hex(v3), hex(v4) FROM hex_nums1;",
				Expected: []sql.Row{{"7ED0599B", "765A8CE4CE74B187", "F753AD20B0C4", "148AA875C3CDB9AF8919493926A3D7C6862FEC7F330152F400C0AECB4467508A"}},
			},
			{
				Query:    "SELECT hex(v1), hex(v2) FROM hex_nums2;",
				Expected: []sql.Row{{"765A8CE4CE74B187", "148AA875C3CDB9AF8919493926A3D7C6862FEC7F330152F400C0AECB4467508A"}},
			},
		},
	},
	{
		Name: "decimal and float in tuple",
		SetUpScript: []string{
			"create table t (d decimal(10, 3), f float);",
			"insert into t values (0.8, 0.8);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from t where (d in (null, 1));",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t where (f in (null, 1));",
				Expected: []sql.Row{},
			},
			{
				// select count to avoid floating point comparison
				Query: "select count(*) from t where (d in (null, 0.8));",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				// This actually matches MySQL behavior
				Query:    "select * from t where (f in (null, 0.8));",
				Expected: []sql.Row{},
			},
			{
				// This actually matches MySQL behavior
				Query: "select count(*) from t where (f in (null, 0.8));",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				// select count to avoid floating point comparison
				Query: "select count(*) from t where (f in (null, cast(0.8 as float)));",
				Expected: []sql.Row{
					{1},
				},
			},
		},
	},
	{
		Name:    "floats in tuple are properly hashed",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (b bool);",
			"insert into t values (false);",
			"create table t_idx (b bool);",
			"create index idx on t_idx(b);",
			"insert into t_idx values (false);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t where (b in (-''));",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from t where (b in (false/'1'));",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from t_idx where (b in (-''));",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from t_idx where (b in (false/'1'));",
				Expected: []sql.Row{
					{0},
				},
			},
		},
	},
	{
		Name:    "hash in tuple picks correct type and skips mixed types",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (v varchar(10));",
			"insert into t values ('abc'), ('def'), ('ghi');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from t where (v in ('xyz')) order by v;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from t where (v in (0, 'xyz')) order by v;",
				Expected: []sql.Row{
					{"abc"},
					{"def"},
					{"ghi"},
				},
			},
			{
				Query:    "select * from t where (v in (1, 'xyz')) order by v;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name:    "strings in tuple are properly hashed",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (v varchar(100));",
			"insert into t values (false);",
			"create table t_idx (v varchar(100));",
			"create index idx on t_idx(v);",
			"insert into t_idx values (false);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t where (v in (-''));",
				Expected: []sql.Row{
					{"0"},
				},
			},
			{
				Query: "select * from t where (v in (false/'1'));",
				Expected: []sql.Row{
					{"0"},
				},
			},
			{
				Query: "select * from t_idx where (v in (-''));",
				Expected: []sql.Row{
					{"0"},
				},
			},
			{
				Query: "select * from t_idx where (v in (false/'1'));",
				Expected: []sql.Row{
					{"0"},
				},
			},
		},
	},
	{
		Name: "strings vs decimals with trailing 0s in IN exprs",
		SetUpScript: []string{
			"create table t (v varchar(100));",
			"insert into t values ('0'), ('0.0'), ('123'), ('123.0');",
			"create table t_idx (v varchar(100));",
			"create index idx on t_idx(v);",
			"insert into t_idx values ('0'), ('0.0'), ('123'), ('123.0');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Skip:  true,
				Query: "select * from t where (v in (0.0, 123));",
				Expected: []sql.Row{
					{"0"},
					{"0.0"},
					{"123"},
					{"123.0"},
				},
			},
			{
				Skip:  true,
				Query: "select * from t_idx where (v in (0.0, 123));",
				Expected: []sql.Row{
					{"0"},
					{"0.0"},
					{"123"},
					{"123.0"},
				},
			},
		},
	},
	{
		Name:    "between type conversion",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t0(c0 bool);",
			"create table t1(c1 bool);",
			"insert into t0 (c0) values (1);",
			"insert into t1 (c1) values (false), (true);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT t0.c0, t1.c1 FROM t0 LEFT  JOIN t1 ON true;",
				Expected: []sql.Row{
					{1, 0},
					{1, 1},
				},
			},
			{
				Query: "SELECT t0.c0, t1.c1 FROM t0 LEFT  JOIN t1 ON ('a' NOT BETWEEN false AND false) WHERE 1 UNION ALL SELECT t0.c0, t1.c1 FROM t0 LEFT  JOIN t1 ON ('a' NOT BETWEEN false AND false) WHERE (NOT 1) UNION ALL SELECT t0.c0, t1.c1 FROM t0 LEFT  JOIN t1 ON ('a' NOT BETWEEN false AND false) WHERE (1 IS NULL);",
				Expected: []sql.Row{
					{1, nil},
				},
			},
		},
	},
	{
		Name:    "bool and string",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t0(c0 BOOL, PRIMARY KEY(c0));",
			"INSERT INTO t0 (c0) VALUES (true);",
			"CREATE TABLE t1(c1 VARCHAR(500));",
			"INSERT INTO t1 (c1) VALUES (true);",
		},

		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t1, t0;",
				Expected: []sql.Row{
					{"1", 1},
				},
			},
			{
				Query: "SELECT (t1.c1 = t0.c0) FROM t1, t0;",
				Expected: []sql.Row{
					{true},
				},
			},
			{
				Query: "SELECT * FROM t1, t0 WHERE t1.c1 = t0.c0;",
				Expected: []sql.Row{
					{"1", 1},
				},
			},
		},
	},
	{
		Name:    "bool and int",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t0(c0 INTEGER, PRIMARY KEY(c0));",
			"INSERT INTO t0 (c0) VALUES (true);",
			"CREATE TABLE t1(c1 VARCHAR(500));",
			"INSERT INTO t1 (c1) VALUES (true);",
		},

		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t1, t0;",
				Expected: []sql.Row{
					{"1", 1},
				},
			},
			{
				Query: "SELECT (t1.c1 = t0.c0) FROM t1, t0;",
				Expected: []sql.Row{
					{true},
				},
			},
			{
				Query: "SELECT * FROM t1, t0 WHERE t1.c1 = t0.c0;",
				Expected: []sql.Row{
					{"1", 1},
				},
			},
		},
	},
	{
		Name:    "range query convert int to string zero value",
		Dialect: "mysql",
		SetUpScript: []string{
			`CREATE TABLE t0(c0 VARCHAR(500));`,
			`INSERT INTO t0(c0) VALUES ('a');`,
			`INSERT INTO t0(c0) VALUES ('1');`,
			`CREATE TABLE t1(c0 INTEGER, PRIMARY KEY(c0));`,
			`INSERT INTO t1(c0) VALUES (0);`,
			`INSERT INTO t1(c0) VALUES (1);`,
			`INSERT INTO t1(c0) VALUES (2);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT /*+ LOOKUP_JOIN(t0,t1) JOIN_ORDER(t0,t1) */ * FROM t1 INNER  JOIN t0 ON ((t0.c0)=(t1.c0));",
				Expected: []sql.Row{
					{0, "a"},
					{1, "1"},
				},
			},
			{
				Query: "INSERT INTO t0(c0) VALUES ('2abc');",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Skip:  true,
				Query: "SELECT /*+ LOOKUP_JOIN(t0,t1) JOIN_ORDER(t0,t1) */ * FROM t1 INNER  JOIN t0 ON ((t0.c0)=(t1.c0));",
				Expected: []sql.Row{
					{0, "a"},
					{1, "1"},
					{2, "2abc"},
				},
			},
		},
	},
}
