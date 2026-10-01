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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
)

// ConversionsScriptTests contains self-contained script tests for conversions.
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
		Name:    "CONVERT USING still converts between incompatible character sets",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 VARCHAR(200)) COLLATE=utf8mb4_0900_ai_ci;",
			"INSERT INTO test VALUES (1, '63273াম'), (2, 'GHD30r'), (3, '8জ্রিয277'), (4, NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT pk, v1, CONVERT(CONVERT(v1 USING latin1) USING utf8mb4) AS round_trip FROM test WHERE v1 <> CONVERT(CONVERT(v1 USING latin1) USING utf8mb4);",
				Expected: []sql.Row{{int64(1), "63273াম", "63273??"}, {int64(3), "8জ্রিয277", "8?????277"}},
			},
		},
	},
	{
		Name: "Ensure proper DECIMAL support (found by fuzzer)",
		SetUpScript: []string{
			"CREATE TABLE `GzaKtwgIya` (`K7t5WY` DECIMAL(64,5), `qBjVrN` VARBINARY(1000), `PvqQtc` SET('c3q6y','kxMqhfkK','XlRI8','dF0N63H','hMPjt0KXRLwCGRr','27fi2s','1FSJ','NcPzIN','Za18lbIgxmZ','on4BKKXykVTbJ','WBfO','RMNG','Sd7','FDzbEO','cLRdLOj1y','syo4','Ul','jfsfDCx6s','yEW3','JyQcWFDl'), `1kv7el` FLOAT, `Y3vfRG` BLOB, `Ijq8CK` TINYTEXT, `tzeStN` MEDIUMINT, `Ak83FQ` BINARY(64), `8Nbp3L` DOUBLE, PRIMARY KEY (`K7t5WY`));",
			"REPLACE INTO `GzaKtwgIya` VALUES ('58567047399981325523662211357420045483361289734772861386428.89028','bvo5~Tt8%kMW2nm2!8HghaeulI6!pMadE+j-J2LeU1O1*-#@Lm8Ibh00bTYiA*H1Q8P1_kQq 24Rrd4@HeF%#7#C#U7%mqOMrQ0%!HVrGV1li.XyYa:7#3V^DtAMDTQ9 cY=07T4|DStrwy4.MAQxOG#1d#fcq+7675$y0e96-2@8-WlQ^p|%E!a^TV!Yj2_eqZZys1z:883l5I%zAT:i56K^T!cx#us $60Tb#gH$1#$P.709E#VrH9FbQ5QZK2hZUH!qUa4Xl8*I*0fT~oAha$8jU5AoWs+Uv!~:14Yq%pLXpP9RlZ:Gd1g|*$Qa.9*^K~YlYWVaxwY~_g6zOMpU$YijT+!_*m3=||cMNn#uN0!!OyCg~GTQlJ11+#@Ohqc7b#2|Jp2Aei56GOmq^I=7cQ=sQh~V.D^HzwK5~4E$QzFXfWNVN5J_w2b4dkR~bB~7F%=@R@9qE~e:-_RnoJcOLfBS@0:*hTIP$5ui|5Ea-l+qU4nx98X6rV2bLBxn8am@p~:xLF#T^_9kJVN76q^18=i *FJo.v-xA2GP==^C^Jz3yBF0OY4bIxC59Y#6G=$w:xh71kMxBcYJKf3+$Ci_uWx0P*AfFNne0_1E0Lwv#3J8vm:. 8Wo~F3VT:@w.t@w .JZz$bok9Tls7RGo=~4 Y$~iELr$s@53YuTPM8oqu!x*1%GswpJR=0K#qs00nW-1MqEUc:0wZv#X4qY^pzVDb:!:!yDhjhh+KIT%2%w@+t8c!f~o!%EnwBIr_OyzL6e1$-R8n0nWPU.toODd*|fW3H$9ZLc9!dMS:QfjI0M$nK 8aGvUVP@9kS~W#Y=Q%=37$@pAUkDTXkJo~-DRvCG6phPp*Xji@9|AEODHi+-6p%X4YM5Y3WasPHcZQ8QgTwi9 N=2RQD_MtVU~0J~3SAx*HrMlKvCPTswZq#q_96ny_A@7g!E2jyaxWFJD:C233onBdchW$WdAc.LZdZHYDR^uwZb9B9p-q.BkD1I',608583,'-7.276514330627342e-28','FN3O_E:$ 5S40T7^Vu1g!Ktn^N|4RE!9GnZiW5dG:%SJb5|SNuuI.d2^qnMY.Xn*_fRfk Eo7OhqY8OZ~pA0^ !2P.uN~r@pZ2!A0+4b*%nxO.tm%S6=$CZ9+c1zu-p $b:7:fOkC%@E3951st@2Q93~8hj:ZGeJ6S@nw-TAG+^lad37aB#xN*rD^9TO0|hleA#.Nh28S2PB72L*TxD0$|XE3S5eVVmbI*pkzE~lPecopX1fUyFj#LC+%~pjmab7^ Kdd4B%8I!ohOCQV.oiw++N|#W2=D4:_sK0@~kTTeNA8_+FMKRwro.M0| LdKHf-McKm0Z-R9+H%!9r l6%7UEB50yNH-ld%eW8!f=LKgZLc*TuTP2DA_o0izvzZokNp3ShR+PA7Fk* 1RcSt5KXe+8tLc+WGP','3RvfN2N.Q1tIffE965#2r=u_-4!u:9w!F1p7+mSsO8ckio|ib 1t@~GtgUkJX',1858932,'DJMaQcI=vS-Jk2L#^2N8qZcRpMJ2Ga!30A+@I!+35d-9bwVEVi5-~i.a%!KdoF5h','1.0354401044541863e+255');",
			"INSERT INTO `GzaKtwgIya` VALUES ('91198031969464085142628031466155813748261645250257051732159.65596','96Lu=focmodq4otVAUN6TD-F$@k^4443higo=KH!1WBDH9|vpEGdO* 1uF6yWjT4:7G|altXnWSv+d:c8Km8vL!b%-nuB8mAxO9E|a5N5#v@z!ij5ifeIEoZGXrhBJl.m*Rx-@%g~t:y$3Pp3Q7Bd3y$=YG%6yibqXWO9$SS+g=*6QzdSCzuR~@v!:.ATye0A@y~DG=uq!PaZd6wN7.2S Aq868-RN3RM61V#N+Qywqo=%iYV*554@h6GPKZ| pmNwQw=PywuyBhr*MHAOXV+u9_-#imKI-wT4gEcA1~lGg1cfL2IvhkwOXRhrjAx-8+R3#4!Ai J6SYP|YUuuGalJ_N8k_8K^~h!JyiH$0JbGQ4AOxO3-eW=BaopOd8FF1.cfFMK!tXR ^I15g:npOuZZO$Vq3yQ4bl4s$E9:t2^.4f.:I4_@u9_UI1ApBthJZNiv~o#*uhs9K@ufZ1YPJQY-pMj$v-lQ2#%=Uu!iEAO3%vQ^5YITKcWRk~$kd1H#F675r@P5#M%*F_xP3Js7$YuEC4YuQjZ A74tMw:KwQ8dR:k_ Sa85G~42-K3%:jk5G9csC@iW3nY|@-:_dg~5@J!FWF5F+nyBgz4fDpdkdk9^:_.t$A3W-C@^Ax.~o|Rq96_i%HeG*7jBjOGhY-e1k@aD@WW.@GmpGAI|T-84gZFG3BU9@#9lpL|U2YCEA.BEA%sxDZ Kw:n+d$Y!SZw0Iml$Bdtyr:02Np=DZpiI%$N9*U=%Jq#$P5BI60WOTK+UynVx9Dd**5q8y9^v+I|PPa#_2XheV5YQU.ONdQQNJxsiRaEl!*=xv4bTWj1wBH#_-eM3T',490529,'-8.419238802182018e+25','|WD!NpWJOfN+_Au 1y!|XF8l38#%%R5%$TRUEaFt%4ywKQ8 O1LD-3qRDrnHAXboH~0uivbo87f+V%=q9~Mvz1EIxsU!whSmPqtb9r*11346R_@L+H#@@Z9H-Dc6j%.D0o##m@B9o7jO#~N81ACI|f#J3z4dho:jc54Xws$8r%cxuov^1$w_58Fv2*.8qbAW$TF153A:8wwj4YIhkd#^Q7 |g7I0iQG0p+yE64rk!Pu!SA-z=ELtLNOCJBk_4!lV$izn%sB6JwM+uq~ 49I7','v|eUA_h2@%t~bn26ci8Ngjm@Lk*G=l2MhxhceV2V|ka#c',8150267,'nX-=1Q$3riw_jlukGuHmjodT_Y_SM$xRbEt$%$%hlIUF1+GpRp~U6JvRX^: k@n#','7.956726808353253e+267');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "DELETE FROM `GzaKtwgIya` WHERE `K7t5WY` = '58567047399981325523662211357420045483361289734772861386428.89028';",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT COUNT(*) FROM GzaKtwgIya",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name:    "Ensure scale is not rounded when inserting to DECIMAL type through float64",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table test (number decimal(40,16));",
			"insert into test values ('11981.5923291839784651');",
			"create table small_test (n decimal(3,2));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM test WHERE number = CONVERT('11981.5923291839784651', DECIMAL)",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "INSERT INTO test VALUES (11981.5923291839784651);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT COUNT(*) FROM test WHERE number = CONVERT('11981.5923291839784651', DECIMAL)",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "INSERT INTO test VALUES (119815923291839784651.11981592329183978465111981592329183978465144);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT COUNT(*) FROM test WHERE number = CONVERT('119815923291839784651.1198159232918398', DECIMAL)",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "INSERT INTO test VALUES (1.1981592329183978465111981592329183978465111981592329183978465144);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT COUNT(*) FROM test WHERE number = CONVERT('1.1981592329183978', DECIMAL)",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "INSERT INTO test VALUES (1.1981592329183978545111981592329183978465111981592329183978465144);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT COUNT(*) FROM test WHERE number = CONVERT('1.1981592329183979', DECIMAL)",
				Expected: []sql.Row{{1}},
			},
			{
				Query:       "INSERT INTO small_test VALUES (12.1);",
				ExpectedErr: types.ErrConvertToDecimalLimit,
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
		Name:    "failed conversion shows warning",
		Dialect: "mysql",
		Assertions: []ScriptTestAssertion{
			{
				Query:                           "SELECT CONVERT('10000-12-31 23:59:59', DATETIME)",
				ExpectedWarning:                 1292,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "Incorrect datetime value: '10000-12-31 23:59:59'",
				SkipResultsCheck:                true,
			},
			{
				Query:                           "SELECT CONVERT('this is not a datetime', DATETIME)",
				ExpectedWarning:                 1292,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "Incorrect datetime value: 'this is not a datetime'",
				SkipResultsCheck:                true,
			},
			{
				Query:                           "SELECT CAST('this is not a datetime' as DATETIME)",
				ExpectedWarning:                 1292,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "Incorrect datetime value: 'this is not a datetime'",
				SkipResultsCheck:                true,
			},
			{
				Query:                           "SELECT CONVERT('this is not a date', DATE)",
				ExpectedWarning:                 1292,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "Incorrect date value: 'this is not a date'",
				SkipResultsCheck:                true,
			},
			{
				Query:                           "SELECT CAST('this is not a date' as DATE)",
				ExpectedWarning:                 1292,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "Incorrect date value: 'this is not a date'",
				SkipResultsCheck:                true,
			},
		},
	},
	{
		Name: "sum() and avg() on DECIMAL type column returns the DECIMAL type result",
		SetUpScript: []string{
			"create table decimal_table (id int, val decimal(18,16));",
			"insert into decimal_table values (1,-2.5633000000000384);",
			"insert into decimal_table values (2,2.5633000000000370);",
			"insert into decimal_table values (3,0.0000000000000004);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT val FROM decimal_table;",
				Expected: []sql.Row{{"-2.5633000000000384"}, {"2.5633000000000370"}, {"0.0000000000000004"}},
			},
			{
				Query:    "SELECT sum(val) FROM decimal_table;",
				Expected: []sql.Row{{"-0.0000000000000010"}},
			},
			{
				Query:    "SELECT avg(val) FROM decimal_table;",
				Expected: []sql.Row{{"-0.00000000000000033333"}},
			},
		},
	},
	{
		Name: "sum() and avg() on non-DECIMAL type column returns the DOUBLE type result",
		SetUpScript: []string{
			"create table float_table (id int primary key, val1 double, val2 float);",
			"insert into float_table values (1,-2.5633000000000384, 2.3);",
			"insert into float_table values (2,2.5633000000000370, 2.4);",
			"insert into float_table values (3,0.0000000000000004, 5.3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT sum(id), sum(val1), sum(val2) FROM float_table ORDER BY id;",
				Expected: []sql.Row{{float64(6), -9.322676295501879e-16, 10.000000238418579}},
			},
			{
				Query:    "SELECT sum(id), sum(val1), sum(val2) FROM float_table ORDER BY id;",
				Expected: []sql.Row{{float64(6), -9.322676295501879e-16, 10.000000238418579}},
			},
			{
				Query:    "SELECT avg(id), avg(val1), avg(val2) FROM float_table ORDER BY id;",
				Expected: []sql.Row{{float64(2), -3.107558765167293e-16, 3.333333412806193}},
			},
		},
	},
	{
		Name: "compare DECIMAL type columns with different precision and scale",
		SetUpScript: []string{
			"create table t (id int primary key, val1 decimal(2, 1), val2 decimal(3, 1));",
			"insert into t values (1, 1.2, 1.1), (2, 1.2, 10.1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select if(val1 < val2, 'YES', 'NO') from t order by id;",
				Expected: []sql.Row{{"NO"}, {"YES"}},
			},
		},
	},
	{
		Name: "'/' division operation result in decimal or float",
		SetUpScript: []string{
			"create table floats (f float);",
			"insert into floats values (1.1), (1.2), (1.3);",
			"create table decimals (d decimal(2,1));",
			"insert into decimals values (1.0), (2.0), (2.5);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select f/2 from floats;",
				Expected: []sql.Row{{0.550000011920929}, {0.6000000238418579}, {0.6499999761581421}},
			},
			{
				Query:    "select 2/f from floats;",
				Expected: []sql.Row{{1.8181817787737895}, {1.6666666004392863}, {1.5384615948919735}},
			},
			{
				Query:    "select d/2 from decimals;",
				Expected: []sql.Row{{"0.50000"}, {"1.00000"}, {"1.25000"}},
			},
			{
				Query:    "select 2/d from decimals;",
				Expected: []sql.Row{{"2.0000"}, {"1.0000"}, {"0.8000"}},
			},
			{
				Query: "select f/d from floats, decimals;",
				Expected: []sql.Row{{1.2999999523162842}, {1.2000000476837158}, {1.100000023841858},
					{0.6499999761581421}, {0.6000000238418579}, {0.550000011920929},
					{0.5199999809265137}, {0.48000001907348633}, {0.4400000095367432}},
			},
			{
				Query: "select d/f from floats, decimals;",
				Expected: []sql.Row{{0.7692307974459868}, {0.8333333002196431}, {0.9090908893868948},
					{1.5384615948919735}, {1.6666666004392863}, {1.8181817787737895},
					{1.9230769936149668}, {2.083333250549108}, {2.272727223467237}},
			},
			{
				Dialect:  "mysql",
				Query:    `select f/'a' from floats;`,
				Expected: []sql.Row{{nil}, {nil}, {nil}},
			},
		},
	},
	{
		Name:    "'%' mod operation result in decimal or float",
		Dialect: "mysql", // % operator between types not defined in other dialects
		SetUpScript: []string{
			"create table a (pk int primary key, c1 int, c2 double, c3 decimal(5,3));",
			"insert into a values (1, 1, 1.111, 1.111), (2, 2, 2.111, 2.111);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select c1 % 2, c2 % 2, c3 % 2 from a;",
				Expected: []sql.Row{{"1", 1.111, "1.111"}, {"0", 0.11100000000000021, "0.111"}},
			},
			{
				Query:    "select c1 % 0.5, c2 % 0.5, c3 % 0.5 from a;",
				Expected: []sql.Row{{"0.0", 0.11099999999999999, "0.111"}, {"0.0", 0.11100000000000021, "0.111"}},
			},
			{
				Query:    "select 20 % c1, 20 % c2, 20 % c3 from a;",
				Expected: []sql.Row{{"0", 0.002000000000000224, "0.002"}, {"0", 1.0009999999999981, "1.001"}},
			},
		},
	},
	{
		Name: "arithmetic bit operations on int, float and decimal types",
		SetUpScript: []string{
			"CREATE TABLE num_types (pk int primary key, a int, b float, c decimal(5,3));",
			"insert into num_types values (1,1,1.1,1.1), (2,2,1.2,2.2), (3,3,1.6,3.7), (4,4,1.7,4.0);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select a & 2.4, a | 2.4, a ^ 2.4 from num_types;",
				Expected: []sql.Row{
					{uint64(0), uint64(3), uint64(3)},
					{uint64(2), uint64(2), uint64(0)},
					{uint64(2), uint64(3), uint64(1)},
					{uint64(0), uint64(6), uint64(6)},
				},
			},
			{
				Query: "select b & 2.4, b | 2.4, b ^ 2.4 from num_types;",
				Expected: []sql.Row{
					{uint64(0), uint64(3), uint64(3)},
					{uint64(0), uint64(3), uint64(3)},
					{uint64(2), uint64(2), uint64(0)},
					{uint64(2), uint64(2), uint64(0)},
				},
			},
			{
				Query: "select c & 2.4, c | 2.4, c ^ 2.4 from num_types;",
				Expected: []sql.Row{
					{uint64(0), uint64(3), uint64(3)},
					{uint64(2), uint64(2), uint64(0)},
					{uint64(0), uint64(6), uint64(6)},
					{uint64(0), uint64(6), uint64(6)},
				},
			},
		},
	},
	{
		Name: "scientific notation for floats",
		SetUpScript: []string{
			"create table t (b bigint unsigned);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "insert into t values (5.2443381514267e+18);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
		},
	},
	{
		Name: "decimal literals should be parsed correctly",
		SetUpScript: []string{
			"SET @testValue = 809826404100301269648758758005707100;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT @testValue;",
				Expected: []sql.Row{{"809826404100301269648758758005707100"}},
			},
		},
	},
	{
		Name: "division and int division operation on negative, small and big value for decimal type column of table",
		SetUpScript: []string{
			"create table t (d decimal(25,10) primary key);",
			"insert into t values (-4990), (2), (22336578);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select d div 314990 from t order by d;",
				Expected: []sql.Row{{0}, {0}, {70}},
			},
			{
				Query:    "select d / 314990 from t order by d;",
				Expected: []sql.Row{{"-0.01584177275469"}, {"0.00000634940792"}, {"70.91202260389219"}},
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
		Name: "count distinct decimals",
		SetUpScript: []string{
			"create table t (i int, j int)",
			"insert into t values (1, 11), (11, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select count(distinct i, j) from t;",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: "select count(distinct cast(i as decimal), cast(j as decimal)) from t;",
				Expected: []sql.Row{
					{2},
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
	{
		Name: "dividing has different rounding behavior",
		SetUpScript: []string{
			"CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);",
			"INSERT INTO tab0 VALUES(97, 1, 99);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT col2 IN ( 98 + col0 / 99 ) from tab0;",
				Expected: []sql.Row{
					{false},
				},
			},
			{
				Query: "SELECT col2 IN ( 98 + 97 / 99 ) from tab0;",
				Expected: []sql.Row{
					{false},
				},
			},
			{
				Query:    "SELECT * FROM tab0 WHERE col2 IN ( 98 + 97 / 99 );",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT ALL * FROM tab0 AS cor0 WHERE col2 IN ( 39 + + 89, col0 + + col1 + + ( - ( - col0 ) ) / col2, + ( col0 ) + - 99, + col1, + col2 * - + col2 * - 12 + col1 + - 66 );",
				Expected: []sql.Row{},
			},
		},
	},

	// Float Tests
	{
		Name:        "float with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table float_tbl (f float primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'f'",
			},
		},
	},

	// Decimal Tests
	{
		Name:        "decimal with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (d decimal primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'd'",
			},
			{
				Query:          "create table bad (d decimal(65,30) primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'd'",
			},
		},
	},
	{
		Name:    "decimals with foreign keys",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table parent (d decimal(4, 2) primary key);",
			"insert into parent values (1.23), (45.67), (78.9);",
			"create table parent_multi (d1 decimal(4,2), d2 decimal(3,1), primary key (d1, d2));",
			"insert into parent_multi values (1.23, 4.5), (45.67, 78.9), (99.99, 0.1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create table child_dec_4_2 (d decimal(4,2), foreign key (d) references parent (d));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "insert into child_dec_4_2 values (1.23), (45.67), (NULL);",
				Expected: []sql.Row{
					{types.NewOkResult(3)},
				},
			},
			{
				Query: "insert into child_dec_4_2 values (1.229999), (45.6711111), (78.90);",
				Expected: []sql.Row{
					{types.NewOkResult(3)},
				},
			},
			{
				Query:       "insert into child_dec_4_2 values (99.99);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "create table child_dec_4_1 (d decimal(4,1), foreign key (d) references parent (d));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child_dec_4_1 values (78.9);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:       "insert into child_dec_4_1 values (99.9);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "create table child_dec_3_2 (d decimal(3,2), foreign key (d) references parent (d));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "insert into child_dec_3_2 values (1.23);",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "create table child_dec_65_30 (d decimal(65,30), foreign key (d) references parent (d));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:       "insert into child_dec_65_30 values (1.23);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "create table child_multi_4_2_3_1 (d1 decimal(4,2), d2 decimal(3,1), foreign key (d1, d2) references parent_multi (d1, d2));",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				}},
			{
				Query: "insert into child_multi_4_2_3_1 values (1.23, 4.5), (45.67, 78.9), (NULL, NULL);",
				Expected: []sql.Row{
					{types.NewOkResult(3)},
				},
			},
			{
				Query:       "insert into child_multi_4_2_3_1 values (1.23, 9.9);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
		},
	},
	{
		Name: "decimal unique key",
		SetUpScript: []string{
			"create table t (i int primary key, d decimal(10, 2) unique)",
			"insert into t values (1, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "insert into t values (2, 1)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
		},
	},
	{
		Name: "Greatest and least with decimal arguments",
		// TODO: This should work in Doltgres https://github.com/dolthub/doltgresql/issues/2378
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t(a decimal(6, 2), b decimal(8, 5), c decimal(5, 1));",
			"insert into t values (2.75, 8.8, 3.1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// https://github.com/dolthub/dolt/issues/10562
				// https://github.com/dolthub/dolt/issues/10567
				Query:    "select greatest(a, b, c), least(a, b, c) from t;",
				Expected: []sql.Row{{"8.80000", "2.75000"}},
			},
			{
				Query: `CREATE TABLE decimal_metadata AS
SELECT
    GREATEST(a, b, c) AS g,
    LEAST(a, b, c) AS l
FROM t
WHERE FALSE`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: `SELECT
    column_name,
    column_type
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'decimal_metadata'
ORDER BY ordinal_position`,
				Expected: []sql.Row{{"g", "decimal(9,5)"}, {"l", "decimal(9,5)"}},
			},
		},
	},
	{
		Name:    "Greatest and least preserve exact decimal digits",
		Dialect: "mysql",
		SetUpScript: []string{
			`CREATE TABLE exact_values (
    id INT PRIMARY KEY,
    a DECIMAL(31, 30),
    b DECIMAL(31, 30)
)`,
			`INSERT INTO exact_values VALUES
    (1, '1.000000000000000000000000000002', '1.000000000000000000000000000001'),
    (2, '-1.000000000000000000000000000002', '-1.000000000000000000000000000001'),
    (3, NULL, '1.000000000000000000000000000001'),
    (4, '1.000000000000000000000000000002', NULL)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT
    id,
    GREATEST(a, b),
    LEAST(a, b),
    GREATEST(b, a),
    LEAST(b, a)
FROM exact_values
ORDER BY id`,
				Expected: []sql.Row{
					{
						1,
						"1.000000000000000000000000000002",
						"1.000000000000000000000000000001",
						"1.000000000000000000000000000002",
						"1.000000000000000000000000000001",
					},
					{
						2,
						"-1.000000000000000000000000000001",
						"-1.000000000000000000000000000002",
						"-1.000000000000000000000000000001",
						"-1.000000000000000000000000000002",
					},
					{3, nil, nil, nil, nil},
					{4, nil, nil, nil, nil},
				},
			},
			{
				Query: `SELECT
    GREATEST(CAST(1.25 AS DECIMAL(3, 2)), NULL),
    LEAST(NULL, CAST(1.25 AS DECIMAL(3, 2)))`,
				Expected: []sql.Row{{nil, nil}},
			},
			{
				Query: `SELECT
    GREATEST(
        1.0000000000000000000000000000000000000002,
        1.0000000000000000000000000000000000000001
    ),
    LEAST(
        1.0000000000000000000000000000000000000002,
        1.0000000000000000000000000000000000000001
    )`,
				Expected: []sql.Row{
					{
						"1.0000000000000000000000000000000000000002",
						"1.0000000000000000000000000000000000000001",
					},
				},
			},
			{
				Query: `SELECT
    GREATEST(1.0, 1.0000000000000000000000000000000000000000),
    LEAST(1.0, 1.0000000000000000000000000000000000000000)`,
				Expected: []sql.Row{
					{
						"1.0000000000000000000000000000000000000000",
						"1.0000000000000000000000000000000000000000",
					},
				},
			},
		},
	},
	{
		Name:    "Greatest and least decimal precision for integer expressions",
		Dialect: "mysql",
		SetUpScript: []string{
			`CREATE TABLE integer_values (i BIGINT, u BIGINT UNSIGNED, b BOOLEAN)`,
			`INSERT INTO integer_values VALUES (2, 2, TRUE)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT
    GREATEST(1.1, 2) AS g_literal,
    LEAST(1.1, TRUE) AS l_boolean,
    GREATEST(1.1, CAST(2 AS SIGNED)) AS g_cast,
    LEAST(1.1, CAST(2 AS UNSIGNED)) AS l_cast`,
				Expected: []sql.Row{{"2.0", "1.0", "2.0", "1.1"}},
				ExpectedColumns: sql.Schema{
					{Name: "g_literal", Type: types.MustCreateDecimalType(2, 1)},
					{Name: "l_boolean", Type: types.MustCreateDecimalType(2, 1)},
					{Name: "g_cast", Type: types.MustCreateDecimalType(21, 1)},
					{Name: "l_cast", Type: types.MustCreateDecimalType(22, 1)},
				},
			},
			{
				Query: `CREATE TABLE expression_metadata AS
SELECT
    GREATEST(1.1, 2) AS g_literal,
    LEAST(1.1, 2) AS l_literal,
    GREATEST(1.1, TRUE) AS g_boolean,
    LEAST(1.1, TRUE) AS l_boolean,
    GREATEST(1.1, CAST(2 AS SIGNED)) AS g_cast,
    LEAST(1.1, CAST(2 AS SIGNED)) AS l_cast,
    GREATEST(1.1, CAST(2 AS UNSIGNED)) AS g_unsigned_cast,
    LEAST(1.1, CAST(2 AS UNSIGNED)) AS l_unsigned_cast
FROM integer_values`,
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: `SELECT column_name, column_type
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'expression_metadata'
ORDER BY ordinal_position`,
				Expected: []sql.Row{
					{"g_literal", "decimal(2,1)"},
					{"l_literal", "decimal(2,1)"},
					{"g_boolean", "decimal(2,1)"},
					{"l_boolean", "decimal(2,1)"},
					{"g_cast", "decimal(21,1)"},
					{"l_cast", "decimal(21,1)"},
					{"g_unsigned_cast", "decimal(22,1)"},
					{"l_unsigned_cast", "decimal(22,1)"},
				},
			},
			{
				Query: `SELECT * FROM expression_metadata`,
				Expected: []sql.Row{{
					"2.0",
					"1.1",
					"1.1",
					"1.0",
					"2.0",
					"1.1",
					"2.0",
					"1.1",
				}},
			},
			{
				Query: `CREATE TABLE literal_metadata AS
SELECT
    GREATEST(1.1, 0) AS g_zero,
    LEAST(1.1, 0) AS l_zero,
    GREATEST(1.1, FALSE) AS g_false,
    LEAST(1.1, FALSE) AS l_false,
    GREATEST(1.1, -123) AS g_negative,
    LEAST(1.1, -123) AS l_negative,
    GREATEST(1.1, 123) AS g_digits,
    LEAST(1.1, 123) AS l_digits,
    GREATEST(1.1, 9223372036854775807) AS g_signed_max,
    LEAST(1.1, 9223372036854775807) AS l_signed_max,
    GREATEST(1.1, -9223372036854775808) AS g_signed_min,
    LEAST(1.1, -9223372036854775808) AS l_signed_min,
    GREATEST(1.1, 18446744073709551615) AS g_unsigned_max,
    LEAST(1.1, 18446744073709551615) AS l_unsigned_max
FROM integer_values`,
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: `SELECT column_name, column_type
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'literal_metadata'
ORDER BY ordinal_position`,
				Expected: []sql.Row{
					{"g_zero", "decimal(2,1)"},
					{"l_zero", "decimal(2,1)"},
					{"g_false", "decimal(2,1)"},
					{"l_false", "decimal(2,1)"},
					{"g_negative", "decimal(4,1)"},
					{"l_negative", "decimal(4,1)"},
					{"g_digits", "decimal(4,1)"},
					{"l_digits", "decimal(4,1)"},
					{"g_signed_max", "decimal(20,1)"},
					{"l_signed_max", "decimal(20,1)"},
					{"g_signed_min", "decimal(20,1)"},
					{"l_signed_min", "decimal(20,1)"},
					{"g_unsigned_max", "decimal(21,1)"},
					{"l_unsigned_max", "decimal(21,1)"},
				},
			},
			{
				Query: `SELECT * FROM literal_metadata`,
				Expected: []sql.Row{{
					"1.1",
					"0.0",
					"1.1",
					"0.0",
					"1.1",
					"-123.0",
					"123.0",
					"1.1",
					"9223372036854775807.0",
					"1.1",
					"1.1",
					"-9223372036854775808.0",
					"18446744073709551615.0",
					"1.1",
				}},
			},
			{
				Query: `CREATE TABLE column_metadata AS
SELECT
    GREATEST(1.1, i) AS g_signed_column,
    LEAST(1.1, i) AS l_signed_column,
    GREATEST(1.1, u) AS g_unsigned_column,
    LEAST(1.1, u) AS l_unsigned_column,
    GREATEST(1.1, b) AS g_boolean_column,
    LEAST(1.1, b) AS l_boolean_column,
    GREATEST(1.1, CAST(i AS SIGNED)) AS g_signed_cast,
    LEAST(1.1, CAST(i AS SIGNED)) AS l_signed_cast,
    GREATEST(1.1, CAST(i AS UNSIGNED)) AS g_unsigned_cast,
    LEAST(1.1, CAST(i AS UNSIGNED)) AS l_unsigned_cast
FROM integer_values`,
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: `SELECT column_name, column_type
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'column_metadata'
ORDER BY ordinal_position`,
				Expected: []sql.Row{
					{"g_signed_column", "decimal(20,1)"},
					{"l_signed_column", "decimal(20,1)"},
					{"g_unsigned_column", "decimal(21,1)"},
					{"l_unsigned_column", "decimal(21,1)"},
					{"g_boolean_column", "decimal(4,1)"},
					{"l_boolean_column", "decimal(4,1)"},
					{"g_signed_cast", "decimal(21,1)"},
					{"l_signed_cast", "decimal(21,1)"},
					{"g_unsigned_cast", "decimal(22,1)"},
					{"l_unsigned_cast", "decimal(22,1)"},
				},
			},
			{
				Query: `SELECT * FROM column_metadata`,
				Expected: []sql.Row{{
					"2.0",
					"1.1",
					"2.0",
					"1.1",
					"1.1",
					"1.0",
					"2.0",
					"1.1",
					"2.0",
					"1.1",
				}},
			},
		},
	},
	{
		Name:    "Greatest and least at the decimal precision limit",
		Dialect: "mysql",
		SetUpScript: []string{
			`CREATE TABLE wide_values (
    id INT PRIMARY KEY,
    a DECIMAL(65, 0),
    b DECIMAL(2, 1),
    c DECIMAL(31, 30)
)`,
			`INSERT INTO wide_values VALUES
    (1, '99999999999999999999999999999999999999999999999999999999999999999', 0.1, 0.1),
    (2, '-99999999999999999999999999999999999999999999999999999999999999999', -0.1, -0.1),
    (3, 1, 0.1, 0.1)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT
    id,
    GREATEST(a, b),
    LEAST(a, b),
    GREATEST(b, a),
    LEAST(b, a)
FROM wide_values
ORDER BY id`,
				Expected: []sql.Row{
					{
						1,
						"99999999999999999999999999999999999999999999999999999999999999999.0",
						"0.1",
						"99999999999999999999999999999999999999999999999999999999999999999.0",
						"0.1",
					},
					{
						2,
						"-0.1",
						"-99999999999999999999999999999999999999999999999999999999999999999.0",
						"-0.1",
						"-99999999999999999999999999999999999999999999999999999999999999999.0",
					},
					{3, "1.0", "0.1", "1.0", "0.1"},
				},
			},
			{
				Query: "SELECT id, GREATEST(a,c), LEAST(a,c) FROM wide_values ORDER BY id",
				Expected: []sql.Row{
					{
						1,
						"99999999999999999999999999999999999999999999999999999999999999999.000000000",
						"0.100000000000000000000000000000",
					},
					{
						2,
						"-0.100000000000000000000000000000",
						"-99999999999999999999999999999999999999999999999999999999999999999.000000000",
					},
					{3, "1.000000000000000000000000000000", "0.100000000000000000000000000000"},
				},
			},
			{
				Query: `CREATE TABLE wide_metadata AS
SELECT
    GREATEST(a, c) AS g,
    LEAST(a, c) AS l
FROM wide_values
WHERE FALSE`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: `SELECT
    column_name,
    column_type
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'wide_metadata'
ORDER BY ordinal_position`,
				Expected: []sql.Row{{"g", "decimal(65,30)"}, {"l", "decimal(65,30)"}},
			},
		},
	},
	{
		Name:    "Greatest and least with decimals and integer types",
		Dialect: "mysql",
		SetUpScript: []string{
			`CREATE TABLE mixed_values (
    id INT PRIMARY KEY,
    i BIGINT,
    u BIGINT UNSIGNED,
    d DECIMAL(2, 1),
    f DOUBLE
)`,
			`INSERT INTO mixed_values VALUES
    (1, -9223372036854775808, 18446744073709551615, 0.1, 1.25),
    (2, 9007199254740993, 9007199254740994, 0.1, 1.25),
    (3, 1, 2, NULL, 1.25)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT
    id,
    GREATEST(i, u, d),
    LEAST(i, u, d),
    GREATEST(d, u, i),
    LEAST(d, u, i)
FROM mixed_values
ORDER BY id`,
				Expected: []sql.Row{
					{
						1,
						"18446744073709551615.0",
						"-9223372036854775808.0",
						"18446744073709551615.0",
						"-9223372036854775808.0",
					},
					{2, "9007199254740994.0", "0.1", "9007199254740994.0", "0.1"},
					{3, nil, nil, nil, nil},
				},
			},
			{
				Query:    "SELECT id, GREATEST(d,f), LEAST(d,f) FROM mixed_values ORDER BY id",
				Expected: []sql.Row{{1, float64(1.25), float64(0.1)}, {2, float64(1.25), float64(0.1)}, {3, nil, nil}},
			},
			{
				Query: `CREATE TABLE mixed_metadata AS
SELECT
    GREATEST(i, d) AS signed_decimal,
    LEAST(u, d) AS unsigned_decimal,
    GREATEST(d, f) AS approximate
FROM mixed_values
WHERE FALSE`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: `SELECT
    column_name,
    column_type
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'mixed_metadata'
ORDER BY ordinal_position`,
				Expected: []sql.Row{
					{"signed_decimal", "decimal(20,1)"},
					{"unsigned_decimal", "decimal(21,1)"},
					{"approximate", "double"},
				},
			},
		},
	},
	{
		Name:    "Greatest and least decimal metadata across integer widths",
		Dialect: "mysql",
		SetUpScript: []string{
			`CREATE TABLE integer_values (
    i8 TINYINT,
    u8 TINYINT UNSIGNED,
    i16 SMALLINT,
    u16 SMALLINT UNSIGNED,
    i24 MEDIUMINT,
    u24 MEDIUMINT UNSIGNED,
    i32 INT,
    u32 INT UNSIGNED,
    i64 BIGINT,
    u64 BIGINT UNSIGNED,
    d DECIMAL(2, 1)
)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `CREATE TABLE integer_metadata AS
SELECT
    GREATEST(i8, d) AS i8,
    GREATEST(u8, d) AS u8,
    GREATEST(i16, d) AS i16,
    GREATEST(u16, d) AS u16,
    GREATEST(i24, d) AS i24,
    GREATEST(u24, d) AS u24,
    GREATEST(i32, d) AS i32,
    GREATEST(u32, d) AS u32,
    GREATEST(i64, d) AS i64,
    GREATEST(u64, d) AS u64
FROM integer_values`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: `SELECT
    column_name,
    column_type
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'integer_metadata'
ORDER BY ordinal_position`,
				Expected: []sql.Row{
					{"i8", "decimal(4,1)"},
					{"u8", "decimal(4,1)"},
					{"i16", "decimal(6,1)"},
					{"u16", "decimal(6,1)"},
					{"i24", "decimal(9,1)"},
					{"u24", "decimal(9,1)"},
					{"i32", "decimal(11,1)"},
					{"u32", "decimal(11,1)"},
					{"i64", "decimal(20,1)"},
					{"u64", "decimal(21,1)"},
				},
			},
		},
	},
}
