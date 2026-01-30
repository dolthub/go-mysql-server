// Copyright 2020-2022 Dolthub, Inc.
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
	"fmt"
	"time"

	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// FunctionQueryTests contains queries that primarily test SQL function calls
var FunctionQueryTests = []QueryTest{
	// Truncate function https://github.com/dolthub/dolt/issues/9916
	{
		Query: "SELECT TRUNCATE(1.223,1)",
		Expected: []sql.Row{
			{"1.2"},
		},
	},
	{
		Query: "SELECT TRUNCATE(1.999,1)",
		Expected: []sql.Row{
			{"1.9"},
		},
	},
	{
		Query: "SELECT TRUNCATE(1.999,0)",
		Expected: []sql.Row{
			{"1"},
		},
	},
	{
		Query: "SELECT TRUNCATE(-1.999,1)",
		Expected: []sql.Row{
			{"-1.9"},
		},
	},
	{
		Query: "SELECT TRUNCATE(122,-2)",
		Expected: []sql.Row{
			{100},
		},
	},
	{
		Query: "SELECT TRUNCATE(10.28*100,0)",
		Expected: []sql.Row{
			{"1028"},
		},
	},
	{
		Query: "SELECT TRUNCATE(NULL,1)",
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: "SELECT TRUNCATE(1.223,NULL)",
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: "SELECT TRUNCATE(0.5,0)",
		Expected: []sql.Row{
			{"0"},
		},
	},
	{
		Query: "SELECT TRUNCATE(-0.5,0)",
		Expected: []sql.Row{
			{"0"},
		},
	},
	{
		Query: "SELECT TRUNCATE(1.223,100)",
		Expected: []sql.Row{
			{"1.223"},
		},
	},
	{
		Query: "SELECT TRUNCATE(1.223,-100)",
		Expected: []sql.Row{
			{"0"},
		},
	},
	{
		Query: "SELECT TRUNCATE('abc',1)",
		Expected: []sql.Row{
			{0.0},
		},
	},
	{
		Query: "SELECT TRUNCATE(1.223,'xyz')",
		Expected: []sql.Row{
			{"1"},
		},
	},
	{
		Query: "SELECT TRUNCATE(1.223,1.5)",
		Expected: []sql.Row{
			{"1.22"},
		},
	},
	{
		Query: "SELECT TRUNCATE(1.223,1.7)",
		Expected: []sql.Row{
			{"1.22"},
		},
	},
	{
		Query: "SELECT TRUNCATE(1.223,0.1)",
		Expected: []sql.Row{
			{"1"},
		},
	},
	{
		Query: "SELECT TRUNCATE(1.223,0.9)",
		Expected: []sql.Row{
			{"1.2"},
		},
	},
	{
		Query: "SELECT TRUNCATE(1.223,-0.5)",
		Expected: []sql.Row{
			{"0"},
		},
	},
	{
		Query: "SELECT TRUNCATE(1.223,-0.9)",
		Expected: []sql.Row{
			{"0"},
		},
	},
	{
		Dialect: "mysql",
		Query:   "SELECT TRUNCATE('123abc',1)",
		Expected: []sql.Row{
			{123.0},
		},
		ExpectedWarning:                 mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount:           1,
		ExpectedWarningMessageSubstring: fmt.Sprintf(sql.ErrTruncatedIncorrect.Message, types.Float64.String(), "123abc"),
	},
	{
		Dialect: "mysql",
		Query:   "SELECT TRUNCATE('1.5abc',1)",
		Expected: []sql.Row{
			{1.5},
		},
		ExpectedWarning:                 mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount:           1,
		ExpectedWarningMessageSubstring: fmt.Sprintf(sql.ErrTruncatedIncorrect.Message, types.Float64.String(), "1.5abc"),
	},
	{
		Dialect: "mysql",
		Query:   "SELECT TRUNCATE('999xyz',2)",
		Expected: []sql.Row{
			{999.0},
		},
		ExpectedWarning:                 mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount:           1,
		ExpectedWarningMessageSubstring: fmt.Sprintf(sql.ErrTruncatedIncorrect.Message, types.Float64.String(), "999xyz"),
	},
	{
		Dialect: "mysql",
		Query:   "SELECT TRUNCATE(1.223,'1.5abc')",
		Expected: []sql.Row{
			{"1.2"},
		},
		ExpectedWarning:                 mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount:           2, // Both input and precision conversions generate warnings
		ExpectedWarningMessageSubstring: fmt.Sprintf(sql.ErrTruncatedIncorrect.Message, types.Int32.String(), "1.5abc"),
	},
	{
		Dialect: "mysql",
		Query:   "SELECT TRUNCATE(1.223,'0.5')",
		Expected: []sql.Row{
			{"1"},
		},
		ExpectedWarning:                 mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount:           2, // Both input and precision conversions generate warnings
		ExpectedWarningMessageSubstring: fmt.Sprintf(sql.ErrTruncatedIncorrect.Message, types.Int32.String(), "0.5"),
	},
	{
		Dialect: "mysql",
		Query:   "SELECT TRUNCATE(1.223,'2.7')",
		Expected: []sql.Row{
			{"1.22"},
		},
		ExpectedWarning:                 mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount:           2, // Both input and precision conversions generate warnings
		ExpectedWarningMessageSubstring: fmt.Sprintf(sql.ErrTruncatedIncorrect.Message, types.Int32.String(), "2.7"),
	},
	{
		Dialect: "mysql",
		Query:   "SELECT TRUNCATE(1.223,'invalid_precision')",
		Expected: []sql.Row{
			{"1"},
		},
		ExpectedWarning:                 mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount:           2, // Both input and precision conversions generate warnings
		ExpectedWarningMessageSubstring: fmt.Sprintf(sql.ErrTruncatedIncorrect.Message, types.Int32.String(), "invalid_precision"),
	},
	{
		Query:          "SELECT TRUNCATE()",
		ExpectedErr:    sql.ErrInvalidArgumentNumber,
		ExpectedErrStr: fmt.Sprintf(sql.ErrInvalidArgumentNumber.Message, function.TruncateFunctionName, 2, 0),
	},
	{
		Query:          "SELECT TRUNCATE(1)",
		ExpectedErr:    sql.ErrInvalidArgumentNumber,
		ExpectedErrStr: fmt.Sprintf(sql.ErrInvalidArgumentNumber.Message, function.TruncateFunctionName, 2, 1),
	},
	{
		Query:          "SELECT TRUNCATE(1,2,3)",
		ExpectedErr:    sql.ErrInvalidArgumentNumber,
		ExpectedErrStr: fmt.Sprintf(sql.ErrInvalidArgumentNumber.Message, function.TruncateFunctionName, 2, 3),
	},
	// String Functions
	{
		Query: `SELECT CONCAT("a", "b", "c")`,
		Expected: []sql.Row{
			{string("abc")},
		},
	},
	{
		Query: `SELECT INSERT("Quadratic", 3, 4, "What")`,
		Expected: []sql.Row{
			{string("QuWhattic")},
		},
	},
	{
		Query: `SELECT INSERT("hello", 2, 2, "xyz")`,
		Expected: []sql.Row{
			{string("hxyzlo")},
		},
	},
	{
		Query: `SELECT INSERT("hello", 1, 2, "xyz")`,
		Expected: []sql.Row{
			{string("xyzllo")},
		},
	},
	{
		Query: `SELECT INSERT("hello", 5, 1, "xyz")`,
		Expected: []sql.Row{
			{string("hellxyz")},
		},
	},
	{
		Query: `SELECT INSERT("hello", 1, 5, "world")`,
		Expected: []sql.Row{
			{string("world")},
		},
	},
	{
		Query: `SELECT INSERT("hello", 3, 10, "world")`,
		Expected: []sql.Row{
			{string("heworld")},
		},
	},
	{
		Query: `SELECT INSERT("hello", 2, 2, "")`,
		Expected: []sql.Row{
			{string("hlo")},
		},
	},
	{
		Query: `SELECT INSERT("hello", 3, 0, "xyz")`,
		Expected: []sql.Row{
			{string("hexyzllo")},
		},
	},
	{
		Query: `SELECT INSERT("hello", 0, 2, "xyz")`,
		Expected: []sql.Row{
			{string("hello")},
		},
	},
	{
		Query: `SELECT INSERT("hello", -1, 2, "xyz")`,
		Expected: []sql.Row{
			{string("hello")},
		},
	},
	{
		Query: `SELECT INSERT("hello", 1, -1, "xyz")`,
		Expected: []sql.Row{
			{string("xyz")},
		},
	},
	{
		Query: `SELECT INSERT("hello", 3, -1, "xyz")`,
		Expected: []sql.Row{
			{string("hexyz")},
		},
	},
	{
		Query: `SELECT INSERT("hello", 2, 100, "xyz")`,
		Expected: []sql.Row{
			{string("hxyz")},
		},
	},
	{
		Query: `SELECT INSERT("hello", 1, 50, "world")`,
		Expected: []sql.Row{
			{string("world")},
		},
	},
	{
		Query: `SELECT INSERT("hello", 10, 2, "xyz")`,
		Expected: []sql.Row{
			{string("hello")},
		},
	},
	{
		Query: `SELECT INSERT("", 1, 2, "xyz")`,
		Expected: []sql.Row{
			{string("")},
		},
	},
	{
		Query: `SELECT INSERT(NULL, 1, 2, "xyz")`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT INSERT("hello", NULL, 2, "xyz")`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT INSERT("hello", 1, NULL, "xyz")`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT INSERT("hello", 1, 2, NULL)`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT COALESCE(NULL, NULL, NULL, 'example', NULL, 1234567890)`,
		Expected: []sql.Row{
			{string("example")},
		},
	},
	{
		Query: `SELECT COALESCE(NULL, NULL, NULL, COALESCE(NULL, 1234567890))`,
		Expected: []sql.Row{
			{int32(1234567890)},
		},
	},
	{
		Query:    "SELECT COALESCE (NULL, NULL)",
		Expected: []sql.Row{{nil}},
		ExpectedColumns: []*sql.Column{
			{
				Name: "COALESCE (NULL, NULL)",
				Type: types.Null,
			},
		},
	},
	{
		Query: `SELECT COALESCE(CAST('{"a": "one \\n two"}' as json), '');`,
		Expected: []sql.Row{
			{"{\"a\": \"one \\n two\"}"},
		},
	},
	{
		Query: "SELECT concat(s, i) FROM mytable",
		Expected: []sql.Row{
			{string("first row1")},
			{string("second row2")},
			{string("third row3")},
		},
	},
	{
		Query: `SELECT INSERT(s, 1, 5, "new") FROM mytable ORDER BY i`,
		Expected: []sql.Row{
			{string("new row")},
			{string("newd row")},
			{string("new row")},
		},
	},
	{
		Query: `SELECT INSERT(s, i, 2, "XY") FROM mytable ORDER BY i`,
		Expected: []sql.Row{
			{string("XYrst row")},
			{string("sXYond row")},
			{string("thXYd row")},
		},
	},
	{
		Query: `SELECT INSERT(s, i + 1, i, UPPER(s)) FROM mytable ORDER BY i`,
		Expected: []sql.Row{
			{string("fFIRST ROWrst row")},
			{string("seSECOND ROWnd row")},
			{string("thiTHIRD ROWrow")},
		},
	},
	{
		Query: `SELECT EXPORT_SET(5, "Y", "N", ",", 4)`,
		Expected: []sql.Row{
			{string("Y,N,Y,N")},
		},
	},
	{
		Query: `SELECT EXPORT_SET(6, "1", "0", ",", 10)`,
		Expected: []sql.Row{
			{string("0,1,1,0,0,0,0,0,0,0")},
		},
	},
	{
		Query: `SELECT EXPORT_SET(0, "1", "0", ",", 4)`,
		Expected: []sql.Row{
			{string("0,0,0,0")},
		},
	},
	{
		Query: `SELECT EXPORT_SET(15, "1", "0", ",", 4)`,
		Expected: []sql.Row{
			{string("1,1,1,1")},
		},
	},
	{
		Query: `SELECT EXPORT_SET(1, "T", "F", ",", 3)`,
		Expected: []sql.Row{
			{string("T,F,F")},
		},
	},
	{
		Query: `SELECT EXPORT_SET(5, "1", "0", "|", 4)`,
		Expected: []sql.Row{
			{string("1|0|1|0")},
		},
	},
	{
		Query: `SELECT EXPORT_SET(5, "1", "0", "", 4)`,
		Expected: []sql.Row{
			{string("1010")},
		},
	},
	{
		Query: `SELECT EXPORT_SET(5, "ON", "OFF", ",", 4)`,
		Expected: []sql.Row{
			{string("ON,OFF,ON,OFF")},
		},
	},
	{
		Query: `SELECT EXPORT_SET(255, "1", "0", ",", 8)`,
		Expected: []sql.Row{
			{string("1,1,1,1,1,1,1,1")},
		},
	},
	{
		Query: `SELECT EXPORT_SET(1024, "1", "0", ",", 12)`,
		Expected: []sql.Row{
			{string("0,0,0,0,0,0,0,0,0,0,1,0")},
		},
	},
	{
		Query: `SELECT EXPORT_SET(5, "1", "0")`,
		Expected: []sql.Row{
			{string("1,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0")},
		},
	},
	{
		Query: `SELECT EXPORT_SET(5, "1", "0", ",", 1)`,
		Expected: []sql.Row{
			{string("1")},
		},
	},
	{
		Query: `SELECT EXPORT_SET(-1, "1", "0", ",", 4)`,
		Expected: []sql.Row{
			{string("1,1,1,1")},
		},
	},
	{
		Query: `SELECT EXPORT_SET(NULL, "1", "0", ",", 4)`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT EXPORT_SET(5, NULL, "0", ",", 4)`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT EXPORT_SET(5, "1", NULL, ",", 4)`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT EXPORT_SET(5, "1", "0", NULL, 4)`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT EXPORT_SET(5, "1", "0", ",", NULL)`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT EXPORT_SET("5", "1", "0", ",", 4)`,
		Expected: []sql.Row{
			{string("1,0,1,0")},
		},
	},
	{
		Query: `SELECT EXPORT_SET(5.7, "1", "0", ",", 4)`,
		Expected: []sql.Row{
			{string("0,1,1,0")},
		},
	},
	{
		Query: `SELECT EXPORT_SET(i, "1", "0", ",", 4) FROM mytable ORDER BY i`,
		Expected: []sql.Row{
			{string("1,0,0,0")},
			{string("0,1,0,0")},
			{string("1,1,0,0")},
		},
	},
	{
		Query: `SELECT MAKE_SET(1, "a", "b", "c")`,
		Expected: []sql.Row{
			{string("a")},
		},
	},
	{
		Query: `SELECT MAKE_SET(1 | 4, "hello", "nice", "world")`,
		Expected: []sql.Row{
			{string("hello,world")},
		},
	},
	{
		Query: `SELECT MAKE_SET(0, "a", "b", "c")`,
		Expected: []sql.Row{
			{string("")},
		},
	},
	{
		Query: `SELECT MAKE_SET(3, "a", "b", "c")`,
		Expected: []sql.Row{
			{string("a,b")},
		},
	},
	{
		Query: `SELECT MAKE_SET(5, "a", "b", "c")`,
		Expected: []sql.Row{
			{string("a,c")},
		},
	},
	{
		Query: `SELECT MAKE_SET(7, "a", "b", "c")`,
		Expected: []sql.Row{
			{string("a,b,c")},
		},
	},
	{
		Query: `SELECT MAKE_SET(1024, "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")`,
		Expected: []sql.Row{
			{string("k")},
		},
	},
	{
		Query: `SELECT MAKE_SET(1025, "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")`,
		Expected: []sql.Row{
			{string("a,k")},
		},
	},
	{
		Query: `SELECT MAKE_SET(7, "a", NULL, "c")`,
		Expected: []sql.Row{
			{string("a,c")},
		},
	},
	{
		Query: `SELECT MAKE_SET(7, NULL, "b", "c")`,
		Expected: []sql.Row{
			{string("b,c")},
		},
	},
	{
		Query: `SELECT MAKE_SET(NULL, "a", "b", "c")`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT MAKE_SET("5", "a", "b", "c")`,
		Expected: []sql.Row{
			{string("a,c")},
		},
	},
	{
		Query: `SELECT MAKE_SET(5.7, "a", "b", "c")`,
		Expected: []sql.Row{
			{string("b,c")},
		},
	},
	{
		Query: `SELECT MAKE_SET(-1, "a", "b", "c")`,
		Expected: []sql.Row{
			{string("a,b,c")},
		},
	},
	{
		Query: `SELECT MAKE_SET(16, "a", "b", "c")`,
		Expected: []sql.Row{
			{string("")},
		},
	},
	{
		Query: `SELECT MAKE_SET(3, "", "test", "")`,
		Expected: []sql.Row{
			{string(",test")},
		},
	},
	{
		Query: `SELECT MAKE_SET(i, "first", "second", "third") FROM mytable ORDER BY i`,
		Expected: []sql.Row{
			{string("first")},
			{string("second")},
			{string("first,second")},
		},
	},
	{
		Query: "SELECT version()",
		Expected: []sql.Row{
			{"8.0.31"},
		},
	},
	{
		Query: `SELECT RAND(100)`,
		Expected: []sql.Row{
			{float64(0.8165026937796166)},
		},
	},
	{
		Query:    `SELECT RAND(i) from mytable order by i`,
		Expected: []sql.Row{{0.6046602879796196}, {0.16729663442585624}, {0.7199826688373036}},
	},
	{
		Query: `SELECT RAND(100) = RAND(100)`,
		Expected: []sql.Row{
			{true},
		},
	},
	{
		Query: `SELECT RAND() = RAND()`,
		Expected: []sql.Row{
			{false},
		},
	},
	{
		Query: "SELECT MOD(i, 2) from mytable order by i limit 1",
		Expected: []sql.Row{
			{"1"},
		},
	},
	{
		Query: "SELECT SIN(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{0.8414709848078965},
		},
	},
	{
		Query: "SELECT COS(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{0.5403023058681398},
		},
	},
	{
		Query: "SELECT TAN(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{1.557407724654902},
		},
	},
	{
		Query: "SELECT ASIN(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{1.5707963267948966},
		},
	},
	{
		Query: "SELECT ACOS(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{0.0},
		},
	},
	{
		Query: "SELECT ATAN(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{0.7853981633974483},
		},
	},
	{
		Query: "SELECT COT(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{0.6420926159343308},
		},
	},
	{
		Query: "SELECT DEGREES(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{57.29577951308232},
		},
	},
	{
		Query: "SELECT RADIANS(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{0.017453292519943295},
		},
	},
	{
		Query: "SELECT CRC32(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{uint64(0x83dcefb7)},
		},
	},
	{
		Query: "SELECT SIGN(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{1},
		},
	},
	{
		Query: "SELECT ASCII(s) from mytable order by i limit 1",
		Expected: []sql.Row{
			{uint64(0x66)},
		},
	},
	{
		Query: "SELECT HEX(s) from mytable order by i limit 1",
		Expected: []sql.Row{
			{"666972737420726F77"},
		},
	},
	{
		Query: "SELECT UNHEX(s) from mytable order by i limit 1",
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: "SELECT BIN(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{"1"},
		},
	},
	{
		Query: "SELECT BIT_LENGTH(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{8},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9818
		Query:    "select bit_length(10)",
		Expected: []sql.Row{{16}},
	},
	{
		Query:    "select bit_length(now())",
		Expected: []sql.Row{{152}},
	},
	{
		Query:    "select bit_length(-10)",
		Expected: []sql.Row{{24}},
	},
	{
		Query:    "select bit_length(true)",
		Expected: []sql.Row{{8}},
	},
	{
		Query: "select date_format(datetime_col, '%D') from datetime_table order by 1",
		Expected: []sql.Row{
			{"1st"},
			{"4th"},
			{"7th"},
		},
	},
	{
		Query: "select time_format(time_col, '%h%p') from datetime_table order by 1",
		Expected: []sql.Row{
			{"03AM"},
			{"03PM"},
			{"04AM"},
		},
	},
	{
		Query: "select from_unixtime(i) from mytable order by 1",
		Expected: []sql.Row{
			{UnixTimeInLocal(1, 0)},
			{UnixTimeInLocal(2, 0)},
			{UnixTimeInLocal(3, 0)},
		},
	},

	// FORMAT Function Tests
	{
		Query: `SELECT FORMAT(val, 2) FROM
			(values row(4328904), row(432053.4853), row(5.93288775208e+08), row("5784029.372"), row(-4229842.122), row(-0.009)) a (val)`,
		Expected: []sql.Row{
			{"4,328,904.00"},
			{"432,053.49"},
			{"593,288,775.21"},
			{"5,784,029.37"},
			{"-4,229,842.12"},
			{"-0.01"},
		},
	},
	{
		Query: "SELECT FORMAT(i, 3) FROM mytable;",
		Expected: []sql.Row{
			{"1.000"},
			{"2.000"},
			{"3.000"},
		},
	},
	{
		Query: `SELECT FORMAT(val, 2, 'da_DK') FROM
			(values row(4328904), row(432053.4853), row(5.93288775208e+08), row("5784029.372"), row(-4229842.122), row(-0.009)) a (val)`,
		Expected: []sql.Row{
			{"4.328.904,00"},
			{"432.053,49"},
			{"593.288.775,21"},
			{"5.784.029,37"},
			{"-4.229.842,12"},
			{"-0,01"},
		},
	},
	{
		Query: "SELECT FORMAT(i, 3, 'da_DK') FROM mytable;",
		Expected: []sql.Row{
			{"1,000"},
			{"2,000"},
			{"3,000"},
		},
	},

	// Date/Time Function Tests
	{
		Query: "SELECT DATEDIFF(date_col, '2019-12-28') FROM datetime_table where date_col = date('2019-12-31T12:00:00');",
		Expected: []sql.Row{
			{3},
		},
	},
	{
		Query: `SELECT DATEDIFF(val, '2019/12/28') FROM
			(values row('2017-11-30 22:59:59'), row('2020/01/02'), row('2021-11-30'), row('2020-12-31T12:00:00')) a (val)`,
		Expected: []sql.Row{
			{-758},
			{5},
			{703},
			{369},
		},
	},
	{
		Query: "SELECT TIMESTAMPDIFF(SECOND,'2007-12-31 23:59:58', '2007-12-31 00:00:00');",
		Expected: []sql.Row{
			{-86398},
		},
	},
	{
		Query: `SELECT TIMESTAMPDIFF(MINUTE, val, '2019/12/28') FROM
			(values row('2017-11-30 22:59:59'), row('2020/01/02'), row('2019-12-27 23:15:55'), row('2019-12-31T12:00:00')) a (val);`,
		Expected: []sql.Row{
			{1090140},
			{-7200},
			{44},
			{-5040},
		},
	},
	{
		Query:    "SELECT TIMEDIFF(null, '2017-11-30 22:59:59');",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT DATEDIFF('2019/12/28', null);",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT TIMESTAMPDIFF(SECOND, null, '2007-12-31 00:00:00');",
		Expected: []sql.Row{{nil}},
	},
	// https://github.com/dolthub/dolt/issues/10393
	{
		Query:    "SELECT TIMESTAMPDIFF(YEAR, DATE '2011-07-05', DATE '2026-07-04')",
		Expected: []sql.Row{{14}},
	},
	{
		Query:    "SELECT TIMESTAMPDIFF(YEAR, DATE '2026-07-04', DATE '2011-07-05')",
		Expected: []sql.Row{{-14}},
	},
	{
		Query:    "SELECT TIMESTAMPDIFF(YEAR, DATE '2026-07-05', DATE '2026-07-04')",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "SELECT TIMESTAMPDIFF(YEAR, DATE '2026-07-04', DATE '2026-07-05')",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "SELECT TIMESTAMPDIFF(YEAR, DATE '2025-07-04', DATE '2026-07-03')",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "SELECT TIMESTAMPDIFF(YEAR, DATE '2026-07-03', DATE '2025-07-04')",
		Expected: []sql.Row{{0}},
	},
	// TRIM Function Tests
	{
		Query:    `SELECT TRIM(mytable.s) AS s FROM mytable`,
		Expected: []sql.Row{{"first row"}, {"second row"}, {"third row"}},
	},
	{
		Query:    `SELECT TRIM("row" from mytable.s) AS s FROM mytable`,
		Expected: []sql.Row{{"first "}, {"second "}, {"third "}},
	},
	{
		Query:    `SELECT TRIM(mytable.s from "first row") AS s FROM mytable`,
		Expected: []sql.Row{{""}, {"first row"}, {"first row"}},
	},
	{
		Query:    `SELECT TRIM(mytable.s from mytable.s) AS s FROM mytable`,
		Expected: []sql.Row{{""}, {""}, {""}},
	},
	{
		Query:    `SELECT TRIM("   foo   ")`,
		Expected: []sql.Row{{"foo"}},
	},
	{
		Query:    `SELECT TRIM(" " FROM "   foo   ")`,
		Expected: []sql.Row{{"foo"}},
	},
	{
		Query:    `SELECT TRIM(LEADING " " FROM "   foo   ")`,
		Expected: []sql.Row{{"foo   "}},
	},
	{
		Query:    `SELECT TRIM(TRAILING " " FROM "   foo   ")`,
		Expected: []sql.Row{{"   foo"}},
	},
	{
		Query:    `SELECT TRIM(BOTH " " FROM "   foo   ")`,
		Expected: []sql.Row{{"foo"}},
	},
	{
		Query:    `SELECT TRIM("" FROM " foo")`,
		Expected: []sql.Row{{" foo"}},
	},
	{
		Query:    `SELECT TRIM("bar" FROM "barfoobar")`,
		Expected: []sql.Row{{"foo"}},
	},
	{
		Query:    `SELECT TRIM(TRAILING "bar" FROM "barfoobar")`,
		Expected: []sql.Row{{"barfoo"}},
	},
	{
		Query:    `SELECT TRIM(TRAILING "foo" FROM "foo")`,
		Expected: []sql.Row{{""}},
	},
	{
		Query:    `SELECT TRIM(LEADING "ooo" FROM TRIM("oooo"))`,
		Expected: []sql.Row{{"o"}},
	},
	{
		Query:    `SELECT TRIM(BOTH "foo" FROM TRIM("barfoobar"))`,
		Expected: []sql.Row{{"barfoobar"}},
	},
	{
		Query:    `SELECT TRIM(LEADING "bar" FROM TRIM("foobar"))`,
		Expected: []sql.Row{{"foobar"}},
	},
	{
		Query:    `SELECT TRIM(TRAILING "oo" FROM TRIM("oof"))`,
		Expected: []sql.Row{{"oof"}},
	},
	{
		Query:    `SELECT TRIM(LEADING "test" FROM TRIM("  test  "))`,
		Expected: []sql.Row{{""}},
	},
	{
		Query:    `SELECT TRIM(LEADING CONCAT("a", "b") FROM TRIM("ababab"))`,
		Expected: []sql.Row{{""}},
	},
	{
		Query:    `SELECT TRIM(TRAILING CONCAT("a", "b") FROM CONCAT("test","ab"))`,
		Expected: []sql.Row{{"test"}},
	},
	{
		Query:    `SELECT TRIM(LEADING 1 FROM "11111112")`,
		Expected: []sql.Row{{"2"}},
	},
	{
		Query:    `SELECT TRIM(LEADING 1 FROM 11111112)`,
		Expected: []sql.Row{{"2"}},
	},

	// SUBSTRING_INDEX Function Tests
	{
		Query: `SELECT SUBSTRING_INDEX('a.b.c.d.e.f', '.', 2)`,
		Expected: []sql.Row{
			{"a.b"},
		},
	},
	{
		Query: `SELECT SUBSTRING_INDEX('a.b.c.d.e.f', '.', -2)`,
		Expected: []sql.Row{
			{"e.f"},
		},
	},
	{
		Query: `SELECT SUBSTRING_INDEX(SUBSTRING_INDEX('source{d}', '{d}', 1), 'r', -1)`,
		Expected: []sql.Row{
			{"ce"},
		},
	},
	{
		Query:    `SELECT SUBSTRING_INDEX(mytable.s, "d", 1) AS s FROM mytable INNER JOIN othertable ON (SUBSTRING_INDEX(mytable.s, "d", 1) = SUBSTRING_INDEX(othertable.s2, "d", 1)) GROUP BY 1 HAVING s = 'secon';`,
		Expected: []sql.Row{{"secon"}},
	},
	{
		Query:    `SELECT SUBSTRING_INDEX(mytable.s, "d", 1) AS s FROM mytable INNER JOIN othertable ON (SUBSTRING_INDEX(mytable.s, "d", 1) = SUBSTRING_INDEX(othertable.s2, "d", 1)) GROUP BY s HAVING s = 'secon';`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT SUBSTRING_INDEX(mytable.s, "d", 1) AS ss FROM mytable INNER JOIN othertable ON (SUBSTRING_INDEX(mytable.s, "d", 1) = SUBSTRING_INDEX(othertable.s2, "d", 1)) GROUP BY s HAVING s = 'secon';`,
		Expected: []sql.Row{},
	},
	{
		Query: `SELECT SUBSTRING_INDEX(mytable.s, "d", 1) AS ss FROM mytable INNER JOIN othertable ON (SUBSTRING_INDEX(mytable.s, "d", 1) = SUBSTRING_INDEX(othertable.s2, "d", 1)) GROUP BY ss HAVING ss = 'secon';`,
		Expected: []sql.Row{
			{"secon"},
		},
	},

	// INET Function Tests
	{
		Query:    `SELECT INET_ATON("10.0.5.10")`,
		Expected: []sql.Row{{uint64(167773450)}},
	},
	{
		Query:    `SELECT INET_NTOA(167773450)`,
		Expected: []sql.Row{{"10.0.5.10"}},
	},
	{
		Query:    `SELECT INET_ATON("10.0.5.11")`,
		Expected: []sql.Row{{uint64(167773451)}},
	},
	{
		Query:    `SELECT INET_NTOA(167773451)`,
		Expected: []sql.Row{{"10.0.5.11"}},
	},
	{
		Query:    `SELECT INET_NTOA(INET_ATON("12.34.56.78"))`,
		Expected: []sql.Row{{"12.34.56.78"}},
	},
	{
		Query:    `SELECT INET_ATON(INET_NTOA("12345678"))`,
		Expected: []sql.Row{{uint64(12345678)}},
	},
	{
		Query:    `SELECT INET_ATON("notanipaddress")`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT INET_NTOA("spaghetti")`,
		Expected: []sql.Row{{"0.0.0.0"}},
	},

	// INET6 Function Tests
	{
		Query:    `SELECT HEX(INET6_ATON("10.0.5.9"))`,
		Expected: []sql.Row{{"0A000509"}},
	},
	{
		Query:    `SELECT HEX(INET6_ATON("::10.0.5.9"))`,
		Expected: []sql.Row{{"0000000000000000000000000A000509"}},
	},
	{
		Query:    `SELECT HEX(INET6_ATON("1.2.3.4"))`,
		Expected: []sql.Row{{"01020304"}},
	},
	{
		Query:    `SELECT HEX(INET6_ATON("fdfe::5455:caff:fefa:9098"))`,
		Expected: []sql.Row{{"FDFE0000000000005455CAFFFEFA9098"}},
	},
	{
		Query:    `SELECT HEX(INET6_ATON("1111:2222:3333:4444:5555:6666:7777:8888"))`,
		Expected: []sql.Row{{"11112222333344445555666677778888"}},
	},
	{
		Query:    `SELECT INET6_ATON("notanipaddress")`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT INET6_NTOA(UNHEX("1234ffff5678ffff1234ffff5678ffff"))`,
		Expected: []sql.Row{{"1234:ffff:5678:ffff:1234:ffff:5678:ffff"}},
	},
	{
		Query:    `SELECT INET6_NTOA(UNHEX("ffffffff"))`,
		Expected: []sql.Row{{"255.255.255.255"}},
	},
	{
		Query:    `SELECT INET6_NTOA(UNHEX("000000000000000000000000ffffffff"))`,
		Expected: []sql.Row{{"::255.255.255.255"}},
	},
	{
		Query:    `SELECT INET6_NTOA(UNHEX("00000000000000000000ffffffffffff"))`,
		Expected: []sql.Row{{"::ffff:255.255.255.255"}},
	},
	{
		Query:    `SELECT INET6_NTOA(UNHEX("0000000000000000000000000000ffff"))`,
		Expected: []sql.Row{{"::ffff"}},
	},
	{
		Query:    `SELECT INET6_NTOA(UNHEX("00000000000000000000000000000000"))`,
		Expected: []sql.Row{{"::"}},
	},
	{
		Query:    `SELECT INET6_NTOA("notanipaddress")`,
		Expected: []sql.Row{{nil}},
	},

	// IS_IPV4/IS_IPV6 Function Tests
	{
		Query:    `SELECT IS_IPV4("10.0.1.10")`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT IS_IPV4("::10.0.1.10")`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT IS_IPV4("notanipaddress")`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT IS_IPV6("10.0.1.10")`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT IS_IPV6("::10.0.1.10")`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT IS_IPV6("notanipaddress")`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT IS_IPV4_COMPAT(INET6_ATON("10.0.1.10"))`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT IS_IPV4_COMPAT(INET6_ATON("::10.0.1.10"))`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT IS_IPV4_COMPAT(INET6_ATON("::ffff:10.0.1.10"))`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT IS_IPV4_COMPAT(INET6_ATON("notanipaddress"))`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT IS_IPV4_MAPPED(INET6_ATON("10.0.1.10"))`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT IS_IPV4_MAPPED(INET6_ATON("::10.0.1.10"))`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT IS_IPV4_MAPPED(INET6_ATON("::ffff:10.0.1.10"))`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT IS_IPV4_COMPAT(INET6_ATON("notanipaddress"))`,
		Expected: []sql.Row{{nil}},
	},

	// Additional Date/Time Function Tests
	{
		Query:    "SELECT YEAR('2007-12-11') FROM mytable",
		Expected: []sql.Row{{int32(2007)}, {int32(2007)}, {int32(2007)}},
	},
	{
		Query:    "SELECT MONTH('2007-12-11') FROM mytable",
		Expected: []sql.Row{{int32(12)}, {int32(12)}, {int32(12)}},
	},
	{
		Query:    "SELECT DAY('2007-12-11') FROM mytable",
		Expected: []sql.Row{{int32(11)}, {int32(11)}, {int32(11)}},
	},
	{
		Query:    "SELECT HOUR('2007-12-11 20:21:22') FROM mytable",
		Expected: []sql.Row{{int32(20)}, {int32(20)}, {int32(20)}},
	},
	{
		Query:    "SELECT MINUTE('2007-12-11 20:21:22') FROM mytable",
		Expected: []sql.Row{{int32(21)}, {int32(21)}, {int32(21)}},
	},
	{
		Query:    "SELECT SECOND('2007-12-11 20:21:22') FROM mytable",
		Expected: []sql.Row{{int32(22)}, {int32(22)}, {int32(22)}},
	},
	{
		Query:    "SELECT DAYOFYEAR('2007-12-11 20:21:22') FROM mytable",
		Expected: []sql.Row{{int32(345)}, {int32(345)}, {int32(345)}},
	},
	{
		Query:    "SELECT SECOND('2007-12-11T20:21:22Z') FROM mytable",
		Expected: []sql.Row{{int32(22)}, {int32(22)}, {int32(22)}},
	},
	{
		Query:    "SELECT DAYOFYEAR('2007-12-11') FROM mytable",
		Expected: []sql.Row{{int32(345)}, {int32(345)}, {int32(345)}},
	},
	{
		Query:    "SELECT DAYOFYEAR('20071211') FROM mytable",
		Expected: []sql.Row{{int32(345)}, {int32(345)}, {int32(345)}},
	},
	{
		Query:    "SELECT YEARWEEK('0000-01-01')",
		Expected: []sql.Row{{int32(1)}},
	},
	{
		Query:    "SELECT YEARWEEK('9999-12-31')",
		Expected: []sql.Row{{int32(999952)}},
	},
	{
		Query:    "SELECT YEARWEEK('2008-02-20', 1)",
		Expected: []sql.Row{{int32(200808)}},
	},
	{
		Query:    "SELECT YEARWEEK('1987-01-01')",
		Expected: []sql.Row{{int32(198652)}},
	},
	{
		Query:    "SELECT YEARWEEK('1987-01-01', 20), YEARWEEK('1987-01-01', 1), YEARWEEK('1987-01-01', 2), YEARWEEK('1987-01-01', 3), YEARWEEK('1987-01-01', 4), YEARWEEK('1987-01-01', 5), YEARWEEK('1987-01-01', 6), YEARWEEK('1987-01-01', 7)",
		Expected: []sql.Row{{int32(198653), int32(198701), int32(198652), int32(198701), int32(198653), int32(198652), int32(198653), int32(198652)}},
	},

	// Additional String Function Tests
	{
		Query:    `SELECT CHAR_LENGTH('áé'), LENGTH('àè')`,
		Expected: []sql.Row{{int32(2), int32(4)}},
	},
	{
		Query:    `SELECT SUBSTR(SUBSTRING('0123456789ABCDEF', 1, 10), -4)`,
		Expected: []sql.Row{{"6789"}},
	},

	// Math Function Tests
	{
		Query: `SELECT FLOOR(15728640/1024/1030)`,
		Expected: []sql.Row{
			{14},
		},
	},
	{
		Query: `SELECT ROUND(15728640/1024/1030)`,
		Expected: []sql.Row{
			{"15"},
		},
	},
	{
		Query: `SELECT ROUND(15.00, 1)`,
		Expected: []sql.Row{
			{"15.0"},
		},
	},
	{
		Query: `SELECT round(15, 1)`,
		Expected: []sql.Row{
			{int8(15)},
		},
	},
	{
		Query:    "SELECT POW(2,3) FROM dual",
		Expected: []sql.Row{{float64(8)}},
	},

	// JSON Function Tests
	{
		Query: `SELECT JSON_MERGE_PRESERVE('{ "a": 1, "b": 2 }','{ "a": 3, "c": 4 }','{ "a": 5, "d": 6 }')`,
		Expected: []sql.Row{
			{types.MustJSON(`{"a": [1, 3, 5], "b": 2, "c": 4, "d": 6}`)},
		},
	},
	{
		Query: `SELECT JSON_MERGE_PRESERVE(val1, val2)
	              FROM (values
						 row('{ "a": 1, "b": 2 }','null'),
	                   row('{ "a": 1, "b": 2 }','"row one"'),
	                   row('{ "a": 3, "c": 4 }','4'),
	                   row('{ "a": 5, "d": 6 }','[true, true]'),
	                   row('{ "a": 5, "d": 6 }','{ "a": 3, "e": 2 }'))
	              test (val1, val2)`,
		Expected: []sql.Row{
			{types.MustJSON(`[{ "a": 1, "b": 2 }, null]`)},
			{types.MustJSON(`[{ "a": 1, "b": 2 }, "row one"]`)},
			{types.MustJSON(`[{ "a": 3, "c": 4 }, 4]`)},
			{types.MustJSON(`[{ "a": 5, "d": 6 }, true, true]`)},
			{types.MustJSON(`{ "a": [5, 3], "d": 6, "e": 2}`)},
		},
	},
	{
		Query: `SELECT JSON_ARRAY()`,
		Expected: []sql.Row{
			{types.MustJSON(`[]`)},
		},
	},
	{
		Query: `SELECT JSON_ARRAY('{"b": 2, "a": [1, 8], "c": null}', null, 4, '[true, false]', "do")`,
		Expected: []sql.Row{
			{types.MustJSON(`["{\"b\": 2, \"a\": [1, 8], \"c\": null}", null, 4, "[true, false]", "do"]`)},
		},
	},
	{
		Query: `SELECT JSON_ARRAY(1, 'say, "hi"', JSON_OBJECT("abc", 22))`,
		Expected: []sql.Row{
			{types.MustJSON(`[1, "say, \"hi\"", {"abc": 22}]`)},
		},
	},
	{
		Query: `SELECT JSON_ARRAY(JSON_OBJECT("a", JSON_ARRAY(1,2)), JSON_OBJECT("b", 22))`,
		Expected: []sql.Row{
			{types.MustJSON(`[{"a": [1, 2]}, {"b": 22}]`)},
		},
	},
	{
		Query: `SELECT JSON_ARRAY(pk, c1, c2, c3) FROM jsontable`,
		Expected: []sql.Row{
			{types.MustJSON(`[1, "row one", [1, 2], {"a": 2}]`)},
			{types.MustJSON(`[2, "row two", [3, 4], {"b": 2}]`)},
			{types.MustJSON(`[3, "row three", [5, 6], {"c": 2}]`)},
			{types.MustJSON(`[4, "row four", [7, 8], {"d": 2}]`)},
		},
	},
	{
		Query: `SELECT JSON_ARRAY(JSON_OBJECT("id", pk, "name", c1), c2, c3) FROM jsontable`,
		Expected: []sql.Row{
			{types.MustJSON(`[{"id": 1,"name": "row one"}, [1, 2], {"a": 2}]`)},
			{types.MustJSON(`[{"id": 2,"name": "row two"}, [3, 4], {"b": 2}]`)},
			{types.MustJSON(`[{"id": 3,"name": "row three"}, [5, 6], {"c": 2}]`)},
			{types.MustJSON(`[{"id": 4,"name": "row four"}, [7, 8], {"d": 2}]`)},
		},
	},
	{
		Query: `SELECT JSON_KEYS(c3) FROM jsontable`,
		Expected: []sql.Row{
			{types.MustJSON(`["a"]`)},
			{types.MustJSON(`["b"]`)},
			{types.MustJSON(`["c"]`)},
			{types.MustJSON(`["d"]`)},
		},
	},
	{
		Query: `SELECT JSON_OVERLAPS(c3, '{"a": 2, "d": 2}') FROM jsontable`,
		Expected: []sql.Row{
			{true},
			{false},
			{false},
			{true},
		},
	},
	{
		Query: `SELECT JSON_MERGE(c3, '{"a": 1}') FROM jsontable`,
		Expected: []sql.Row{
			{types.MustJSON(`{"a": [2, 1]}`)},
			{types.MustJSON(`{"a": 1, "b": 2}`)},
			{types.MustJSON(`{"a": 1, "c": 2}`)},
			{types.MustJSON(`{"a": 1, "d": 2}`)},
		},
	},
	{
		Query: `SELECT JSON_MERGE_PRESERVE(c3, '{"a": 1}') FROM jsontable`,
		Expected: []sql.Row{
			{types.MustJSON(`{"a": [2, 1]}`)},
			{types.MustJSON(`{"a": 1, "b": 2}`)},
			{types.MustJSON(`{"a": 1, "c": 2}`)},
			{types.MustJSON(`{"a": 1, "d": 2}`)},
		},
	},
	{
		Query: `SELECT JSON_MERGE_PATCH(c3, '{"a": 1}') FROM jsontable`,
		Expected: []sql.Row{
			{types.MustJSON(`{"a": 1}`)},
			{types.MustJSON(`{"a": 1, "b": 2}`)},
			{types.MustJSON(`{"a": 1, "c": 2}`)},
			{types.MustJSON(`{"a": 1, "d": 2}`)},
		},
	},
	{
		Query: `SELECT CONCAT(JSON_OBJECT('aa', JSON_OBJECT('bb', 123, 'y', 456), 'z', JSON_OBJECT('cc', 321, 'x', 654)), "")`,
		Expected: []sql.Row{
			{`{"z": {"x": 654, "cc": 321}, "aa": {"y": 456, "bb": 123}}`},
		},
	},
	{
		Query: `SELECT CONCAT(JSON_ARRAY(JSON_OBJECT('aa', 123, 'z', 456), JSON_OBJECT('BB', 321, 'Y', 654)), "")`,
		Expected: []sql.Row{
			{`[{"z": 456, "aa": 123}, {"Y": 654, "BB": 321}]`},
		},
	},

	// Conversion Function Tests
	{
		Query: "SELECT CAST(-3 AS UNSIGNED) FROM mytable",
		Expected: []sql.Row{
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
		},
	},
	{
		Query:    "SELECT CAST(-3 AS DOUBLE) FROM dual",
		Expected: []sql.Row{{-3.0}},
	},
	{
		Query:    `SELECT CONVERT("-3.9876", FLOAT) FROM dual`,
		Expected: []sql.Row{{float32(-3.9876)}},
	},
	{
		Query:    "SELECT CAST(10.56789 as CHAR(3));",
		Expected: []sql.Row{{"10."}},
	},
	{
		Query:    "SELECT CAST(10.56789 as CHAR(30));",
		Expected: []sql.Row{{"10.56789"}},
	},
	{
		Query:    "SELECT CAST('abcdef' as BINARY(10));",
		Expected: []sql.Row{{[]byte("abcdef\x00\x00\x00\x00")}},
	},
	{
		Query:    `SELECT CONVERT(10.12345, DECIMAL(4,2))`,
		Expected: []sql.Row{{"10.12"}},
	},
	{
		Query:    `SELECT CONVERT(1234567893.1234567893, DECIMAL(20,10))`,
		Expected: []sql.Row{{"1234567893.1234567893"}},
	},
	{
		Query:    `SELECT CONVERT(10, DECIMAL(4,2))`,
		Expected: []sql.Row{{"10.00"}},
	},

	// Additional JSON Function Tests
	{
		Query:    `SELECT JSON_UNQUOTE('"foo"')`,
		Expected: []sql.Row{{"foo"}},
	},
	{
		Query:    `SELECT JSON_UNQUOTE('[1, 2, 3]')`,
		Expected: []sql.Row{{"[1, 2, 3]"}},
	},
	{
		Query:    `SELECT JSON_UNQUOTE('"\\t\\u0032"')`,
		Expected: []sql.Row{{"\t2"}},
	},
	{
		Query:    `SELECT JSON_UNQUOTE('"\t\\u0032"')`,
		Expected: []sql.Row{{"\t2"}},
	},
	{
		Query:    `SELECT JSON_UNQUOTE(JSON_EXTRACT('{"xid":"hello"}', '$.xid')) = "hello"`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT JSON_QUOTE('"foo"')`,
		Expected: []sql.Row{{`"\"foo\""`}},
	},
	{
		Query:    `SELECT JSON_QUOTE('[1, 2, 3]')`,
		Expected: []sql.Row{{`"[1, 2, 3]"`}},
	},
	{
		Query:    `SELECT JSON_QUOTE('"\t\u0032"')`,
		Expected: []sql.Row{{`"\"\tu0032\""`}},
	},
	{
		Query:    `SELECT JSON_QUOTE('"\t\\u0032"')`,
		Expected: []sql.Row{{`"\"\t\\u0032\""`}},
	},
	{
		Query:    `SELECT JSON_EXTRACT('{"xid":"hello"}', '$.xid') = "hello"`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT JSON_EXTRACT('{"xid":"hello"}', '$.xid') = '"hello"'`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT JSON_UNQUOTE(JSON_EXTRACT('{"xid":null}', '$.xid'))`,
		Expected: []sql.Row{{"null"}},
	},

	// Utility Function Tests
	{
		Query: `SELECT DATABASE()`,
		Expected: []sql.Row{
			{"mydb"},
		},
	},
	{
		Query: `SELECT USER()`,
		Expected: []sql.Row{
			{"root@localhost"},
		},
	},
	{
		Query: `SELECT CURRENT_USER()`,
		Expected: []sql.Row{
			{"root@localhost"},
		},
	},
	{
		Query: `SELECT CURRENT_USER`,
		Expected: []sql.Row{
			{"root@localhost"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "CURRENT_USER",
				Type: types.LongText,
			},
		},
	},
	{
		Query:    "SELECT SLEEP(0.5)",
		Expected: []sql.Row{{int(0)}},
	},

	// Encoding Function Tests
	{
		Query:    "SELECT TO_BASE64('foo')",
		Expected: []sql.Row{{string("Zm9v")}},
	},
	{
		Query:    "SELECT FROM_BASE64('YmFy')",
		Expected: []sql.Row{{[]byte("bar")}},
	},

	// Comparison Function Tests
	{
		Query:    `SELECT GREATEST(1, 2, "3", 4)`,
		Expected: []sql.Row{{float64(4)}},
	},
	{
		Query:    `SELECT GREATEST(1, 2, "9", "foo999")`,
		Expected: []sql.Row{{float64(9)}},
	},
	{
		Query:    `SELECT GREATEST("aaa", "bbb", "ccc")`,
		Expected: []sql.Row{{"ccc"}},
	},
	{
		Query:    `SELECT GREATEST(i, s) FROM mytable`,
		Expected: []sql.Row{{float64(1)}, {float64(2)}, {float64(3)}},
	},
	{
		Query:    `SELECT GREATEST(1, 2, 3, 4)`,
		Expected: []sql.Row{{int64(4)}},
	},
	{
		Query:    `SELECT LEAST(1, 2, 3, 4)`,
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    `SELECT LEAST(1, 2, "3", 4)`,
		Expected: []sql.Row{{float64(1)}},
	},
	{
		Query:    `SELECT LEAST(1, 2, "9", "foo999")`,
		Expected: []sql.Row{{float64(1)}},
	},
	{
		Query:    `SELECT LEAST("aaa", "bbb", "ccc")`,
		Expected: []sql.Row{{"aaa"}},
	},
	{
		Query:    `SELECT LEAST(i, s) FROM mytable`,
		Expected: []sql.Row{{float64(1)}, {float64(2)}, {float64(3)}},
	},
	{
		Query:    `SELECT LEAST(CAST("1920-02-03 07:41:11" AS DATETIME), CAST("1980-06-22 14:32:56" AS DATETIME))`,
		Expected: []sql.Row{{time.Date(1920, 2, 3, 7, 41, 11, 0, time.UTC)}},
	},

	// Additional Math Function Tests
	{
		Query:    "select abs(-i) from mytable order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	// https://github.com/dolthub/dolt/issues/9735
	{
		Query:                 "select log('10asdf', '100f')",
		Expected:              []sql.Row{{float64(2)}},
		ExpectedWarningsCount: 2,
	},
	{
		Query:                 "select log('a10asdf', 'b100f')",
		Expected:              []sql.Row{{nil}},
		ExpectedWarningsCount: 2,
	},
	// https://github.com/dolthub/dolt/issues/10171
	{
		Query:                 "select abs('hi')",
		Expected:              []sql.Row{{float64(0)}},
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select abs('12.3hi')",
		Expected:              []sql.Row{{12.3}},
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select abs('-342.12hi')",
		Expected:              []sql.Row{{342.12}},
		ExpectedWarningsCount: 1,
	},
	// https://github.com/dolthub/dolt/issues/10270
	{
		Query:    "select abs(1 and true)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select abs(true)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select abs(2 and true)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select abs(false)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select abs(false or 2)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select abs(date('2020-12-15'))",
		Expected: []sql.Row{{float64(20201215)}},
		// https://github.com/dolthub/dolt/issues/10278
		Skip: true,
	},
	{
		Query:    "select abs(time('12:23:43'))",
		Expected: []sql.Row{{float64(122343)}},
		// https://github.com/dolthub/dolt/issues/10278
		Skip: true,
	},
	// Date Manipulation Function Tests
	{
		Query:    "SELECT TIMESTAMPADD(DAY, 1, '2018-05-02')",
		Expected: []sql.Row{{"2018-05-03"}},
	},
	{
		Query:                 "select timestampadd(day, 1, '0000-00-00')",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select timestampadd(day, 1, 0)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:    "SELECT DATE_ADD('2018-05-02', INTERVAL 1 day)",
		Expected: []sql.Row{{"2018-05-03"}},
	},
	{
		Query:    "SELECT DATE_ADD(DATE('2018-05-02'), INTERVAL 1 day)",
		Expected: []sql.Row{{time.Date(2018, time.May, 3, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    "select date_add(time('12:13:14'), interval 1 minute);",
		Expected: []sql.Row{{types.Timespan(44054000000)}},
	},
	{
		Query:                 "select date_add(0, interval 1 day)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select date_sub(0, interval 1 day)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:    "SELECT DATE_SUB('2018-05-02', INTERVAL 1 DAY)",
		Expected: []sql.Row{{"2018-05-01"}},
	},
	{
		Query:    "SELECT DATE_SUB(DATE('2018-05-02'), INTERVAL 1 DAY)",
		Expected: []sql.Row{{time.Date(2018, time.May, 1, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    "select date_sub(time('12:13:14'), interval 1 minute);",
		Expected: []sql.Row{{types.Timespan(43934000000)}},
	},
	{
		Query:    "SELECT '2018-05-02' + INTERVAL 1 DAY",
		Expected: []sql.Row{{time.Date(2018, time.May, 3, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    "SELECT '2018-05-02' - INTERVAL 1 DAY",
		Expected: []sql.Row{{time.Date(2018, time.May, 1, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    "SELECT CONVERT('9999-12-31 23:59:59', DATETIME)",
		Expected: []sql.Row{{time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)}},
	},
	{
		Query:    "SELECT DATETIME('9999-12-31 23:59:59')",
		Expected: []sql.Row{{time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)}},
	},
	{
		Query:    "SELECT TIMESTAMP('2020-12-31 23:59:59')",
		Expected: []sql.Row{{time.Date(2020, time.December, 31, 23, 59, 59, 0, time.UTC)}},
	},
	{
		Query:    "SELECT CONVERT('10000-12-31 23:59:59', DATETIME)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT '9999-12-31 23:59:59' + INTERVAL 1 DAY",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT DATE_ADD('9999-12-31 23:59:59', INTERVAL 1 DAY)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:                 "select 0 + interval 1 day",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select 0 - interval 1 day",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select datediff(0, '2020-10-10')",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select datediff('0000-00-00', '2020-10-10')",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select datediff('2020-10-10', 0)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select datediff('2020-10-10', '0000-00-00')",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:    "SELECT EXTRACT(DAY FROM '9999-12-31 23:59:59')",
		Expected: []sql.Row{{31}},
	},

	// String Search Function Tests
	{
		Query:    "SELECT instr(s, 'row') as l FROM mytable ORDER BY i",
		Expected: []sql.Row{{int64(7)}, {int64(8)}, {int64(7)}},
	},
	{
		Query:    "SELECT instr(s, 'first') as l FROM mytable ORDER BY i",
		Expected: []sql.Row{{int64(1)}, {int64(0)}, {int64(0)}},
	},
	{
		Query:    "SELECT instr(s, 'o') as l FROM mytable ORDER BY i",
		Expected: []sql.Row{{int64(8)}, {int64(4)}, {int64(8)}},
	},
	{
		Query:    "SELECT instr(s, NULL) as l FROM mytable ORDER BY l",
		Expected: []sql.Row{{nil}, {nil}, {nil}},
	},
	{
		Query:    `select locate("o", s) from mytable order by i`,
		Expected: []sql.Row{{8}, {4}, {8}},
	},
	{
		Query:    `select locate("o", s, 5) from mytable order by i`,
		Expected: []sql.Row{{8}, {9}, {8}},
	},
	{
		Query:    `select locate(upper("roW"), upper(s), power(10, 0)) from mytable order by i`,
		Expected: []sql.Row{{7}, {8}, {7}},
	},
	{
		Query: "select find_in_set('second row', s) from mytable;",
		Expected: []sql.Row{
			{0},
			{1},
			{0},
		},
	},
	{
		Query: "select find_in_set(s, 'first row,second row,third row') from mytable;",
		Expected: []sql.Row{
			{1},
			{2},
			{3},
		},
	},

	// Additional Math Function Tests
	{
		Query:    "select log2(i) from mytable order by i",
		Expected: []sql.Row{{0.0}, {1.0}, {1.5849625007211563}},
	},

	// UUID and Compression Function Tests
	{
		Query: `select uuid() = uuid()`,
		Expected: []sql.Row{
			{false},
		},
	},
	{
		Query:    `select instr(REPLACE(CONVERT(UUID() USING utf8mb4), '-', ''), '-')`,
		Expected: []sql.Row{{0}},
	},
	{
		Query: "select uncompress(compress('thisisastring'))",
		Expected: []sql.Row{
			{[]byte{0x74, 0x68, 0x69, 0x73, 0x69, 0x73, 0x61, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67}},
		},
	},
	{
		Query: "select length(compress(repeat('a', 1000)))",
		Expected: []sql.Row{
			{24}, // 21 in MySQL because of library implementation differences
		},
	},
	{
		Query: "select length(uncompress(compress(repeat('a', 1000))))",
		Expected: []sql.Row{
			{1000},
		},
	},
	{
		Query: "select uncompressed_length(compress(repeat('a', 1000)))",
		Expected: []sql.Row{
			{uint32(1000)},
		},
	},
	// date-related functions
	{
		Query:    "select day(0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select day(false)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:                 "select day(true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:    "select day('0000-00-00')",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select day('0000-01-01')",
		Expected: []sql.Row{{1}},
	},
	{
		Query:                 "select dayname(0)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select dayname(false)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select dayname(true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select dayname('0000-00-00')",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query: "select dayname('0000-01-01')",
		// This is Sunday in MySQL. It seems like Go's time library considers 0000-02-29 a valid date but MySQL does
		// not. This is why the days of the week are off. 0000 is not a real year anyway. This test is to make sure
		// 0000-01-01 is not interpreted as zero time
		Expected: []sql.Row{{"Saturday"}},
	},
	{
		Query:    "select dayname('2025-11-13')",
		Expected: []sql.Row{{"Thursday"}},
	},
	{
		Query:    "select dayofmonth(0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select dayofmonth(false)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:                 "select dayofmonth(true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:    "select dayofmonth('0000-00-00')",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select dayofmonth('0000-01-01')",
		Expected: []sql.Row{{1}},
	},
	{
		Query:                 "select dayofweek(0)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select dayofweek(false)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select dayofweek(true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select dayofweek('0000-00-00')",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query: "select dayofweek('0000-01-01')",
		// This is 1 (Sunday) in MySQL. It seems like Go's time library considers 0000-02-29 a valid date but MySQL does
		// not. This is why the days of the week are off. 0000 is not a real year anyway. This test is to make sure
		// 0000-01-01 is not interpreted as zero time
		Expected: []sql.Row{{7}},
	},
	{
		Query:    "select dayofweek('2025-11-13')",
		Expected: []sql.Row{{5}},
	},
	{
		Query:                 "select dayofyear(0)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select dayofyear(false)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select dayofyear(true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select dayofyear('0000-00-00')",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:    "select dayofyear('0000-01-01')",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select month(0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select month(false)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:                 "select month(true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:    "select month('0000-00-00')",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select month('0000-01-01')",
		Expected: []sql.Row{{1}},
	},
	{
		Query:                 "select monthname(0)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select monthname(false)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select monthname(true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select monthname('0000-00-00')",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:    "select monthname('0000-01-01')",
		Expected: []sql.Row{{"January"}},
	},
	{
		Query:                 "select week(0)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select week(false)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select week(true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select week('0000-00-00')",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:    "select week('0000-01-01')",
		Expected: []sql.Row{{1}},
	},
	{
		Query:                 "select weekday(0)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select weekday(false)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select weekday(true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select weekday('0000-00-00')",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query: "select weekday('0000-01-01')",
		// This is 6 (Sunday) in MySQL. It seems like Go's time library considers 0000-02-29 a valid date but MySQL does
		// not. This is why the days of the week are off. 0000 is not a real year anyway. This test is to make sure
		// 0000-01-01 is not interpreted as zero time
		Expected: []sql.Row{{5}},
	},
	{
		Query:    "select weekday('2025-11-13')",
		Expected: []sql.Row{{3}},
	},
	{
		Query:                 "select weekofyear(0)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select weekofyear(false)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select weekofyear(true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select weekofyear('0000-00-00')",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:    "select weekofyear('0000-01-01')",
		Expected: []sql.Row{{52}},
	},
	{
		Query:                 "select yearweek(0)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select yearweek(false)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select yearweek(true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:                 "select yearweek('0000-00-00')",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:    "select yearweek('0000-01-01')",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select quarter(0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select quarter(false)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:                 "select quarter(true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:    "select quarter('0000-00-00')",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select quarter('0000-01-01')",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select date('0000-01-01')",
		Expected: []sql.Row{{"0000-01-01"}},
	},
	{
		Query:    "select date('0000-00-00')",
		Expected: []sql.Row{{"0000-00-00"}},
	},
	{
		Query:    "select date(0)",
		Expected: []sql.Row{{"0000-00-00"}},
	},
	{
		Query:    "select date(false)",
		Expected: []sql.Row{{"0000-00-00"}},
	},
	{
		Query:                 "select date(true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:    "select extract(day from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(day from false)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:                 "select extract(day from true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query: "select extract(week from 0)",
		// This is 613566757 in MySQL but that value seems related to this bug https://bugs.mysql.com/bug.php?id=71414&files=1
		Expected: []sql.Row{{1}},
	},
	{
		Query: "select extract(week from false)",
		// This is 613566757 in MySQL but that value seems related to this bug https://bugs.mysql.com/bug.php?id=71414&files=1
		Expected: []sql.Row{{1}},
	},
	{
		Query:                 "select extract(week from true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:    "select extract(month from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(month from false)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:                 "select extract(month from true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:    "select extract(quarter from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(quarter from false)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:                 "select extract(quarter from true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:    "select extract(year from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(year from false)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:                 "select extract(year from true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:    "select extract(year_month from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(year_month from false)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:                 "select extract(year_month from true)",
		Expected:              []sql.Row{{nil}},
		ExpectedWarning:       mysql.ERTruncatedWrongValue,
		ExpectedWarningsCount: 1,
	},
	{
		Query:    "select extract(day_microsecond from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(day_microsecond from false)",
		Expected: []sql.Row{{0}},
	},
	{
		// https://github.com/dolthub/dolt/issues/10087
		Skip:     true,
		Query:    "select extract(day_microsecond from true)",
		Expected: []sql.Row{{1000000}},
	},
	{
		Query:    "select extract(day_second from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(day_second from false)",
		Expected: []sql.Row{{0}},
	},
	{
		// https://github.com/dolthub/dolt/issues/10087
		Skip:     true,
		Query:    "select extract(day_second from true)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select extract(day_minute from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(day_minute from false)",
		Expected: []sql.Row{{0}},
	},
	{
		// https://github.com/dolthub/dolt/issues/10087
		Skip:     true,
		Query:    "select extract(day_minute from true)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(day_hour from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(day_hour from false)",
		Expected: []sql.Row{{0}},
	},
	{
		// https://github.com/dolthub/dolt/issues/10087
		Skip:     true,
		Query:    "select extract(day_hour from true)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(second_microsecond from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(second_microsecond from false)",
		Expected: []sql.Row{{0}},
	},
	{
		// https://github.com/dolthub/dolt/issues/10087
		Skip:     true,
		Query:    "select extract(second_microsecond from true)",
		Expected: []sql.Row{{1000000}},
	},
	{
		Query:    "select extract(minute_microsecond from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(minute_microsecond from false)",
		Expected: []sql.Row{{0}},
	},
	{
		// https://github.com/dolthub/dolt/issues/10087
		Skip:     true,
		Query:    "select extract(minute_microsecond from true)",
		Expected: []sql.Row{{1000000}},
	},
	{
		Query:    "select extract(minute_second from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(minute_second from false)",
		Expected: []sql.Row{{0}},
	},
	{
		// https://github.com/dolthub/dolt/issues/10087
		Skip:     true,
		Query:    "select extract(minute_second from true)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select extract(hour_microsecond from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(hour_microsecond from false)",
		Expected: []sql.Row{{0}},
	},
	{
		// https://github.com/dolthub/dolt/issues/10087
		Skip:     true,
		Query:    "select extract(hour_microsecond from true)",
		Expected: []sql.Row{{1000000}},
	},
	{
		Query:    "select extract(hour_second from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(hour_second from false)",
		Expected: []sql.Row{{0}},
	},
	{
		// https://github.com/dolthub/dolt/issues/10087
		Skip:     true,
		Query:    "select extract(hour_second from true)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select extract(hour_minute from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(hour_minute from false)",
		Expected: []sql.Row{{0}},
	},
	{
		// https://github.com/dolthub/dolt/issues/10087
		Skip:     true,
		Query:    "select extract(hour_minute from true)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(microsecond from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(microsecond from false)",
		Expected: []sql.Row{{0}},
	},
	{
		// https://github.com/dolthub/dolt/issues/10087
		Skip:     true,
		Query:    "select extract(microsecond from true)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(second from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(second from false)",
		Expected: []sql.Row{{0}},
	},
	{
		// https://github.com/dolthub/dolt/issues/10087
		Skip:     true,
		Query:    "select extract(second from true)",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select extract(minute from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(minute from false)",
		Expected: []sql.Row{{0}},
	},
	{
		// https://github.com/dolthub/dolt/issues/10087
		Skip:     true,
		Query:    "select extract(minute from true)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(hour from 0)",
		Expected: []sql.Row{{0}},
	},
	{
		Query:    "select extract(hour from false)",
		Expected: []sql.Row{{0}},
	},
	{
		// https://github.com/dolthub/dolt/issues/10087
		Skip:     true,
		Query:    "select extract(hour from true)",
		Expected: []sql.Row{{0}},
	},
}
