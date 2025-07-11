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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// FunctionQueryTests contains queries that primarily test SQL function calls
var FunctionQueryTests = []QueryTest{
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
			{64},
		},
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
}
