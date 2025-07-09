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
		Query: `SELECT HEX(WEIGHT_STRING("ABC"))`,
		Expected: []sql.Row{
			{string("006100620063")},
		},
	},
	{
		Query: `SELECT HEX(WEIGHT_STRING("abc"))`,
		Expected: []sql.Row{
			{string("006100620063")},
		},
	},
	{
		Query: `SELECT HEX(WEIGHT_STRING("A"))`,
		Expected: []sql.Row{
			{string("0061")},
		},
	},
	{
		Query: `SELECT HEX(WEIGHT_STRING(""))`,
		Expected: []sql.Row{
			{string("")},
		},
	},
	{
		Query: `SELECT HEX(WEIGHT_STRING("AB", "CHAR", 5))`,
		Expected: []sql.Row{
			{string("00610062002000200020")},
		},
	},
	{
		Query: `SELECT HEX(WEIGHT_STRING("ABCDE", "CHAR", 3))`,
		Expected: []sql.Row{
			{string("006100620063")},
		},
	},
	{
		Query: `SELECT HEX(WEIGHT_STRING("AB", "BINARY", 5))`,
		Expected: []sql.Row{
			{string("4142000000")},
		},
	},
	{
		Query: `SELECT HEX(WEIGHT_STRING("ABCDE", "BINARY", 3))`,
		Expected: []sql.Row{
			{string("414243")},
		},
	},
	{
		Query: `SELECT HEX(WEIGHT_STRING("A B"))`,
		Expected: []sql.Row{
			{string("006100200062")},
		},
	},
	{
		Query: `SELECT HEX(WEIGHT_STRING("123"))`,
		Expected: []sql.Row{
			{string("003100320033")},
		},
	},
	{
		Query: `SELECT WEIGHT_STRING(NULL)`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT HEX(WEIGHT_STRING("first row"))`,
		Expected: []sql.Row{
			{string("0066006900720073007400200072006F0077")},
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
}