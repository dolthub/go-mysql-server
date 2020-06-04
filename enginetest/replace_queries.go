// Copyright 2020 Liquidata, Inc.
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

package enginetest

import (
	"github.com/liquidata-inc/go-mysql-server/sql"
	"math"
)

// TODO: none of these tests insert into tables without primary key columns, which have different semantics for
//  REPLACE INTO queries. Add some tables / data without primary keys.
var ReplaceQueries = []WriteQueryTest{
	{
		"REPLACE INTO mytable VALUES (1, 'first row');",
		[]sql.Row{{sql.NewOkResult(2)}},
		"SELECT s FROM mytable WHERE i = 1;",
		[]sql.Row{{"first row"}},
	},
	{
		"REPLACE INTO mytable SET i = 1, s = 'first row';",
		[]sql.Row{{sql.NewOkResult(2)}},
		"SELECT s FROM mytable WHERE i = 1;",
		[]sql.Row{{"first row"}},
	},
	{
		"REPLACE INTO mytable VALUES (1, 'new row same i');",
		[]sql.Row{{sql.NewOkResult(2)}},
		"SELECT s FROM mytable WHERE i = 1;",
		[]sql.Row{{"new row same i"}},
	},
	{
		"REPLACE INTO mytable SET i = 1, s = 'new row same i';",
		[]sql.Row{{sql.NewOkResult(2)}},
		"SELECT s FROM mytable WHERE i = 1;",
		[]sql.Row{{"new row same i"}},
	},
	{
		"REPLACE INTO mytable (s, i) VALUES ('x', 999);",
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT i FROM mytable WHERE s = 'x';",
		[]sql.Row{{int64(999)}},
	},
	{
		"REPLACE INTO mytable SET s = 'x', i = 999;",
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT i FROM mytable WHERE s = 'x';",
		[]sql.Row{{int64(999)}},
	},
	{
		"REPLACE INTO mytable VALUES (999, 'x');",
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT i FROM mytable WHERE s = 'x';",
		[]sql.Row{{int64(999)}},
	},
	{
		"REPLACE INTO mytable SET i = 999, s = 'x';",
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT i FROM mytable WHERE s = 'x';",
		[]sql.Row{{int64(999)}},
	},
	{
		`REPLACE INTO typestable VALUES (
			999, 127, 32767, 2147483647, 9223372036854775807,
			255, 65535, 4294967295, 18446744073709551615,
			3.40282346638528859811704183484516925440e+38, 1.797693134862315708145274237317043567981e+308,
			'2037-04-05 12:51:36', '2231-11-07',
			'random text', true, '{"key":"value"}', 'blobdata'
			);`,
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT * FROM typestable WHERE id = 999;",
		[]sql.Row{{
			int64(999), int8(math.MaxInt8), int16(math.MaxInt16), int32(math.MaxInt32), int64(math.MaxInt64),
			uint8(math.MaxUint8), uint16(math.MaxUint16), uint32(math.MaxUint32), uint64(math.MaxUint64),
			float32(math.MaxFloat32), float64(math.MaxFloat64),
			sql.Timestamp.MustConvert("2037-04-05 12:51:36"), sql.Date.MustConvert("2231-11-07"),
			"random text", sql.True, ([]byte)(`{"key":"value"}`), "blobdata",
		}},
	},
	{
		`REPLACE INTO typestable SET
			id = 999, i8 = 127, i16 = 32767, i32 = 2147483647, i64 = 9223372036854775807,
			u8 = 255, u16 = 65535, u32 = 4294967295, u64 = 18446744073709551615,
			f32 = 3.40282346638528859811704183484516925440e+38, f64 = 1.797693134862315708145274237317043567981e+308,
			ti = '2037-04-05 12:51:36', da = '2231-11-07',
			te = 'random text', bo = true, js = '{"key":"value"}', bl = 'blobdata'
			;`,
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT * FROM typestable WHERE id = 999;",
		[]sql.Row{{
			int64(999), int8(math.MaxInt8), int16(math.MaxInt16), int32(math.MaxInt32), int64(math.MaxInt64),
			uint8(math.MaxUint8), uint16(math.MaxUint16), uint32(math.MaxUint32), uint64(math.MaxUint64),
			float32(math.MaxFloat32), float64(math.MaxFloat64),
			sql.Timestamp.MustConvert("2037-04-05 12:51:36"), sql.Date.MustConvert("2231-11-07"),
			"random text", sql.True, ([]byte)(`{"key":"value"}`), "blobdata",
		}},
	},
	{
		`REPLACE INTO typestable VALUES (
			999, -128, -32768, -2147483648, -9223372036854775808,
			0, 0, 0, 0,
			1.401298464324817070923729583289916131280e-45, 4.940656458412465441765687928682213723651e-324,
			'0000-00-00 00:00:00', '0000-00-00',
			'', false, '', ''
			);`,
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT * FROM typestable WHERE id = 999;",
		[]sql.Row{{
			int64(999), int8(-math.MaxInt8 - 1), int16(-math.MaxInt16 - 1), int32(-math.MaxInt32 - 1), int64(-math.MaxInt64 - 1),
			uint8(0), uint16(0), uint32(0), uint64(0),
			float32(math.SmallestNonzeroFloat32), float64(math.SmallestNonzeroFloat64),
			sql.Timestamp.Zero(), sql.Date.Zero(),
			"", sql.False, ([]byte)(`""`), "",
		}},
	},
	{
		`REPLACE INTO typestable SET
			id = 999, i8 = -128, i16 = -32768, i32 = -2147483648, i64 = -9223372036854775808,
			u8 = 0, u16 = 0, u32 = 0, u64 = 0,
			f32 = 1.401298464324817070923729583289916131280e-45, f64 = 4.940656458412465441765687928682213723651e-324,
			ti = '0000-00-00 00:00:00', da = '0000-00-00',
			te = '', bo = false, js = '', bl = ''
			;`,
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT * FROM typestable WHERE id = 999;",
		[]sql.Row{{
			int64(999), int8(-math.MaxInt8 - 1), int16(-math.MaxInt16 - 1), int32(-math.MaxInt32 - 1), int64(-math.MaxInt64 - 1),
			uint8(0), uint16(0), uint32(0), uint64(0),
			float32(math.SmallestNonzeroFloat32), float64(math.SmallestNonzeroFloat64),
			sql.Timestamp.Zero(), sql.Date.Zero(),
			"", sql.False, ([]byte)(`""`), "",
		}},
	},
	{
		`REPLACE INTO typestable VALUES (999, null, null, null, null, null, null, null, null,
			null, null, null, null, null, null, null, null);`,
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT * FROM typestable WHERE id = 999;",
		[]sql.Row{{int64(999), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}},
	},
	{
		`REPLACE INTO typestable SET id=999, i8=null, i16=null, i32=null, i64=null, u8=null, u16=null, u32=null, u64=null,
			f32=null, f64=null, ti=null, da=null, te=null, bo=null, js=null, bl=null;`,
		[]sql.Row{{sql.NewOkResult(1)}},
		"SELECT * FROM typestable WHERE id = 999;",
		[]sql.Row{{int64(999), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}},
	},
}

var ReplaceErrorTests = []GenericErrorQueryTest{
	{
		"too few values",
		"REPLACE INTO mytable (s, i) VALUES ('x');",
	},
	{
		"too many values one column",
		"REPLACE INTO mytable (s) VALUES ('x', 999);",
	},
	{
		"too many values two columns",
		"REPLACE INTO mytable (i, s) VALUES (999, 'x', 'y');",
	},
	{
		"too few values no columns specified",
		"REPLACE INTO mytable VALUES (999);",
	},
	{
		"too many values no columns specified",
		"REPLACE INTO mytable VALUES (999, 'x', 'y');",
	},
	{
		"non-existent column values",
		"REPLACE INTO mytable (i, s, z) VALUES (999, 'x', 999);",
	},
	{
		"non-existent column set",
		"REPLACE INTO mytable SET i = 999, s = 'x', z = 999;",
	},
	{
		"duplicate column values",
		"REPLACE INTO mytable (i, s, s) VALUES (999, 'x', 'x');",
	},
	{
		"duplicate column set",
		"REPLACE INTO mytable SET i = 999, s = 'y', s = 'y';",
	},
	{
		"null given to non-nullable values",
		"INSERT INTO mytable (i, s) VALUES (null, 'y');",
	},
	{
		"null given to non-nullable set",
		"INSERT INTO mytable SET i = null, s = 'y';",
	},
}