// Copyright 2020-2021 Dolthub, Inc.
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
	"math"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TODO: none of these tests insert into tables without primary key columns, which have different semantics for
// REPLACE INTO queries. Add some tables / data without primary keys.
var ReplaceQueries = []WriteQueryTest{
	{
		WriteQuery:          "REPLACE INTO mytable VALUES (1, 'first row');",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT s FROM mytable WHERE i = 1;",
		ExpectedSelect:      []sql.Row{{"first row"}},
	},
	{
		WriteQuery:          "REPLACE INTO mytable SET i = 1, s = 'first row';",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT s FROM mytable WHERE i = 1;",
		ExpectedSelect:      []sql.Row{{"first row"}},
	},
	{
		WriteQuery:          "REPLACE INTO mytable VALUES (1, 'new row same i');",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT s FROM mytable WHERE i = 1;",
		ExpectedSelect:      []sql.Row{{"new row same i"}},
	},
	{
		WriteQuery:          "REPLACE INTO mytable SET i = 1, s = 'new row same i';",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT s FROM mytable WHERE i = 1;",
		ExpectedSelect:      []sql.Row{{"new row same i"}},
	},
	{
		WriteQuery:          "REPLACE INTO mytable (s, i) VALUES ('x', 999);",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT i FROM mytable WHERE s = 'x';",
		ExpectedSelect:      []sql.Row{{int64(999)}},
	},
	{
		WriteQuery:          "REPLACE INTO mytable SET s = 'x', i = 999;",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT i FROM mytable WHERE s = 'x';",
		ExpectedSelect:      []sql.Row{{int64(999)}},
	},
	{
		WriteQuery:          "REPLACE INTO mytable VALUES (999, 'x');",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT i FROM mytable WHERE s = 'x';",
		ExpectedSelect:      []sql.Row{{int64(999)}},
	},
	{
		WriteQuery:          "REPLACE INTO mytable SET i = 999, s = 'x';",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT i FROM mytable WHERE s = 'x';",
		ExpectedSelect:      []sql.Row{{int64(999)}},
	},
	{
		WriteQuery: `REPLACE INTO typestable VALUES (
			999, 127, 32767, 2147483647, 9223372036854775807,
			255, 65535, 4294967295, 18446744073709551615,
			3.40282346638528859811704183484516925440e+38, 1.797693134862315708145274237317043567981e+308,
			'2037-04-05 12:51:36', '2231-11-07',
			'random text', true, '{"key":"value"}', 'blobdata', 'v1', 'v2'
			);`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect: []sql.Row{{
			int64(999), int8(math.MaxInt8), int16(math.MaxInt16), int32(math.MaxInt32), int64(math.MaxInt64),
			uint8(math.MaxUint8), uint16(math.MaxUint16), uint32(math.MaxUint32), uint64(math.MaxUint64),
			float32(math.MaxFloat32), float64(math.MaxFloat64),
			sql.MustConvert(types.Timestamp.Convert(ctx, "2037-04-05 12:51:36")), sql.MustConvert(types.Date.Convert(ctx, "2231-11-07")),
			"random text", sql.True, types.MustJSON(`{"key":"value"}`), []byte("blobdata"), "v1", "v2",
		}},
	},
	{
		WriteQuery: `REPLACE INTO typestable SET
			id = 999, i8 = 127, i16 = 32767, i32 = 2147483647, i64 = 9223372036854775807,
			u8 = 255, u16 = 65535, u32 = 4294967295, u64 = 18446744073709551615,
			f32 = 3.40282346638528859811704183484516925440e+38, f64 = 1.797693134862315708145274237317043567981e+308,
			ti = '2037-04-05 12:51:36', da = '2231-11-07',
			te = 'random text', bo = true, js = '{"key":"value"}', bl = 'blobdata', e1 = 'v1', s1 = 'v2'
			;`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect: []sql.Row{{
			int64(999), int8(math.MaxInt8), int16(math.MaxInt16), int32(math.MaxInt32), int64(math.MaxInt64),
			uint8(math.MaxUint8), uint16(math.MaxUint16), uint32(math.MaxUint32), uint64(math.MaxUint64),
			float32(math.MaxFloat32), float64(math.MaxFloat64),
			sql.MustConvert(types.Timestamp.Convert(ctx, "2037-04-05 12:51:36")), sql.MustConvert(types.Date.Convert(ctx, "2231-11-07")),
			"random text", sql.True, types.MustJSON(`{"key":"value"}`), []byte("blobdata"), "v1", "v2",
		}},
	},
	{
		SkipServerEngine: true, // the datetime returned is not non-zero
		WriteQuery: `REPLACE INTO typestable VALUES (
			999, -128, -32768, -2147483648, -9223372036854775808,
			0, 0, 0, 0,
			1.401298464324817070923729583289916131280e-45, 4.940656458412465441765687928682213723651e-324,
			'0000-00-00 00:00:00', '0000-00-00',
			'', false, '""', '', '', ''
			);`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect: []sql.Row{{
			int64(999), int8(-math.MaxInt8 - 1), int16(-math.MaxInt16 - 1), int32(-math.MaxInt32 - 1), int64(-math.MaxInt64 - 1),
			uint8(0), uint16(0), uint32(0), uint64(0),
			float32(math.SmallestNonzeroFloat32), float64(math.SmallestNonzeroFloat64),
			types.Timestamp.Zero(), types.Date.Zero(),
			"", sql.False, types.MustJSON(`""`), []byte(""), "", "",
		}},
	},
	{
		SkipServerEngine: true, // the datetime returned is not non-zero
		WriteQuery: `REPLACE INTO typestable SET
			id = 999, i8 = -128, i16 = -32768, i32 = -2147483648, i64 = -9223372036854775808,
			u8 = 0, u16 = 0, u32 = 0, u64 = 0,
			f32 = 1.401298464324817070923729583289916131280e-45, f64 = 4.940656458412465441765687928682213723651e-324,
			ti = '0000-00-00 00:00:00', da = '0000-00-00',
			te = '', bo = false, js = '""', bl = '', e1 = '', s1 = ''
			;`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect: []sql.Row{{
			int64(999), int8(-math.MaxInt8 - 1), int16(-math.MaxInt16 - 1), int32(-math.MaxInt32 - 1), int64(-math.MaxInt64 - 1),
			uint8(0), uint16(0), uint32(0), uint64(0),
			float32(math.SmallestNonzeroFloat32), float64(math.SmallestNonzeroFloat64),
			types.Timestamp.Zero(), types.Date.Zero(),
			"", sql.False, types.MustJSON(`""`), []byte(""), "", "",
		}},
	},
	{
		WriteQuery: `REPLACE INTO typestable VALUES (999, null, null, null, null, null, null, null, null,
			null, null, null, null, null, null, null, null, null, null);`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect:      []sql.Row{{int64(999), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}},
	},
	{
		WriteQuery: `REPLACE INTO typestable SET id=999, i8=null, i16=null, i32=null, i64=null, u8=null, u16=null, u32=null, u64=null,
			f32=null, f64=null, ti=null, da=null, te=null, bo=null, js=null, bl=null, e1=null, s1=null;`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect:      []sql.Row{{int64(999), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}},
	},
}

var ReplaceErrorTests = []GenericErrorQueryTest{
	{
		Name:  "too few values",
		Query: "REPLACE INTO mytable (s, i) VALUES ('x');",
	},
	{
		Name:  "too many values one column",
		Query: "REPLACE INTO mytable (s) VALUES ('x', 999);",
	},
	{
		Name:  "too many values two columns",
		Query: "REPLACE INTO mytable (i, s) VALUES (999, 'x', 'y');",
	},
	{
		Name:  "too few values no columns specified",
		Query: "REPLACE INTO mytable VALUES (999);",
	},
	{
		Name:  "too many values no columns specified",
		Query: "REPLACE INTO mytable VALUES (999, 'x', 'y');",
	},
	{
		Name:  "non-existent column values",
		Query: "REPLACE INTO mytable (i, s, z) VALUES (999, 'x', 999);",
	},
	{
		Name:  "non-existent column set",
		Query: "REPLACE INTO mytable SET i = 999, s = 'x', z = 999;",
	},
	{
		Name:  "duplicate column values",
		Query: "REPLACE INTO mytable (i, s, s) VALUES (999, 'x', 'x');",
	},
	{
		Name:  "duplicate column set",
		Query: "REPLACE INTO mytable SET i = 999, s = 'y', s = 'y';",
	},
	{
		Name:  "null given to non-nullable values",
		Query: "INSERT INTO mytable (i, s) VALUES (null, 'y');",
	},
	{
		Name:  "null given to non-nullable set",
		Query: "INSERT INTO mytable SET i = null, s = 'y';",
	},
}
