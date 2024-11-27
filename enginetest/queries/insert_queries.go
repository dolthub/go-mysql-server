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
	"time"

	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var InsertQueries = []WriteQueryTest{
	{
		WriteQuery:          "INSERT INTO keyless VALUES ();",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM keyless WHERE c0 IS NULL;",
		ExpectedSelect:      []sql.UntypedSqlRow{{nil, nil}},
	},
	{
		WriteQuery:          "INSERT INTO keyless () VALUES ();",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM keyless WHERE c0 IS NULL;",
		ExpectedSelect:      []sql.UntypedSqlRow{{nil, nil}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (s, i) VALUES ('x', '10.0');",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT i FROM mytable WHERE s = 'x';",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(10)}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (s, i) VALUES ('x', '64.6');",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT i FROM mytable WHERE s = 'x';",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(65)}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (s, i) VALUES ('x', 999);",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT i FROM mytable WHERE s = 'x';",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(999)}},
	},
	{
		WriteQuery:          "INSERT INTO niltable (i, f) VALUES (10, 10.0), (12, 12.0);",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT i,f FROM niltable WHERE f IN (10.0, 12.0) ORDER BY f;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(10), 10.0}, {int64(12), 12.0}},
	},
	{
		WriteQuery:          "INSERT INTO mytable SET s = 'x', i = 999;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT i FROM mytable WHERE s = 'x';",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(999)}},
	},
	{
		WriteQuery:          "INSERT INTO mytable VALUES (999, 'x');",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT i FROM mytable WHERE s = 'x';",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(999)}},
	},
	{
		WriteQuery:          "INSERT INTO mytable SET i = 999, s = 'x';",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT i FROM mytable WHERE s = 'x';",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(999)}},
	},
	{
		WriteQuery:          "INSERT INTO mytable VALUES (999, _binary 'x');",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT s FROM mytable WHERE i = 999;",
		ExpectedSelect:      []sql.UntypedSqlRow{{"x"}},
	},
	{
		WriteQuery:          "INSERT INTO mytable SET i = 999, s = _binary 'x';",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT s FROM mytable WHERE i = 999;",
		ExpectedSelect:      []sql.UntypedSqlRow{{"x"}},
	},
	{
		WriteQuery: `INSERT INTO typestable VALUES (
			999, 127, 32767, 2147483647, 9223372036854775807,
			255, 65535, 4294967295, 18446744073709551615,
			3.40282346638528859811704183484516925440e+38, 1.797693134862315708145274237317043567981e+308,
			'2037-04-05 12:51:36', '2231-11-07',
			'random text', true, '{"key":"value"}', 'blobdata', 'v1', 'v2'
			);`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect: []sql.UntypedSqlRow{{
			int64(999), int8(math.MaxInt8), int16(math.MaxInt16), int32(math.MaxInt32), int64(math.MaxInt64),
			uint8(math.MaxUint8), uint16(math.MaxUint16), uint32(math.MaxUint32), uint64(math.MaxUint64),
			float32(math.MaxFloat32), float64(math.MaxFloat64),
			sql.MustConvert(types.Timestamp.Convert("2037-04-05 12:51:36")), sql.MustConvert(types.Date.Convert("2231-11-07")),
			"random text", sql.True, types.MustJSON(`{"key":"value"}`), []byte("blobdata"), "v1", "v2",
		}},
	},
	{
		WriteQuery: `INSERT INTO typestable SET
			id = 999, i8 = 127, i16 = 32767, i32 = 2147483647, i64 = 9223372036854775807,
			u8 = 255, u16 = 65535, u32 = 4294967295, u64 = 18446744073709551615,
			f32 = 3.40282346638528859811704183484516925440e+38, f64 = 1.797693134862315708145274237317043567981e+308,
			ti = '2037-04-05 12:51:36', da = '2231-11-07',
			te = 'random text', bo = true, js = '{"key":"value"}', bl = 'blobdata', e1 = 'v1', s1 = 'v2'
			;`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect: []sql.UntypedSqlRow{{
			int64(999), int8(math.MaxInt8), int16(math.MaxInt16), int32(math.MaxInt32), int64(math.MaxInt64),
			uint8(math.MaxUint8), uint16(math.MaxUint16), uint32(math.MaxUint32), uint64(math.MaxUint64),
			float32(math.MaxFloat32), float64(math.MaxFloat64),
			sql.MustConvert(types.Timestamp.Convert("2037-04-05 12:51:36")), sql.MustConvert(types.Date.Convert("2231-11-07")),
			"random text", sql.True, types.MustJSON(`{"key":"value"}`), []byte("blobdata"), "v1", "v2",
		}},
	},
	{
		SkipServerEngine: true, // the datetime returned is not non-zero
		WriteQuery: `INSERT INTO typestable VALUES (
			999, -128, -32768, -2147483648, -9223372036854775808,
			0, 0, 0, 0,
			1.401298464324817070923729583289916131280e-45, 4.940656458412465441765687928682213723651e-324,
			'0000-00-00 00:00:00', '0000-00-00',
			'', false, '""', '', '', ''
			);`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect: []sql.UntypedSqlRow{{
			int64(999), int8(-math.MaxInt8 - 1), int16(-math.MaxInt16 - 1), int32(-math.MaxInt32 - 1), int64(-math.MaxInt64 - 1),
			uint8(0), uint16(0), uint32(0), uint64(0),
			float32(math.SmallestNonzeroFloat32), float64(math.SmallestNonzeroFloat64),
			types.Timestamp.Zero(), types.Date.Zero(),
			"", sql.False, types.MustJSON(`""`), []byte(""), "", "",
		}},
	},
	{
		SkipServerEngine: true, // the datetime returned is not non-zero
		WriteQuery: `INSERT INTO typestable SET
			id = 999, i8 = -128, i16 = -32768, i32 = -2147483648, i64 = -9223372036854775808,
			u8 = 0, u16 = 0, u32 = 0, u64 = 0,
			f32 = 1.401298464324817070923729583289916131280e-45, f64 = 4.940656458412465441765687928682213723651e-324,
			ti = '0000-00-00 00:00:00', da = '0000-00-00',
			te = '', bo = false, js = '""', bl = '', e1 = 'v1', s1 = 'v2'
			;`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect: []sql.UntypedSqlRow{{
			int64(999), int8(-math.MaxInt8 - 1), int16(-math.MaxInt16 - 1), int32(-math.MaxInt32 - 1), int64(-math.MaxInt64 - 1),
			uint8(0), uint16(0), uint32(0), uint64(0),
			float32(math.SmallestNonzeroFloat32), float64(math.SmallestNonzeroFloat64),
			types.Timestamp.Zero(), types.Date.Zero(),
			"", sql.False, types.MustJSON(`""`), []byte(""), "v1", "v2",
		}},
	},
	{
		SkipServerEngine: true, // the datetime returned is not non-zero
		WriteQuery: `INSERT INTO typestable SET
			id = 999, i8 = -128, i16 = -32768, i32 = -2147483648, i64 = -9223372036854775808,
			u8 = 0, u16 = 0, u32 = 0, u64 = 0,
			f32 = 1.401298464324817070923729583289916131280e-45, f64 = 4.940656458412465441765687928682213723651e-324,
			ti = '2037-04-05 12:51:36 -0000 UTC', da = '0000-00-00',
			te = '', bo = false, js = '""', bl = '', e1 = 'v1', s1 = 'v2'
			;`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect: []sql.UntypedSqlRow{{
			int64(999), int8(-math.MaxInt8 - 1), int16(-math.MaxInt16 - 1), int32(-math.MaxInt32 - 1), int64(-math.MaxInt64 - 1),
			uint8(0), uint16(0), uint32(0), uint64(0),
			float32(math.SmallestNonzeroFloat32), float64(math.SmallestNonzeroFloat64),
			sql.MustConvert(types.Timestamp.Convert("2037-04-05 12:51:36")), types.Date.Zero(),
			"", sql.False, types.MustJSON(`""`), []byte(""), "v1", "v2",
		}},
	},
	{
		WriteQuery:          `INSERT INTO mytable (i,s) VALUES (10, 'NULL')`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i = 10;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(10), "NULL"}},
	},
	{
		WriteQuery: `INSERT INTO typestable VALUES (999, null, null, null, null, null, null, null, null,
			null, null, null, null, null, null, null, null, null, null);`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(999), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}},
	},
	{
		WriteQuery:          `INSERT INTO typestable (id, ti, da) VALUES (999, '2021-09-1', '2021-9-01');`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT id, ti, da FROM typestable WHERE id = 999;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(999), sql.MustConvert(types.Timestamp.Convert("2021-09-01")), sql.MustConvert(types.Date.Convert("2021-09-01"))}},
	},
	{
		WriteQuery: `INSERT INTO typestable SET id=999, i8=null, i16=null, i32=null, i64=null, u8=null, u16=null, u32=null, u64=null,
			f32=null, f64=null, ti=null, da=null, te=null, bo=null, js=null, bl=null, e1=null, s1=null;`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(999), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}},
	},
	{
		WriteQuery:          "INSERT INTO mytable SELECT i+100,s FROM mytable",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
			{int64(101), "first row"},
			{int64(102), "second row"},
			{int64(103), "third row"},
		},
	},
	{
		WriteQuery:          "INSERT INTO emptytable SELECT * FROM mytable",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM emptytable ORDER BY i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
	},
	{
		WriteQuery:          "INSERT INTO emptytable SELECT * FROM mytable where mytable.i > 2",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM emptytable ORDER BY i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{int64(3), "third row"},
		},
	},
	{
		WriteQuery:          "INSERT INTO niltable (i,f) SELECT i+10, NULL FROM mytable where mytable.i > 2",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM niltable where i > 10 ORDER BY i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{13, nil, nil, nil},
		},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) SELECT i+10, 'new' FROM mytable",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
			{int64(11), "new"},
			{int64(12), "new"},
			{int64(13), "new"},
		},
	},
	{
		WriteQuery:          "INSERT INTO mytable SELECT i2+100, s2 FROM othertable",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY i,s",
		ExpectedSelect: []sql.UntypedSqlRow{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
			{int64(101), "third"},
			{int64(102), "second"},
			{int64(103), "first"},
		},
	},
	{
		WriteQuery:          "INSERT INTO emptytable (s,i) SELECT * FROM othertable",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM emptytable ORDER BY i,s",
		ExpectedSelect: []sql.UntypedSqlRow{
			{int64(1), "third"},
			{int64(2), "second"},
			{int64(3), "first"},
		},
	},
	{
		WriteQuery:          "INSERT INTO emptytable (s,i) SELECT concat(m.s, o.s2), m.i FROM othertable o JOIN mytable m ON m.i=o.i2",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM emptytable ORDER BY i,s",
		ExpectedSelect: []sql.UntypedSqlRow{
			{int64(1), "first rowthird"},
			{int64(2), "second rowsecond"},
			{int64(3), "third rowfirst"},
		},
	},
	{
		WriteQuery: `INSERT INTO emptytable (s,i) SELECT s,i from mytable where i = 1
			union select s,i from mytable where i = 3`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM emptytable ORDER BY i,s",
		ExpectedSelect: []sql.UntypedSqlRow{
			{int64(1), "first row"},
			{int64(3), "third row"},
		},
	},
	{
		WriteQuery: `INSERT INTO emptytable (s,i) SELECT s,i from mytable where i = 1
			union select s,i from mytable where i = 3
			union select s,i from mytable where i > 2`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM emptytable ORDER BY i,s",
		ExpectedSelect: []sql.UntypedSqlRow{
			{int64(1), "first row"},
			{int64(3), "third row"},
		},
	},
	{
		WriteQuery: `INSERT INTO emptytable (s,i)
			SELECT s,i from mytable where i = 1
			union all select s,i+1 from mytable where i < 2
			union all select s,i+2 from mytable where i in (1)`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM emptytable ORDER BY i,s",
		ExpectedSelect: []sql.UntypedSqlRow{
			{int64(1), "first row"},
			{int64(2), "first row"},
			{int64(3), "first row"},
		},
	},
	{
		WriteQuery:          "INSERT INTO emptytable (s,i) SELECT distinct s,i from mytable",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM emptytable ORDER BY i,s",
		ExpectedSelect: []sql.UntypedSqlRow{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) SELECT (i + 10.0) / 10.0 + 10 + i, concat(s, ' new') FROM mytable",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY i, s",
		ExpectedSelect: []sql.UntypedSqlRow{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
			{int64(12), "first row new"},
			{int64(13), "second row new"},
			{int64(14), "third row new"},
		},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) SELECT CHAR_LENGTH(s), concat('numrows: ', count(*)) from mytable group by 1",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY i, s",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
			{9, "numrows: 2"},
			{10, "numrows: 1"},
		},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) SELECT CHAR_LENGTH(s) as len, concat('numrows: ', count(*)) from mytable group by 1 HAVING len > 9",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY i, s",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
			{10, "numrows: 1"},
		},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) SELECT i * 2, concat(s,s) from mytable order by 1 desc limit 1",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY i, s",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
			{6, "third rowthird row"},
		},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) SELECT i + 3, concat(s,s) from mytable order by 1 desc",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY i, s",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
			{4, "first rowfirst row"},
			{5, "second rowsecond row"},
			{6, "third rowthird row"},
		},
	},
	{
		WriteQuery: `INSERT INTO mytable (i,s) SELECT sub.i + 10, ot.s2
				FROM othertable ot INNER JOIN
					(SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub
				ON sub.i = ot.i2 order by 1`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable where i > 10 ORDER BY i, s",
		ExpectedSelect: []sql.UntypedSqlRow{
			{11, "third"},
			{12, "second"},
			{13, "first"},
		},
	},
	{
		WriteQuery: `INSERT INTO mytable (i,s) SELECT sub.i + 10, ot.s2
				FROM (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub
				INNER JOIN othertable ot ON sub.i = ot.i2 order by 1`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable where i > 10 ORDER BY i, s",
		ExpectedSelect: []sql.UntypedSqlRow{
			{11, "third"},
			{12, "second"},
			{13, "first"},
		},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (1, 'hello') ON DUPLICATE KEY UPDATE s='hello'",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i = 1",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "hello"}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (1, 'hello2') ON DUPLICATE KEY UPDATE s='hello3'",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i = 1",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "hello3"}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (1, 'hello') ON DUPLICATE KEY UPDATE i=10",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i = 10",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(10), "first row"}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (1, 'hello2') ON DUPLICATE KEY UPDATE s='hello3'",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i = 1",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "hello3"}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (1, 'hello2'), (2, 'hello3'), (4, 'no conflict') ON DUPLICATE KEY UPDATE s='hello4'",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(5)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY 1",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, "hello4"},
			{2, "hello4"},
			{3, "third row"},
			{4, "no conflict"},
		},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (10, 'hello') ON DUPLICATE KEY UPDATE s='hello'",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY 1",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
			{10, "hello"},
		},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (1,'hi') ON DUPLICATE KEY UPDATE s=VALUES(s)",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i = 1",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "hi"}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (1, 'hi') AS dt(new_i,new_s) ON DUPLICATE KEY UPDATE s=new_s",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i = 1",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "hi"}},
		Skip:                true, // https://github.com/dolthub/dolt/issues/7638
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (1, 'hi') AS dt ON DUPLICATE KEY UPDATE mytable.s=dt.s",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i = 1",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "hir"}},
		Skip:                true, // https://github.com/dolthub/dolt/issues/7638
	},
	{
		WriteQuery:          "INSERT INTO mytable (s,i) values ('dup',1) ON DUPLICATE KEY UPDATE s=CONCAT(VALUES(s), 'licate')",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i = 1",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "duplicate"}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (1,'mar'), (2,'par') ON DUPLICATE KEY UPDATE s=CONCAT(VALUES(s), 'tial')",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(4)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i IN (1,2) ORDER BY i",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "martial"}, {int64(2), "partial"}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (1,'maybe') ON DUPLICATE KEY UPDATE i=VALUES(i)+8000, s=VALUES(s)",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i = 8001",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(8001), "maybe"}},
	},
	{
		WriteQuery:          "INSERT INTO auto_increment_tbl (c0) values (44)",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 4}}},
		SelectQuery:         "SELECT * FROM auto_increment_tbl ORDER BY pk",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, 11},
			{2, 22},
			{3, 33},
			{4, 44},
		},
	},
	{
		WriteQuery:          "INSERT INTO auto_increment_tbl (c0) values (44),(55)",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 2, InsertID: 4}}},
		SelectQuery:         "SELECT * FROM auto_increment_tbl ORDER BY pk",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, 11},
			{2, 22},
			{3, 33},
			{4, 44},
			{5, 55},
		},
	},
	{
		WriteQuery:          "INSERT INTO auto_increment_tbl values (NULL, 44)",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 4}}},
		SelectQuery:         "SELECT * FROM auto_increment_tbl ORDER BY pk",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, 11},
			{2, 22},
			{3, 33},
			{4, 44},
		},
	},
	{
		WriteQuery:          "INSERT INTO auto_increment_tbl values (0, 44)",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 0}}},
		SelectQuery:         "SELECT * FROM auto_increment_tbl ORDER BY pk",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, 11},
			{2, 22},
			{3, 33},
			{4, 44},
		},
	},
	{
		WriteQuery:          "INSERT INTO auto_increment_tbl values (5, 44)",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 0}}},
		SelectQuery:         "SELECT * FROM auto_increment_tbl ORDER BY pk",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, 11},
			{2, 22},
			{3, 33},
			{5, 44},
		},
	},
	{
		WriteQuery: "INSERT INTO auto_increment_tbl values " +
			"(NULL, 44), (NULL, 55), (9, 99), (NULL, 110), (NULL, 121)",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 5, InsertID: 4}}},
		SelectQuery:         "SELECT * FROM auto_increment_tbl ORDER BY pk",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, 11},
			{2, 22},
			{3, 33},
			{4, 44},
			{5, 55},
			{9, 99},
			{10, 110},
			{11, 121},
		},
	},
	{
		WriteQuery:          `INSERT INTO auto_increment_tbl (c0) SELECT 44 FROM dual`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 4}}},
		SelectQuery:         "SELECT * FROM auto_increment_tbl",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, 11},
			{2, 22},
			{3, 33},
			{4, 44},
		},
	},
	{
		WriteQuery:          `INSERT INTO othertable VALUES ("fourth", 1) ON DUPLICATE KEY UPDATE s2="fourth"`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM othertable",
		ExpectedSelect: []sql.UntypedSqlRow{
			{"first", int64(3)},
			{"second", int64(2)},
			{"fourth", int64(1)},
		},
	},
	{
		WriteQuery:          `INSERT INTO othertable(S2,I2) values ('fourth',0)`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         `SELECT * FROM othertable where s2='fourth'`,
		ExpectedSelect: []sql.UntypedSqlRow{
			{"fourth", 0},
		},
	},
	{
		WriteQuery:          `INSERT INTO auto_increment_tbl VALUES ('4', 44)`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         `SELECT * from auto_increment_tbl where pk=4`,
		ExpectedSelect: []sql.UntypedSqlRow{
			{4, 44},
		},
	},
	{
		WriteQuery:          `INSERT INTO keyless (c0, c1) SELECT * from keyless where c0=0 and c1=0`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         `SELECT * from keyless where c0=0`,
		ExpectedSelect: []sql.UntypedSqlRow{
			{0, 0},
			{0, 0},
		},
	},
	{
		WriteQuery:          `insert into keyless (c0, c1) select a.c0, a.c1 from (select 1, 1) as a(c0, c1) join keyless on a.c0 = keyless.c0`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
		SelectQuery:         `SELECT * from keyless where c0=1`,
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, 1},
			{1, 1},
			{1, 1},
			{1, 1},
		},
	},
	{
		WriteQuery:          "with t (i,f) as (select 4,'fourth row' from dual) insert into mytable select i,f from t",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
		SelectQuery:         "select * from mytable order by i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
			{4, "fourth row"},
		},
	},
	{
		WriteQuery:          "with recursive t (i,f) as (select 4,4 from dual union all select i + 1, i + 1 from t where i < 5) insert into mytable select i,f from t",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 2}}},
		SelectQuery:         "select * from mytable order by i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
			{4, "4"},
			{5, "5"},
		},
	},
}

var SpatialInsertQueries = []WriteQueryTest{
	{
		WriteQuery:          "INSERT INTO point_table VALUES (1, POINT(1,1));",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM point_table;",
		ExpectedSelect:      []sql.UntypedSqlRow{{5, types.Point{X: 1, Y: 2}}, {1, types.Point{X: 1, Y: 1}}},
	},
	{
		WriteQuery:          "INSERT INTO point_table VALUES (1, 0x000000000101000000000000000000F03F0000000000000040);",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM point_table;",
		ExpectedSelect:      []sql.UntypedSqlRow{{5, types.Point{X: 1, Y: 2}}, {1, types.Point{X: 1, Y: 2}}},
	},
	{
		WriteQuery:          "INSERT INTO line_table VALUES (2, LINESTRING(POINT(1,2),POINT(3,4)));",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM line_table;",
		ExpectedSelect:      []sql.UntypedSqlRow{{0, types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}}, {1, types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}, {X: 5, Y: 6}}}}, {2, types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}}},
	},
	{
		WriteQuery:          "INSERT INTO line_table VALUES (2, 0x00000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040);",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM line_table;",
		ExpectedSelect:      []sql.UntypedSqlRow{{0, types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}}, {1, types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}, {X: 5, Y: 6}}}}, {2, types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}}},
	},
	{
		WriteQuery:          "INSERT INTO polygon_table VALUES (2, POLYGON(LINESTRING(POINT(1,1),POINT(1,-1),POINT(-1,-1),POINT(-1,1),POINT(1,1))));",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM polygon_table;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{0, types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{1, types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}, {Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{2, types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 1}, {X: 1, Y: -1}, {X: -1, Y: -1}, {X: -1, Y: 1}, {X: 1, Y: 1}}}}}},
		},
	},
	{
		WriteQuery:          "INSERT INTO polygon_table VALUES (2, 0x0000000001030000000100000005000000000000000000F03F000000000000F03F000000000000F03F000000000000F0BF000000000000F0BF000000000000F0BF000000000000F0BF000000000000F03F000000000000F03F000000000000F03F);",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM polygon_table;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{0, types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{1, types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}, {Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{2, types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 1}, {X: 1, Y: -1}, {X: -1, Y: -1}, {X: -1, Y: 1}, {X: 1, Y: 1}}}}}}},
	},
	{
		WriteQuery:          "INSERT INTO geometry_table VALUES (100, POINT(123.456,7.89));",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM geometry_table;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, types.Point{X: 1, Y: 2}},
			{2, types.Point{SRID: 4326, X: 1, Y: 2}},
			{3, types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{4, types.LineString{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{5, types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{6, types.Polygon{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}},
			{7, types.MultiPoint{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{8, types.MultiPoint{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{9, types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}}}},
			{10, types.MultiLineString{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}}}},
			{11, types.MultiPolygon{Polygons: []types.Polygon{{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 2}, {X: 3, Y: 4}, {X: 0, Y: 0}}}}}}}},
			{12, types.MultiPolygon{SRID: 4326, Polygons: []types.Polygon{{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}, {SRID: 4326, X: 0, Y: 0}}}}}}}},
			{13, types.GeomColl{Geoms: []types.GeometryValue{types.GeomColl{Geoms: []types.GeometryValue{}}}}},
			{14, types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{}}}}},
			{100, types.Point{X: 123.456, Y: 7.89}},
		},
	},
	{
		WriteQuery:          "INSERT INTO geometry_table VALUES (100, 0x00000000010100000077BE9F1A2FDD5E408FC2F5285C8F1F40);",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM geometry_table;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, types.Point{X: 1, Y: 2}},
			{2, types.Point{SRID: 4326, X: 1, Y: 2}},
			{3, types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{4, types.LineString{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{5, types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{6, types.Polygon{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}},
			{7, types.MultiPoint{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{8, types.MultiPoint{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{9, types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}}}},
			{10, types.MultiLineString{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}}}},
			{11, types.MultiPolygon{Polygons: []types.Polygon{{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 2}, {X: 3, Y: 4}, {X: 0, Y: 0}}}}}}}},
			{12, types.MultiPolygon{SRID: 4326, Polygons: []types.Polygon{{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}, {SRID: 4326, X: 0, Y: 0}}}}}}}},
			{13, types.GeomColl{Geoms: []types.GeometryValue{types.GeomColl{Geoms: []types.GeometryValue{}}}}},
			{14, types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{}}}}},
			{100, types.Point{X: 123.456, Y: 7.89}},
		},
	},
	{
		WriteQuery:          "INSERT INTO geometry_table VALUES (100, LINESTRING(POINT(1,2),POINT(3,4)));",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM geometry_table;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, types.Point{X: 1, Y: 2}},
			{2, types.Point{SRID: 4326, X: 1, Y: 2}},
			{3, types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{4, types.LineString{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{5, types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{6, types.Polygon{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}},
			{7, types.MultiPoint{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{8, types.MultiPoint{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{9, types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}}}},
			{10, types.MultiLineString{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}}}},
			{11, types.MultiPolygon{Polygons: []types.Polygon{{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 2}, {X: 3, Y: 4}, {X: 0, Y: 0}}}}}}}},
			{12, types.MultiPolygon{SRID: 4326, Polygons: []types.Polygon{{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}, {SRID: 4326, X: 0, Y: 0}}}}}}}},
			{13, types.GeomColl{Geoms: []types.GeometryValue{types.GeomColl{Geoms: []types.GeometryValue{}}}}},
			{14, types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{}}}}},
			{100, types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
		},
	},
	{
		WriteQuery:          "INSERT INTO geometry_table VALUES (100, 0x00000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040);",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM geometry_table;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, types.Point{X: 1, Y: 2}},
			{2, types.Point{SRID: 4326, X: 1, Y: 2}},
			{3, types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{4, types.LineString{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{5, types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{6, types.Polygon{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}},
			{7, types.MultiPoint{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{8, types.MultiPoint{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{9, types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}}}},
			{10, types.MultiLineString{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}}}},
			{11, types.MultiPolygon{Polygons: []types.Polygon{{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 2}, {X: 3, Y: 4}, {X: 0, Y: 0}}}}}}}},
			{12, types.MultiPolygon{SRID: 4326, Polygons: []types.Polygon{{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}, {SRID: 4326, X: 0, Y: 0}}}}}}}},
			{13, types.GeomColl{Geoms: []types.GeometryValue{types.GeomColl{Geoms: []types.GeometryValue{}}}}},
			{14, types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{}}}}},
			{100, types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
		},
	},
	{
		WriteQuery:          "INSERT INTO geometry_table VALUES (100, POLYGON(LINESTRING(POINT(1,1),POINT(1,-1),POINT(-1,-1),POINT(-1,1),POINT(1,1))));",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM geometry_table;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, types.Point{X: 1, Y: 2}},
			{2, types.Point{SRID: 4326, X: 1, Y: 2}},
			{3, types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{4, types.LineString{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{5, types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{6, types.Polygon{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}},
			{7, types.MultiPoint{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{8, types.MultiPoint{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{9, types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}}}},
			{10, types.MultiLineString{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}}}},
			{11, types.MultiPolygon{Polygons: []types.Polygon{{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 2}, {X: 3, Y: 4}, {X: 0, Y: 0}}}}}}}},
			{12, types.MultiPolygon{SRID: 4326, Polygons: []types.Polygon{{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}, {SRID: 4326, X: 0, Y: 0}}}}}}}},
			{13, types.GeomColl{Geoms: []types.GeometryValue{types.GeomColl{Geoms: []types.GeometryValue{}}}}},
			{14, types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{}}}}},
			{100, types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 1}, {X: 1, Y: -1}, {X: -1, Y: -1}, {X: -1, Y: 1}, {X: 1, Y: 1}}}}}},
		},
	},
	{
		WriteQuery:          "INSERT INTO geometry_table VALUES (100, 0x0000000001030000000100000005000000000000000000F03F000000000000F03F000000000000F03F000000000000F0BF000000000000F0BF000000000000F0BF000000000000F0BF000000000000F03F000000000000F03F000000000000F03F);",
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM geometry_table;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, types.Point{X: 1, Y: 2}},
			{2, types.Point{SRID: 4326, X: 1, Y: 2}},
			{3, types.LineString{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{4, types.LineString{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{5, types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{6, types.Polygon{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}},
			{7, types.MultiPoint{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{8, types.MultiPoint{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{9, types.MultiLineString{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}}}},
			{10, types.MultiLineString{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}}}},
			{11, types.MultiPolygon{Polygons: []types.Polygon{{Lines: []types.LineString{{Points: []types.Point{{X: 0, Y: 0}, {X: 1, Y: 2}, {X: 3, Y: 4}, {X: 0, Y: 0}}}}}}}},
			{12, types.MultiPolygon{SRID: 4326, Polygons: []types.Polygon{{SRID: 4326, Lines: []types.LineString{{SRID: 4326, Points: []types.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}, {SRID: 4326, X: 0, Y: 0}}}}}}}},
			{13, types.GeomColl{Geoms: []types.GeometryValue{types.GeomColl{Geoms: []types.GeometryValue{}}}}},
			{14, types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{types.GeomColl{SRID: 4326, Geoms: []types.GeometryValue{}}}}},
			{100, types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 1}, {X: 1, Y: -1}, {X: -1, Y: -1}, {X: -1, Y: 1}, {X: 1, Y: 1}}}}}},
		},
	},
}

var InsertScripts = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/7322
		Name: "issue 7322: values expression is subquery",
		SetUpScript: []string{
			"create table xy (x int auto_increment primary key, y varchar(50) not null)",
			"create table uv (u int auto_increment primary key, v varchar(50) not null, x_id int, constraint u_x_fk foreign key (x_id) references xy (x))",
			"insert into xy values (1,'admin'), (2, 'standard')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO uv(v, x_id) VALUES ('test', (SELECT x FROM xy WHERE y = 'admin'));",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, InsertID: 1}}},
			},
			{
				Query:       "INSERT INTO uv(v, x_id) VALUES ('test', (SELECT x FROM xy WHERE x > 0));",
				ExpectedErr: sql.ErrExpectedSingleRow,
			},
			{
				Query:    "select * from uv",
				Expected: []sql.UntypedSqlRow{{1, "test", 1}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/6675
		Name: "issue 6675: on duplicate rearranged getfield indexes from select source",
		SetUpScript: []string{
			"create table xy (x int primary key, y datetime)",
			"insert into xy values (0,'2023-09-16')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO xy (y,x) select * from (select cast('2019-12-31T12:00:00Z' as date), 0) dt(a,b) ON DUPLICATE KEY UPDATE x=dt.b+1, y=dt.a",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				Query:    "select * from xy",
				Expected: []sql.UntypedSqlRow{{1, time.Date(2019, time.December, 31, 0, 0, 0, 0, time.UTC)}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/4857
		Name: "issue 4857: insert cte column alias with table alias qualify panic",
		SetUpScript: []string{
			"create table xy (x int primary key, y int)",
			"insert into xy values (0,0), (1,1), (2,2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `With a as (
  With b as (
    Select sum(x) as x, y from xy where x < 2 group by y
  )
  Select * from b d
) insert into xy (x,y) select x+9,y+9 from a;`,
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 2, InsertID: 0}}},
			},
		},
	},
	{
		Name: "INSERT zero date DATETIME NOT NULL is valid",
		SetUpScript: []string{
			"CREATE TABLE t1 (dt datetime not null)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO t1 (dt) VALUES ('0001-01-01 00:00:00');",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
		},
	},
	{
		Name: "insert into sparse auto_increment table",
		SetUpScript: []string{
			"create table auto (pk int primary key auto_increment)",
			"insert into auto values (10), (20), (30)",
			"insert into auto values (NULL)",
			"insert into auto values (40)",
			"insert into auto values (0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{10}, {20}, {30}, {31}, {40}, {41},
				},
			},
		},
	},
	{
		Name: "insert negative values into auto_increment values",
		SetUpScript: []string{
			"create table auto (pk int primary key auto_increment)",
			"insert into auto values (10), (20), (30)",
			"insert into auto values (-1), (-2), (-3)",
			"insert into auto () values ()",
			"insert into auto values (0), (0), (0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{-3}, {-2}, {-1}, {10}, {20}, {30}, {31}, {32}, {33}, {34},
				},
			},
		},
	},
	{
		Name: "insert into auto_increment unique key column",
		SetUpScript: []string{
			"create table auto (pk int primary key, npk int unique auto_increment)",
			"insert into auto (pk) values (10), (20), (30)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{10, 1}, {20, 2}, {30, 3},
				},
			},
		},
	},
	{
		Name: "insert into auto_increment with multiple unique key columns",
		SetUpScript: []string{
			"create table auto (pk int primary key, npk1 int auto_increment, npk2 int, unique(npk1, npk2))",
			"insert into auto (pk) values (10), (20), (30)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{10, 1, nil}, {20, 2, nil}, {30, 3, nil},
				},
			},
		},
	},
	{
		Name: "insert into auto_increment key/index column",
		SetUpScript: []string{
			"create table auto_no_primary (i int auto_increment, index(i))",
			"insert into auto_no_primary (i) values (0), (0), (0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto_no_primary order by 1",
				Expected: []sql.UntypedSqlRow{
					{1}, {2}, {3},
				},
			},
		},
	},
	{
		Name: "insert into auto_increment with multiple key/index columns",
		SetUpScript: []string{
			"create table auto_no_primary (i int auto_increment, j int, index(i))",
			"insert into auto_no_primary (i) values (0), (0), (0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto_no_primary order by 1",
				Expected: []sql.UntypedSqlRow{
					{1, nil}, {2, nil}, {3, nil},
				},
			},
		},
	},
	{
		Name: "auto increment table handles deletes",
		SetUpScript: []string{
			"create table auto (pk int primary key auto_increment)",
			"insert into auto values (10)",
			"delete from auto where pk = 10",
			"insert into auto values (NULL)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{11},
				},
			},
		},
	},
	{
		Name: "create auto_increment table with out-of-line primary key def",
		SetUpScript: []string{
			`create table auto (
				pk int auto_increment,
				c0 int,
				primary key(pk)
			);`,
			"insert into auto values (NULL,10), (NULL,20), (NULL,30)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{1, 10}, {2, 20}, {3, 30},
				},
			},
		},
	},
	{
		Name: "alter auto_increment value",
		SetUpScript: []string{
			`create table auto (
				pk int auto_increment,
				c0 int,
				primary key(pk)
			);`,
			"insert into auto values (NULL,10), (NULL,20), (NULL,30)",
			"alter table auto auto_increment 9;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT AUTO_INCREMENT FROM information_schema.tables WHERE table_name = 'auto' AND table_schema = DATABASE()",
				Expected: []sql.UntypedSqlRow{{uint64(9)}},
			},
			{
				Query: "insert into auto values (NULL,90)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{
					RowsAffected: 1,
					InsertID:     9,
				}}},
			},
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{1, 10}, {2, 20}, {3, 30}, {9, 90},
				},
			},
		},
	},
	{
		Name: "alter auto_increment value to float",
		SetUpScript: []string{
			`create table auto (
				pk int auto_increment,
				c0 int,
				primary key(pk)
			);`,
			"insert into auto values (NULL,10), (NULL,20), (NULL,30)",
			"alter table auto auto_increment = 19.9;",
			"insert into auto values (NULL,190)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{1, 10}, {2, 20}, {3, 30}, {19, 190},
				},
			},
		},
	},
	{
		Name: "auto increment on tinyint",
		SetUpScript: []string{
			"create table auto (pk tinyint primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{1}, {10}, {11},
				},
			},
		},
	},
	{
		Name: "auto increment on smallint",
		SetUpScript: []string{
			"create table auto (pk smallint primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{1}, {10}, {11},
				},
			},
		},
	},
	{
		Name: "auto increment on mediumint",
		SetUpScript: []string{
			"create table auto (pk mediumint primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{1}, {10}, {11},
				},
			},
		},
	},
	{
		Name: "auto increment on int",
		SetUpScript: []string{
			"create table auto (pk int primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{1}, {10}, {11},
				},
			},
		},
	},
	{
		Name: "auto increment on bigint",
		SetUpScript: []string{
			"create table auto (pk bigint primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{1}, {10}, {11},
				},
			},
		},
	},
	{
		Name: "auto increment on tinyint unsigned",
		SetUpScript: []string{
			"create table auto (pk tinyint unsigned primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{uint64(1)}, {uint64(10)}, {uint64(11)},
				},
			},
		},
	},
	{
		Name: "auto increment on smallint unsigned",
		SetUpScript: []string{
			"create table auto (pk smallint unsigned primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{uint64(1)}, {uint64(10)}, {uint64(11)},
				},
			},
		},
	},
	{
		Name: "auto increment on mediumint unsigned",
		SetUpScript: []string{
			"create table auto (pk mediumint unsigned primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{uint64(1)}, {uint64(10)}, {uint64(11)},
				},
			},
		},
	},
	{
		Name: "auto increment on int unsigned",
		SetUpScript: []string{
			"create table auto (pk int unsigned primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{uint64(1)}, {uint64(10)}, {uint64(11)},
				},
			},
		},
	},
	{
		Name: "auto increment on bigint unsigned",
		SetUpScript: []string{
			"create table auto (pk bigint unsigned primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{uint64(1)}, {uint64(10)}, {uint64(11)},
				},
			},
		},
	},
	{
		Name: "auto increment on float",
		SetUpScript: []string{
			"create table auto (pk float primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{float64(1)}, {float64(10)}, {float64(11)},
				},
			},
		},
	},
	{
		Name: "auto increment on double",
		SetUpScript: []string{
			"create table auto (pk double primary key auto_increment)",
			"insert into auto values (NULL),(10),(0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.UntypedSqlRow{
					{float64(1)}, {float64(10)}, {float64(11)},
				},
			},
		},
	},
	{
		Name: "sql_mode=NO_auto_value_ON_ZERO",
		SetUpScript: []string{
			"set @old_sql_mode=@@sql_mode;",
			"set @@sql_mode='NO_auto_value_ON_ZERO';",
			"create table auto (i int auto_increment, index (i));",
			"create table auto_pk (i int auto_increment primary key);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select auto_increment from information_schema.tables where table_name='auto' and table_schema=database()",
				Expected: []sql.UntypedSqlRow{
					{nil},
				},
			},
			{
				Query: "insert into auto values (0), (0), (1-1)",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 3, InsertID: 0}},
				},
			},
			{
				Query: "select * from auto order by i",
				Expected: []sql.UntypedSqlRow{
					{0},
					{0},
					{0},
				},
			},
			{
				Query: "select auto_increment from information_schema.tables where table_name='auto' and table_schema=database()",
				Expected: []sql.UntypedSqlRow{
					{nil},
				},
			},
			{
				Query: "insert into auto values (1)",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1, InsertID: 0}},
				},
			},
			{
				Query: "select auto_increment from information_schema.tables where table_name='auto' and table_schema=database()",
				Expected: []sql.UntypedSqlRow{
					{uint64(2)},
				},
			},

			{
				Query: "select auto_increment from information_schema.tables where table_name='auto_pk' and table_schema=database()",
				Expected: []sql.UntypedSqlRow{
					{nil},
				},
			},
			{
				Query: "insert into auto_pk values (0), (1), (NULL), ()",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 4, InsertID: 2}},
				},
			},
			{
				Query: "select * from auto_pk",
				Expected: []sql.UntypedSqlRow{
					{0},
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "select auto_increment from information_schema.tables where table_name='auto_pk' and table_schema=database()",
				Expected: []sql.UntypedSqlRow{
					{uint64(4)},
				},
			},

			{
				// restore old sql_mode just in case
				SkipResultsCheck: true,
				Query:            "set @@sql_mode=@old_sql_mode",
			},
		},
	},
	{
		Name: "explicit DEFAULT",
		SetUpScript: []string{
			"CREATE TABLE t1(id int DEFAULT '2', dt datetime DEFAULT now());",
			"CREATE TABLE t2(id varchar(100) DEFAULT (uuid()));",
			"CREATE TABLE t3(a int DEFAULT '1', b int default (2 * a));",
			"CREATE TABLE t4(c0 varchar(10) null default 'c0', c1 varchar(10) null default 'c1');",
			// MySQL allows the current_timestamp() function to NOT be in parens when used as a default
			// https://dev.mysql.com/doc/refman/8.0/en/data-type-defaults.html
			"CREATE TABLE t5(c0 varchar(100) DEFAULT (repeat('_', 100)), c1 datetime DEFAULT current_timestamp());",
			// Regression test case for custom column ordering: https://github.com/dolthub/dolt/issues/4004
			"create table t6 (color enum('red', 'blue', 'green') default 'blue', createdAt timestamp default (current_timestamp()));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO T1 values (DEFAULT, DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "INSERT INTO t1 (id, dt) values (DEFAULT, DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "INSERT INTO t1 (dt, ID) values (DEFAULT, DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "INSERT INTO t1 (ID) values (DEFAULT), (3)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 2}}},
			},
			{
				Query:    "INSERT INTO t1 (dt) values (DEFAULT), ('1981-02-16 00:00:00')",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 2}}},
			},
			{
				Query:    "INSERT INTO t1 values (100, '2000-01-01 12:34:56'), (DEFAULT, DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 2}}},
			},
			{
				Query:    "INSERT INTO t1 (id, dt) values (100, '2022-01-01 01:01:01'), (DEFAULT, DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 2}}},
			},
			{
				Query:    "INSERT INTO t1 (id) values (10), (DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 2}}},
			},
			{
				Query:    "INSERT INTO t1 (DT) values ('2022-02-02 02:02:02'), (DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 2}}},
			},
			{
				Query:    "INSERT INTO t2 values ('10'), (DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 2}}},
			},
			{
				Query:    "INSERT INTO t2 (id) values (DEFAULT), ('11'), (DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 3}}},
			},
			{
				Query:    "select count(distinct id) from t2",
				Expected: []sql.UntypedSqlRow{{5}},
			},
			{
				Query:    "INSERT INTO t3 (a) values (DEFAULT), ('2'), (DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 3}}},
			},
			{
				Query:    "SELECT b from t3 order by b asc",
				Expected: []sql.UntypedSqlRow{{2}, {2}, {4}},
			},
			{
				Query:    "INSERT INTO T4 (c1, c0) values (DEFAULT, NULL)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "select * from t4",
				Expected: []sql.UntypedSqlRow{{nil, "c1"}},
			},
			{
				Query:    "INSERT INTO T5 values (DEFAULT, DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "INSERT INTO T5 (c0, c1) values (DEFAULT, DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "INSERT INTO T5 (c1) values (DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
			{
				// Custom column order should use the correct column defaults
				Query:    "insert into T6(createdAt, color) values (DEFAULT, DEFAULT);",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
		},
	},
	{
		Name: "Explicit default with column reference",
		SetUpScript: []string{
			"CREATE TABLE t1 (a int default 1, b int default (a+1));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO t1 (a,b) values (1, DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "select * from t1 order by a",
				Expected: []sql.UntypedSqlRow{{1, 2}},
			},
			{
				Query:    "INSERT INTO t1 values (2, DEFAULT)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "select * from t1 where a = 2 order by a",
				Expected: []sql.UntypedSqlRow{{2, 3}},
			},
			{
				Query:    "INSERT INTO t1 (b,a) values (DEFAULT, 3)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "select * from t1 where a = 3 order by a",
				Expected: []sql.UntypedSqlRow{{3, 4}},
			},
		},
	},
	{
		Name: "Try INSERT IGNORE with primary key, non null, and single row violations",
		SetUpScript: []string{
			"CREATE TABLE y (pk int primary key, c1 int NOT NULL);",
			"INSERT IGNORE INTO y VALUES (1, 1), (1,2), (2, 2), (3, 3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM y",
				Expected: []sql.UntypedSqlRow{
					{1, 1}, {2, 2}, {3, 3},
				},
			},
			{
				Query: "INSERT IGNORE INTO y VALUES (1, 2), (4,4)",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1}},
				},
				ExpectedWarning: mysql.ERDupEntry,
			},
			{
				Query: "INSERT IGNORE INTO y VALUES (5, NULL)",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1}},
				},
				ExpectedWarning: mysql.ERBadNullError,
			},
			{
				Query: "INSERT IGNORE INTO y SELECT * FROM y WHERE pk=(SELECT pk+10 FROM y WHERE pk > 1);",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 0}},
				},
				ExpectedWarning: mysql.ERSubqueryNo1Row,
			},
			{
				Query: "INSERT IGNORE INTO y SELECT 10, 0 FROM dual WHERE 1=(SELECT 1 FROM dual UNION SELECT 2 FROM dual);",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 0}},
				},
				ExpectedWarning: mysql.ERSubqueryNo1Row,
			},
			{
				Query: "INSERT IGNORE INTO y SELECT 11, 0 FROM dual WHERE 1=(SELECT 1 FROM dual UNION SELECT 2 FROM dual) UNION SELECT 12, 0 FROM dual;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1}},
				},
				ExpectedWarning: mysql.ERSubqueryNo1Row,
			},
			{
				Query: "INSERT IGNORE INTO y SELECT 13, 0 FROM dual UNION SELECT 14, 0 FROM dual WHERE 1=(SELECT 1 FROM dual UNION SELECT 2 FROM dual);",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1}},
				},
				ExpectedWarning: mysql.ERSubqueryNo1Row,
			},
			{
				Query: "INSERT IGNORE INTO y VALUES (3, 8)",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 0}},
				},
				ExpectedWarning: mysql.ERDupEntry,
			},
		},
	},
	{
		Name: "INSERT Accumulator tests",
		SetUpScript: []string{
			"CREATE TABLE test(pk int primary key, val int)",
			"INSERT INTO test values (1,1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       `INSERT INTO test VALUES (2,2),(2,3)`,
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{
				Query:    `DELETE FROM test where pk = 1;`,
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query: `INSERT INTO test VALUES (1,1)`,
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1}},
				},
			},
		},
	},
	{
		Name: "INSERT Case Sensitivity",
		SetUpScript: []string{
			"CREATE TABLE test (PK int PRIMARY KEY);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "insert into test(pk) values (1)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
		},
	},
	{
		Name: "INSERT string with exact char length but extra byte length",
		SetUpScript: []string{
			"CREATE TABLE city (id int PRIMARY KEY, district char(20) NOT NULL DEFAULT '');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO city VALUES (1,'San Pedro de Macorís');",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
		},
	},
	{
		Name: "Insert on duplicate key",
		SetUpScript: []string{
			`CREATE TABLE users (
				id varchar(42) PRIMARY KEY
			)`,
			`CREATE TABLE nodes (
			    id varchar(42) PRIMARY KEY,
			    owner varchar(42),
			    status varchar(12),
			    timestamp bigint NOT NULL,
			    FOREIGN KEY(owner) REFERENCES users(id)
			)`,
			"INSERT INTO users values ('milo'), ('dabe')",
			"INSERT INTO nodes values ('id1', 'milo', 'off', 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "insert into nodes(id,owner,status,timestamp) values('id1','dabe','off',2) on duplicate key update owner='milo',status='on'",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 2}},
				},
			},
			{
				Query: "insert into nodes(id,owner,status,timestamp) values('id2','dabe','off',3) on duplicate key update owner='milo',status='on'",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "select * from nodes",
				Expected: []sql.UntypedSqlRow{
					{"id1", "milo", "on", 1},
					{"id2", "dabe", "off", 3},
				},
			},
		},
	},
	{
		Name: "Insert on duplicate key references table in subquery",
		SetUpScript: []string{
			`create table a (i int primary key)`,
			`insert into a values (1)`,
			`create table b (j int primary key)`,
			`insert into b values (1), (2), (3)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `insert into a (select * from b) on duplicate key update a.i = b.j + 100`,
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 4}},
				},
			},
			{
				Query: "select * from a",
				Expected: []sql.UntypedSqlRow{
					{101},
					{2},
					{3},
				},
			},
		},
	},
	{
		Name: "Insert on duplicate key references table in aliased subquery",
		SetUpScript: []string{
			`create table a (i int primary key)`,
			`insert into a values (1)`,
			`create table b (j int primary key)`,
			`insert into b values (1), (2), (3)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       `insert into a (select * from b as t) on duplicate key update a.i = b.j + 100`,
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query: `insert into a (select * from b as t) on duplicate key update a.i = t.j + 100`,
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 4}},
				},
			},
			{
				Query: "select * from a",
				Expected: []sql.UntypedSqlRow{
					{101},
					{2},
					{3},
				},
			},
		},
	},
	{
		Name: "insert on duplicate key update errors",
		SetUpScript: []string{
			`create table a (i int primary key)`,
			`create table b (i int primary key)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       `insert into a (select * from b) on duplicate key update i = i`,
				ExpectedErr: sql.ErrAmbiguousColumnName,
			},
			{
				Query:       `insert into a (select * from b) on duplicate key update b.i = a.i`,
				ExpectedErr: sql.ErrTableNotFound,
			},
		},
	},
	{
		Name: "Insert on duplicate key references table in subquery with join",
		SetUpScript: []string{
			`create table a (i int primary key, j int)`,
			`insert into a values (1,1)`,
			`create table b (x int primary key)`,
			`insert into b values (1), (2), (3)`,
			`create table c (y int primary key)`,
			`insert into c values (1), (2), (3)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `insert into a (select * from b join c where b.x = c.y) on duplicate key update a.j = b.x + c.y + 100`,
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 4}},
				},
			},
			{
				Query: "select * from a",
				Expected: []sql.UntypedSqlRow{
					{1, 102},
					{2, 2},
					{3, 3},
				},
			},
		},
	},
	{
		// refer to https://github.com/dolthub/dolt/issues/6437
		Name: "Insert on duplicate key references table in subquery with alias",
		SetUpScript: []string{
			`create table a (i int primary key)`,
			`insert into a values (1)`,
			`create table b (i int primary key)`,
			`insert into b values (1), (2), (3)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `insert into a (select t.i from b as t, b where t.i = b.i) on duplicate key update i = b.i;`,
				Skip:  true,
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 2}},
				},
			},
			{
				Query: "select * from a",
				Skip:  true,
				Expected: []sql.UntypedSqlRow{
					{1},
					{2},
					{3},
				},
			},
		},
	},
	{
		Name: "Insert on duplicate key references table in cte",
		SetUpScript: []string{
			`create table a (i int primary key)`,
			`insert into a values (1)`,
			`create table b (j int primary key)`,
			`insert into b values (1), (2), (3)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `insert into a with cte as (select * from b) select * from cte on duplicate key update a.i = cte.j + 100`,
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 4}},
				},
			},
			{
				Query: "select * from a",
				Skip:  true,
				Expected: []sql.UntypedSqlRow{
					{101},
					{2},
					{3},
				},
			},
		},
	},
	{
		Name: "insert on duplicate key with incorrect row alias",
		SetUpScript: []string{
			`create table a (i int primary key)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       `insert into a values (1) as new(c, d) on duplicate key update i = c`,
				ExpectedErr: sql.ErrColumnCountMismatch,
			},
		},
	},
	{
		Name: "Insert throws primary key violations",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY key);",
			"CREATE TABLE t2 (pk1 int, pk2 int, PRIMARY KEY (pk1, pk2));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO t VALUES (1), (2);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				Query:       "INSERT into t VALUES (1);",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.UntypedSqlRow{{1}, {2}},
			},
			{
				Query:    "INSERT into t2 VALUES (1, 1), (2, 2);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				Query:       "INSERT into t2 VALUES (1, 1);",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{
				Query:    "SELECT * from t2;",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}},
			},
		},
	},
	{
		Name: "Insert throws unique key violations",
		SetUpScript: []string{
			"CREATE TABLE t (pk int PRIMARY key, col1 int UNIQUE);",
			"CREATE TABLE t2 (pk int PRIMARY key, col1 int, col2 int, UNIQUE KEY (col1, col2));",
			"INSERT into t VALUES (1, 1);",
			"INSERT into t2 VALUES (1, 1, 1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "INSERT INTO t VALUES (2, 2), (3, 1), (4, 4);",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.UntypedSqlRow{{1, 1}},
			},
			{
				Query:       "INSERT INTO t2 VALUES (2, 2, 2), (3, 1, 1), (4, 4, 4);",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "SELECT * from t2;",
				Expected: []sql.UntypedSqlRow{{1, 1, 1}},
			},
			{
				Query:       "INSERT INTO t VALUES (5, 2), (6, 2);",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.UntypedSqlRow{{1, 1}},
			},
			{
				Query:       "INSERT INTO t2 VALUES (5, 2, 2), (6, 2, 2);",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "SELECT * from t2;",
				Expected: []sql.UntypedSqlRow{{1, 1, 1}},
			},
			{
				Query:    "INSERT into t2 VALUES (5, NULL, 1), (6, NULL, 1), (7, 1, NULL), (8, 1, NULL), (9, NULL, NULL), (10, NULL, NULL)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(6)}},
			},
			{
				Query:    "SELECT * from t2;",
				Expected: []sql.UntypedSqlRow{{1, 1, 1}, {5, nil, 1}, {6, nil, 1}, {7, 1, nil}, {8, 1, nil}, {9, nil, nil}, {10, nil, nil}},
			},
		},
	},
	{
		Name: "Insert throws unique key violations for keyless tables",
		SetUpScript: []string{
			"CREATE TABLE t (not_pk int NOT NULL, col1 int UNIQUE);",
			"CREATE TABLE t2 (not_pk int NOT NULL, col1 int, col2 int, UNIQUE KEY (col1, col2));",
			"INSERT into t VALUES (1, 1);",
			"INSERT into t2 VALUES (1, 1, 1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "INSERT INTO t VALUES (2, 2), (3, 1), (4, 4);",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.UntypedSqlRow{{1, 1}},
			},
			{
				Query:       "INSERT INTO t2 VALUES (2, 2, 2), (3, 1, 1), (4, 4, 4);",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "SELECT * from t2;",
				Expected: []sql.UntypedSqlRow{{1, 1, 1}},
			},
			{
				Query:       "INSERT INTO t VALUES (5, 2), (6, 2);",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "SELECT * from t;",
				Expected: []sql.UntypedSqlRow{{1, 1}},
			},
			{
				Query:       "INSERT INTO t2 VALUES (5, 2, 2), (6, 2, 2);",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "SELECT * from t2;",
				Expected: []sql.UntypedSqlRow{{1, 1, 1}},
			},
			{
				Query:    "INSERT into t2 VALUES (5, NULL, 1), (6, NULL, 1), (7, 1, NULL), (8, 1, NULL), (9, NULL, NULL), (10, NULL, NULL)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(6)}},
			},
			{
				Query:    "SELECT * from t2;",
				Expected: []sql.UntypedSqlRow{{1, 1, 1}, {5, nil, 1}, {6, nil, 1}, {7, 1, nil}, {8, 1, nil}, {9, nil, nil}, {10, nil, nil}},
			},
		},
	},
	{
		Name: "Insert into unique key that overlaps with primary key",
		SetUpScript: []string{
			"CREATE TABLE t (pk1 int, pk2 int, col int, PRIMARY KEY(pk1, pk2), UNIQUE KEY(col, pk2));",
			"INSERT into t (pk1, pk2, col) VALUES (1, 1, 1), (2, 1, 2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "INSERT INTO t (pk1, pk2, col) VALUES (3, 1, 1);",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:       "UPDATE t SET col = col + 1",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
		},
	},
	{
		Name: "INSERT INTO ... SELECT works properly with ENUM",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY NOT NULL, v1 ENUM('a','b','c'));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO test (pk, v1) VALUES (1, 'a');",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "INSERT INTO test (pk, v1) SELECT 2 as pk, 'a' as v1;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
		},
	},
	{
		Name: "INSERT INTO ... SELECT works properly with SET",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY NOT NULL, v1 SET('a','b','c'));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO test (pk, v1) VALUES (1, 'a');",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "INSERT INTO test (pk, v1) SELECT 2 as pk, 'a' as v1;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
		},
	},
	{
		Name: "INSERT INTO ... SELECT with TEXT types",
		SetUpScript: []string{
			"create table t1 (i int primary key, t text);",
			"insert into t1 values (1, '2001-01-01'), (2, 'badtime'), (3, '');",
			"create table t2 (d datetime);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "insert into t2(d) select t from t1 where false;",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query:          "insert into t2(d) select t from t1 where i = 3;",
				ExpectedErrStr: "Incorrect datetime value: ''",
			},
			{
				Query:          "insert into t2(d) select t from t1 where i = 2;",
				ExpectedErrStr: "Incorrect datetime value: 'badtime'",
			},
			{
				Query: "insert into t2(d) select t from t1 where i = 1;",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from t2;",
				Expected: []sql.UntypedSqlRow{
					{time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC)},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/5411
		Name: "Defaults with escaped strings",
		SetUpScript: []string{
			`CREATE TABLE escpe (
                               id int NOT NULL AUTO_INCREMENT,
                               t1 varchar(15) DEFAULT 'foo''s baz',
                               t2 varchar(15) DEFAULT 'who\'s dat',
                               t3 varchar(15) DEFAULT "joe\'s bar",
                               t4 varchar(15) DEFAULT "quote""bazzar",
                               t5 varchar(15) DEFAULT 'back\\''slash',
                               t6 varchar(15) DEFAULT 'tab\ttab',
                               t7 varchar(15) DEFAULT 'new\nline',
                               PRIMARY KEY (id)
                     );`,
			"INSERT INTO escpe VALUES ();",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT t1 from escpe",
				Expected: []sql.UntypedSqlRow{{"foo's baz"}},
			},
			{
				Query:    "SELECT t2 from escpe",
				Expected: []sql.UntypedSqlRow{{"who's dat"}},
			},
			{
				Query:    "SELECT t3 from escpe",
				Expected: []sql.UntypedSqlRow{{"joe's bar"}},
			},
			{
				Query:    "SELECT t4 from escpe",
				Expected: []sql.UntypedSqlRow{{"quote\"bazzar"}},
			},
			{
				Query:    "SELECT t5 from escpe",
				Expected: []sql.UntypedSqlRow{{"back\\'slash"}},
			},
			{
				Query:    "SELECT t6 from escpe",
				Expected: []sql.UntypedSqlRow{{"tab\ttab"}},
			},
			{
				Query:    "SELECT t7 from escpe",
				Expected: []sql.UntypedSqlRow{{"new\nline"}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/5411
		Name: "check constrains with escaped strings",
		SetUpScript: []string{
			`CREATE TABLE quoted ( id int NOT NULL AUTO_INCREMENT,
                                   val varchar(15) NOT NULL CHECK (val IN ('joe''s',
                                                                           "jan's",
                                                                           'mia\\''s',
                                                                           'bob\'s',
                                                                           'tab\tvs\tcoke',
                                                                           'percent\%')),
                                   PRIMARY KEY (id));`,
			`INSERT INTO quoted VALUES (0,"joe's");`,
			`INSERT INTO quoted VALUES (0,"jan's");`,
			`INSERT INTO quoted VALUES (0,"mia\\'s");`,
			`INSERT INTO quoted VALUES (0,"bob's");`,
			`INSERT INTO quoted VALUES (0,"tab\tvs\tcoke");`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT val from quoted order by id",
				Expected: []sql.UntypedSqlRow{
					{"joe's"},
					{"jan's"},
					{"mia\\'s"},
					{"bob's"},
					{"tab\tvs\tcoke"}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/5799
		Name: "check IN TUPLE constraint with duplicate key update",
		SetUpScript: []string{
			"create table alphabet (letter varchar(1), constraint `good_letters` check (letter in ('a','l','e','c')))",
		},
		Assertions: []ScriptTestAssertion{
			{
				// dolt table import with -u option generates a duplicate key update with values(col)
				Query: "insert into alphabet values ('a') on duplicate key update letter = values(letter)",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query:       "insert into alphabet values ('z') on duplicate key update letter = values(letter)",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
		},
	},
	{
		Name: "INSERT IGNORE works with FK Violations",
		SetUpScript: []string{
			"CREATE TABLE t1 (id INT PRIMARY KEY, v int);",
			"CREATE TABLE t2 (id INT PRIMARY KEY, v2 int, CONSTRAINT mfk FOREIGN KEY (v2) REFERENCES t1(id));",
			"INSERT INTO t1 values (1,1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "INSERT IGNORE INTO t2 VALUES (1,2);",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 0}},
				},
				ExpectedWarning: mysql.ErNoReferencedRow2,
			},
		},
	},
	{
		Name: "insert duplicate key doesn't prevent other updates",
		SetUpScript: []string{
			"CREATE TABLE t1 (pk BIGINT PRIMARY KEY, v1 VARCHAR(3));",
			"INSERT INTO t1 VALUES (1, 'abc');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from t1 order by pk",
				Expected: []sql.UntypedSqlRow{{1, "abc"}},
			},
			{
				Query:       "INSERT INTO t1 VALUES (1, 'abc');",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{
				Query:    "INSERT INTO t1 VALUES (2, 'def');",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from t1 order by pk",
				Expected: []sql.UntypedSqlRow{{1, "abc"}, {2, "def"}},
			},
		},
	},
	{
		Name: "insert duplicate key doesn't prevent other updates, autocommit off",
		SetUpScript: []string{
			"CREATE TABLE t1 (pk BIGINT PRIMARY KEY, v1 VARCHAR(3));",
			"INSERT INTO t1 VALUES (1, 'abc');",
			"SET autocommit = 0;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from t1 order by pk",
				Expected: []sql.UntypedSqlRow{{1, "abc"}},
			},
			{
				Query:       "INSERT INTO t1 VALUES (1, 'abc');",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{
				Query:    "INSERT INTO t1 VALUES (2, 'def');",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:            "commit",
				SkipResultsCheck: true,
			},
			{
				Query:    "select * from t1 order by pk",
				Expected: []sql.UntypedSqlRow{{1, "abc"}, {2, "def"}},
			},
		},
	},
}

var InsertDuplicateKeyKeyless = []ScriptTest{
	{
		Name: "insert on duplicate key for keyless table",
		SetUpScript: []string{
			`create table t (i int unique, j varchar(128))`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `insert into t values (0, "first")`,
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: `insert into t values (0, "second") on duplicate key update j = "third"`,
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(2)},
				},
			},
			{
				Query: `select i, j from t order by i`,
				Expected: []sql.UntypedSqlRow{
					{0, "third"},
				},
			},
		},
	},
	{
		Name: "insert on duplicate key for keyless table multiple unique columns",
		SetUpScript: []string{
			`create table t (c1 int, c2 int, c3 int, unique key(c1,c2))`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `insert into t(c1, c2, c3) values (0, 0, 0) on duplicate key update c3 = 0`,
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: `select c1, c2, c3 from t order by c1`,
				Expected: []sql.UntypedSqlRow{
					{0, 0, 0},
				},
			},
			{
				Query: `insert into t(c1, c2, c3) values (0, 0, 1) on duplicate key update c3 = 0`,
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: `select c1, c2, c3 from t order by c1`,
				Expected: []sql.UntypedSqlRow{
					{0, 0, 0},
				},
			},
			{
				Query: `insert into t(c1, c2, c3) values (0, 0, 0) on duplicate key update c3 = 1`,
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(2)},
				},
			},
			{
				Query: `select c1, c2, c3 from t order by c1`,
				Expected: []sql.UntypedSqlRow{
					{0, 0, 1},
				},
			},
		},
	},
	{
		Name: "insert on duplicate key for keyless tables with nulls",
		SetUpScript: []string{
			`create table t (c1 int, c2 int, c3 int, unique key(c1, c2))`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `insert into t(c1, c2, c3) values (0, null, 0) on duplicate key update c3 = 0`,
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: `select c1, c2, c3 from t order by c1`,
				Expected: []sql.UntypedSqlRow{
					{0, nil, 0},
				},
			},
			{
				Query: `insert into t(c1, c2, c3) values (0, null, 1) on duplicate key update c3 = 0`,
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: `select c1, c2, c3 from t order by c1, c2, c3`,
				Expected: []sql.UntypedSqlRow{
					{0, nil, 0},
					{0, nil, 1},
				},
			},
			{
				Query: `insert into t(c1, c2, c3) values (0, null, 0) on duplicate key update c3 = 1`,
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: `select c1, c2, c3 from t order by c1, c2, c3`,
				Expected: []sql.UntypedSqlRow{
					{0, nil, 0},
					{0, nil, 0},
					{0, nil, 1},
				},
			},
			{
				Query: `insert into t(c1, c2, c3) values (0, 0, 0) on duplicate key update c3 = null`,
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: `select c1, c2, c3 from t order by c1, c2, c3`,
				Expected: []sql.UntypedSqlRow{
					{0, nil, 0},
					{0, nil, 0},
					{0, nil, 1},
					{0, 0, 0},
				},
			},
			{
				Query: `insert into t(c1, c2, c3) values (0, 0, 0) on duplicate key update c3 = null`,
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(2)},
				},
			},
			{
				Query: `select c1, c2, c3 from t order by c1, c2, c3`,
				Expected: []sql.UntypedSqlRow{
					{0, nil, 0},
					{0, nil, 0},
					{0, nil, 1},
					{0, 0, nil},
				},
			},
		},
	},
	{
		Name: "insert on duplicate key for keyless table mixed ordering",
		SetUpScript: []string{
			`create table t (c1 int, c2 int, c3 int, unique key(c2, c1))`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `insert into t(c1, c2, c3) values (0, 0, 0) on duplicate key update c3 = 0`,
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: `select c1, c2, c3 from t order by c1`,
				Expected: []sql.UntypedSqlRow{
					{0, 0, 0},
				},
			},
			{
				Query: `insert into t(c1, c2, c3) values (0, 0, 1) on duplicate key update c3 = 0`,
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: `select c1, c2, c3 from t order by c1`,
				Expected: []sql.UntypedSqlRow{
					{0, 0, 0},
				},
			},
			{
				Query: `insert into t(c1, c2, c3) values (0, 0, 0) on duplicate key update c3 = 1`,
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(2)},
				},
			},
			{
				Query: `select c1, c2, c3 from t order by c1`,
				Expected: []sql.UntypedSqlRow{
					{0, 0, 1},
				},
			},
		},
	},
	{
		Name: "insert on duplicate key for keyless table multiple unique columns batched",
		SetUpScript: []string{
			`create table t (c1 int, c2 int, c3 int, unique key(c1,c2))`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `insert into t(c1, c2, c3) values (0, 0, 0), (0, 0, 0), (0, 0, 1), (0, 0, 1) on duplicate key update c3 = 1`,
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(3)},
				},
			},
			{
				Query: `select c1, c2, c3 from t order by c1, c2, c3`,
				Expected: []sql.UntypedSqlRow{
					{0, 0, 1},
				},
			},
			{
				Query: `insert into t(c1, c2, c3) values (0, 0, 1), (0, 0, 2), (0, 0, 3), (0, 0, 4) on duplicate key update c3 = 100`,
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(2)},
				},
			},
			{
				Query: `select c1, c2, c3 from t order by c1, c2, c3`,
				Expected: []sql.UntypedSqlRow{
					{0, 0, 100},
				},
			},
			{
				Query: `insert into t(c1, c2, c3) values (0, 0, 1), (0, 1, 1), (0, 2, 2), (0, 3, 3) on duplicate key update c3 = 200`,
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(5)},
				},
			},
			{
				Query: `select c1, c2, c3 from t order by c1, c2, c3`,
				Expected: []sql.UntypedSqlRow{
					{0, 0, 200},
					{0, 1, 1},
					{0, 2, 2},
					{0, 3, 3},
				},
			},
		},
	},
}

var InsertErrorTests = []GenericErrorQueryTest{
	{
		Name:  "try to insert empty into col without default value",
		Query: "INSERT INTO mytable VALUES ();",
	},
	{
		Name:  "try to insert empty into col without default value",
		Query: "INSERT INTO mytable () VALUES ();",
	},
	{
		Name:  "too few values",
		Query: "INSERT INTO mytable (s, i) VALUES ('x');",
	},
	{
		Name:  "too many values one column",
		Query: "INSERT INTO mytable (s) VALUES ('x', 999);",
	},
	{
		Name:  "missing binding",
		Query: "INSERT INTO mytable (s) VALUES (?);",
	},
	{
		Name:  "too many values two columns",
		Query: "INSERT INTO mytable (i, s) VALUES (999, 'x', 'y');",
	},
	{
		Name:  "too few values no columns specified",
		Query: "INSERT INTO mytable VALUES (999);",
	},
	{
		Name:  "too many values no columns specified",
		Query: "INSERT INTO mytable VALUES (999, 'x', 'y');",
	},
	{
		Name:  "non-existent column values",
		Query: "INSERT INTO mytable (i, s, z) VALUES (999, 'x', 999);",
	},
	{
		Name:  "non-existent column set",
		Query: "INSERT INTO mytable SET i = 999, s = 'x', z = 999;",
	},
	{
		Name:  "duplicate column",
		Query: "INSERT INTO mytable (i, s, s) VALUES (999, 'x', 'x');",
	},
	{
		Name:  "duplicate column set",
		Query: "INSERT INTO mytable SET i = 999, s = 'y', s = 'y';",
	},
	{
		Name:  "null given to non-nullable",
		Query: "INSERT INTO mytable (i, s) VALUES (null, 'y');",
	},
	{
		Name:  "incompatible types",
		Query: "INSERT INTO mytable (i, s) select * FROM othertable",
	},
	{
		Name:  "column count mismatch in select",
		Query: "INSERT INTO mytable (i) select * FROM othertable",
	},
	{
		Name:  "column count mismatch in select",
		Query: "INSERT INTO mytable select s FROM othertable",
	},
	{
		Name:  "column count mismatch in join select",
		Query: "INSERT INTO mytable (s,i) SELECT * FROM othertable o JOIN mytable m ON m.i=o.i2",
	},
	{
		Name:  "duplicate key",
		Query: "INSERT INTO mytable (i,s) values (1, 'hello')",
	},
	{
		Name:  "duplicate keys",
		Query: "INSERT INTO mytable SELECT * from mytable",
	},
	{
		Name:  "bad column in on duplicate key update clause",
		Query: "INSERT INTO mytable values (10, 'b') ON DUPLICATE KEY UPDATE notExist = 1",
	},
}

var InsertErrorScripts = []ScriptTest{
	{
		Name:        "create table with non-pk auto_increment column",
		Query:       "create table bad (pk int primary key, c0 int auto_increment);",
		ExpectedErr: sql.ErrInvalidAutoIncCols,
	},
	{
		Name:        "create multiple auto_increment columns",
		Query:       "create table bad (pk1 int auto_increment, pk2 int auto_increment, primary key (pk1,pk2));",
		ExpectedErr: sql.ErrInvalidAutoIncCols,
	},
	{
		Name:        "create auto_increment column with default",
		Query:       "create table bad (pk1 int auto_increment default 10, c0 int);",
		ExpectedErr: sql.ErrInvalidAutoIncCols,
	},
	{
		Name: "try inserting string that is too long",
		SetUpScript: []string{
			"create table bad (s varchar(9))",
		},
		Query:       "insert into bad values ('1234567890')",
		ExpectedErr: types.ErrLengthBeyondLimit,
	},
	{
		Name: "try inserting varbinary larger than max limit",
		SetUpScript: []string{
			"create table bad (vb varbinary(65535))",
		},
		Query:       "insert into bad values (repeat('0', 65536))",
		ExpectedErr: types.ErrLengthBeyondLimit,
	},
}

var InsertIgnoreScripts = []ScriptTest{
	{
		Name: "Test that INSERT IGNORE with Non nullable columns works",
		SetUpScript: []string{
			"CREATE TABLE x (pk int primary key, c1 varchar(20) NOT NULL);",
			"INSERT IGNORE INTO x VALUES (1, NULL)",
			"CREATE TABLE y (pk int primary key, c1 int NOT NULL);",
			"INSERT IGNORE INTO y VALUES (1, NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM x",
				Expected: []sql.UntypedSqlRow{
					{1, ""},
				},
			},
			{
				Query: "SELECT * FROM y",
				Expected: []sql.UntypedSqlRow{
					{1, 0},
				},
			},
			{
				Query: "INSERT IGNORE INTO y VALUES (2, NULL)",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1}},
				},
				ExpectedWarning: mysql.ERBadNullError,
			},
		},
	},
	{
		Name: "Test that INSERT IGNORE properly addresses data conversion",
		SetUpScript: []string{
			"CREATE TABLE t1 (pk int primary key, v1 int)",
			"CREATE TABLE t2 (pk int primary key, v2 varchar(1))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "INSERT IGNORE INTO t1 VALUES (1, 'dasd')",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1}},
				},
				ExpectedWarning: mysql.ERTruncatedWrongValueForField,
			},
			{
				Query: "SELECT * FROM t1",
				Expected: []sql.UntypedSqlRow{
					{1, 0},
				},
			},
			{
				Query: "INSERT IGNORE INTO t2 values (1, 'adsda')",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1}},
				},
				ExpectedWarning: mysql.ERUnknownError,
			},
			{
				Query: "SELECT * FROM t2",
				Expected: []sql.UntypedSqlRow{
					{1, "a"},
				},
			},
		},
	},
	{
		Name: "Insert Ignore works correctly with ON DUPLICATE UPDATE",
		SetUpScript: []string{
			"CREATE TABLE t1 (id INT PRIMARY KEY, v int);",
			"INSERT INTO t1 VALUES (1,1)",
			"CREATE TABLE t2 (pk int primary key, v2 varchar(1))",
			"ALTER TABLE t2 ADD CONSTRAINT cx CHECK (pk < 100)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "INSERT IGNORE INTO t1 VALUES (1,2) ON DUPLICATE KEY UPDATE v='dsd';",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 2}},
				},
				ExpectedWarning: mysql.ERTruncatedWrongValueForField,
			},
			{
				Query: "SELECT * FROM t1",
				Expected: []sql.UntypedSqlRow{
					{1, 0},
				},
			},
			{
				Query: "INSERT IGNORE INTO t2 values (1, 'adsda')",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1}},
				},
				ExpectedWarning: mysql.ERUnknownError,
			},
			{
				Query: "SELECT * FROM t2",
				Expected: []sql.UntypedSqlRow{
					{1, "a"},
				},
			},
			{
				Query:    "INSERT IGNORE INTO t2 VALUES (1, 's') ON DUPLICATE KEY UPDATE pk = 1000", // violates constraint
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 0}}},
			},
			{
				Query: "SELECT * FROM t2",
				Expected: []sql.UntypedSqlRow{
					{1, "a"},
				},
			},
		},
	},
	{
		Name: "Test that INSERT IGNORE INTO works with unique keys",
		SetUpScript: []string{
			"CREATE TABLE one_uniq(pk int PRIMARY KEY, col1 int UNIQUE)",
			"CREATE TABLE two_uniq(pk int PRIMARY KEY, col1 int, col2 int, UNIQUE KEY col1_col2_uniq (col1, col2))",
			"INSERT INTO one_uniq values (1, 1)",
			"INSERT INTO two_uniq values (1, 1, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "INSERT IGNORE INTO one_uniq VALUES (3, 2), (2, 1), (4, null), (5, null)",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 3}},
				},
				ExpectedWarning: mysql.ERDupEntry,
			},
			{
				Query: "SELECT * from one_uniq;",
				Expected: []sql.UntypedSqlRow{
					{1, 1}, {3, 2}, {4, nil}, {5, nil},
				},
			},
			{
				Query: "INSERT IGNORE INTO two_uniq VALUES (4, 1, 2), (5, 2, 1), (6, null, 1), (7, null, 1), (12, 1, 1), (8, 1, null), (9, 1, null), (10, null, null), (11, null, null)",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 8}},
				},
				ExpectedWarning: mysql.ERDupEntry,
			},
			{
				Query: "SELECT * from two_uniq;",
				Expected: []sql.UntypedSqlRow{
					{1, 1, 1}, {4, 1, 2}, {5, 2, 1}, {6, nil, 1}, {7, nil, 1}, {8, 1, nil}, {9, 1, nil}, {10, nil, nil}, {11, nil, nil},
				},
			},
		},
	},
}

var IgnoreWithDuplicateUniqueKeyKeylessScripts = []ScriptTest{
	{
		Name: "Test that INSERT IGNORE INTO works with unique keys on a keyless table",
		SetUpScript: []string{
			"CREATE TABLE one_uniq(not_pk int, value int UNIQUE)",
			"CREATE TABLE two_uniq(not_pk int, col1 int, col2 int, UNIQUE KEY col1_col2_uniq (col1, col2));",
			"INSERT INTO one_uniq values (1, 1)",
			"INSERT INTO two_uniq values (1, 1, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "INSERT IGNORE INTO one_uniq VALUES (3, 2), (2, 1), (4, null), (5, null)",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 3}},
				},
				ExpectedWarning: mysql.ERDupEntry,
			},
			{
				Query: "SELECT * from one_uniq;",
				Expected: []sql.UntypedSqlRow{
					{1, 1}, {3, 2}, {4, nil}, {5, nil},
				},
			},
			{
				Query: "INSERT IGNORE INTO two_uniq VALUES (4, 1, 2), (5, 2, 1), (6, null, 1), (7, null, 1), (12, 1, 1), (8, 1, null), (9, 1, null), (10, null, null), (11, null, null)",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 8}},
				},
				ExpectedWarning: mysql.ERDupEntry,
			},
			{
				Query: "SELECT * from two_uniq;",
				Expected: []sql.UntypedSqlRow{
					{1, 1, 1}, {4, 1, 2}, {5, 2, 1}, {6, nil, 1}, {7, nil, 1}, {8, 1, nil}, {9, 1, nil}, {10, nil, nil}, {11, nil, nil},
				},
			},
		},
	},
	{
		Name: "INSERT IGNORE INTO multiple violations of a unique secondary index",
		SetUpScript: []string{
			"CREATE TABLE keyless(pk int, val int)",
			"INSERT INTO keyless values (1, 1), (2, 2), (3, 3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT IGNORE INTO keyless VALUES (1, 2);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:       "ALTER TABLE keyless ADD CONSTRAINT c UNIQUE(val)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "DELETE FROM keyless where pk = 1 and val = 2",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "ALTER TABLE keyless ADD CONSTRAINT c UNIQUE(val)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:           "INSERT IGNORE INTO keyless VALUES (1, 3)",
				Expected:        []sql.UntypedSqlRow{{types.NewOkResult(0)}},
				ExpectedWarning: mysql.ERDupEntry,
			},
		},
	},
	{
		Name: "UPDATE IGNORE keyless tables and secondary indexes",
		SetUpScript: []string{
			"CREATE TABLE keyless(pk int, val int)",
			"INSERT INTO keyless VALUES (1, 1), (2, 2), (3, 3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "UPDATE IGNORE keyless SET val = 2 where pk = 1",
				Expected: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
			},
			{
				Query:    "SELECT * FROM keyless ORDER BY pk",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 2}, {3, 3}},
			},
			{
				Query:       "ALTER TABLE keyless ADD CONSTRAINT c UNIQUE(val)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:           "UPDATE IGNORE keyless SET val = 1 where pk = 1",
				Expected:        []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
				ExpectedWarning: mysql.ERDupEntry,
			},
			{
				Query:    "ALTER TABLE keyless ADD CONSTRAINT c UNIQUE(val)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:           "UPDATE IGNORE keyless SET val = 3 where pk = 1",
				Expected:        []sql.UntypedSqlRow{{newUpdateResult(1, 0)}},
				ExpectedWarning: mysql.ERDupEntry,
			},
			{
				Query:    "SELECT * FROM keyless ORDER BY pk",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}},
			},
			{
				Query:           "UPDATE IGNORE keyless SET val = val + 1 ORDER BY pk",
				Expected:        []sql.UntypedSqlRow{{newUpdateResult(3, 1)}},
				ExpectedWarning: mysql.ERDupEntry,
			},
			{
				Query:    "SELECT * FROM keyless ORDER BY pk",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 4}},
			},
		},
	},
}

var InsertBrokenScripts = []ScriptTest{
	// TODO: Condense all of our casting logic into a single error.
	{
		Name: "Test that INSERT IGNORE assigns the closest dataype correctly",
		SetUpScript: []string{
			"CREATE TABLE x (pk int primary key, c1 varchar(20) NOT NULL);",
			`INSERT IGNORE INTO x VALUES (1, "one"), (2, TRUE), (3, "three")`,
			"CREATE TABLE y (pk int primary key, c1 int NOT NULL);",
			`INSERT IGNORE INTO y VALUES (1, 1), (2, "two"), (3,3);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM x",
				Expected: []sql.UntypedSqlRow{
					{1, "one"}, {2, 1}, {3, "three"},
				},
			},
			{
				Query: "SELECT * FROM y",
				Expected: []sql.UntypedSqlRow{
					{1, 1}, {2, 0}, {3, 3},
				},
			},
			{
				Query: `INSERT IGNORE INTO y VALUES (4, "four")`,
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1}},
				},
				ExpectedWarning: mysql.ERTruncatedWrongValueForField,
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/3157
		Name: "auto increment does not increment on error",
		SetUpScript: []string{
			"create table auto1 (pk int primary key auto_increment);",
			"insert into auto1 values (null);",
			"create table auto2 (pk int primary key auto_increment, c int not null);",
			"insert into auto2 values (null, 1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table auto1;",
				Expected: []sql.UntypedSqlRow{
					{"auto1", "CREATE TABLE `auto1` (\n" +
						"  `pk` int NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query:       "insert into auto1 values (1);",
				ExpectedErr: sql.ErrPrimaryKeyViolation,
			},
			{
				Query: "show create table auto1;",
				Expected: []sql.UntypedSqlRow{
					{"auto1", "CREATE TABLE `auto1` (\n" +
						"  `pk` int NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into auto1 values (null);",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1, InsertID: 2}},
				},
			},
			{
				Query: "show create table auto1;",
				Expected: []sql.UntypedSqlRow{
					{"auto1", "CREATE TABLE `auto1` (\n" +
						"  `pk` int NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "select * from auto1;",
				Expected: []sql.UntypedSqlRow{
					{1},
					{2},
				},
			},

			{
				Query: "show create table auto2;",
				Expected: []sql.UntypedSqlRow{
					{"auto2", "CREATE TABLE `auto2` (\n" +
						"  `pk` int NOT NULL AUTO_INCREMENT,\n" +
						"  `c` int NOT NULL,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query:       "insert into auto2 values (null, null);",
				ExpectedErr: sql.ErrInsertIntoNonNullableProvidedNull,
			},
			{
				Query: "show create table auto2;",
				Expected: []sql.UntypedSqlRow{
					{"auto2", "CREATE TABLE `auto2` (\n" +
						"  `pk` int NOT NULL AUTO_INCREMENT,\n" +
						"  `c` int NOT NULL,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into auto2 values (null, 2);",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1, InsertID: 2}},
				},
			},
			{
				Query: "show create table auto2;",
				Expected: []sql.UntypedSqlRow{
					{"auto2", "CREATE TABLE `auto2` (\n" +
						"  `pk` int NOT NULL AUTO_INCREMENT,\n" +
						"  `c` int NOT NULL,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "select * from auto2;",
				Expected: []sql.UntypedSqlRow{
					{1, 1},
					{2, 2},
				},
			},
		},
	},
}
