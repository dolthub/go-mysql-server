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

	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
)

var InsertQueries = []WriteQueryTest{
	{
		WriteQuery:          "INSERT INTO keyless VALUES ();",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM keyless WHERE c0 IS NULL;",
		ExpectedSelect:      []sql.Row{{nil, nil}},
	},
	{
		WriteQuery:          "INSERT INTO keyless () VALUES ();",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM keyless WHERE c0 IS NULL;",
		ExpectedSelect:      []sql.Row{{nil, nil}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (s, i) VALUES ('x', '10.0');",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT i FROM mytable WHERE s = 'x';",
		ExpectedSelect:      []sql.Row{{int64(10)}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (s, i) VALUES ('x', '64.6');",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT i FROM mytable WHERE s = 'x';",
		ExpectedSelect:      []sql.Row{{int64(64)}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (s, i) VALUES ('x', 999);",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT i FROM mytable WHERE s = 'x';",
		ExpectedSelect:      []sql.Row{{int64(999)}},
	},
	{
		WriteQuery:          "INSERT INTO niltable (i, f) VALUES (10, 10.0), (12, 12.0);",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         "SELECT i,f FROM niltable WHERE f IN (10.0, 12.0) ORDER BY f;",
		ExpectedSelect:      []sql.Row{{int64(10), 10.0}, {int64(12), 12.0}},
	},
	{
		WriteQuery:          "INSERT INTO mytable SET s = 'x', i = 999;",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT i FROM mytable WHERE s = 'x';",
		ExpectedSelect:      []sql.Row{{int64(999)}},
	},
	{
		WriteQuery:          "INSERT INTO mytable VALUES (999, 'x');",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT i FROM mytable WHERE s = 'x';",
		ExpectedSelect:      []sql.Row{{int64(999)}},
	},
	{
		WriteQuery:          "INSERT INTO mytable SET i = 999, s = 'x';",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT i FROM mytable WHERE s = 'x';",
		ExpectedSelect:      []sql.Row{{int64(999)}},
	},
	{
		WriteQuery:          "INSERT INTO mytable VALUES (999, _binary 'x');",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT s FROM mytable WHERE i = 999;",
		ExpectedSelect:      []sql.Row{{"x"}},
	},
	{
		WriteQuery:          "INSERT INTO mytable SET i = 999, s = _binary 'x';",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT s FROM mytable WHERE i = 999;",
		ExpectedSelect:      []sql.Row{{"x"}},
	},
	{
		WriteQuery: `INSERT INTO typestable VALUES (
			999, 127, 32767, 2147483647, 9223372036854775807,
			255, 65535, 4294967295, 18446744073709551615,
			3.40282346638528859811704183484516925440e+38, 1.797693134862315708145274237317043567981e+308,
			'2037-04-05 12:51:36', '2231-11-07',
			'random text', true, '{"key":"value"}', 'blobdata'
			);`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect: []sql.Row{{
			int64(999), int8(math.MaxInt8), int16(math.MaxInt16), int32(math.MaxInt32), int64(math.MaxInt64),
			uint8(math.MaxUint8), uint16(math.MaxUint16), uint32(math.MaxUint32), uint64(math.MaxUint64),
			float32(math.MaxFloat32), float64(math.MaxFloat64),
			sql.MustConvert(sql.Timestamp.Convert("2037-04-05 12:51:36")), sql.MustConvert(sql.Date.Convert("2231-11-07")),
			"random text", sql.True, sql.MustJSON(`{"key":"value"}`), []byte("blobdata"),
		}},
	},
	{
		WriteQuery: `INSERT INTO typestable SET
			id = 999, i8 = 127, i16 = 32767, i32 = 2147483647, i64 = 9223372036854775807,
			u8 = 255, u16 = 65535, u32 = 4294967295, u64 = 18446744073709551615,
			f32 = 3.40282346638528859811704183484516925440e+38, f64 = 1.797693134862315708145274237317043567981e+308,
			ti = '2037-04-05 12:51:36', da = '2231-11-07',
			te = 'random text', bo = true, js = '{"key":"value"}', bl = 'blobdata'
			;`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect: []sql.Row{{
			int64(999), int8(math.MaxInt8), int16(math.MaxInt16), int32(math.MaxInt32), int64(math.MaxInt64),
			uint8(math.MaxUint8), uint16(math.MaxUint16), uint32(math.MaxUint32), uint64(math.MaxUint64),
			float32(math.MaxFloat32), float64(math.MaxFloat64),
			sql.MustConvert(sql.Timestamp.Convert("2037-04-05 12:51:36")), sql.MustConvert(sql.Date.Convert("2231-11-07")),
			"random text", sql.True, sql.MustJSON(`{"key":"value"}`), []byte("blobdata"),
		}},
	},
	{
		WriteQuery: `INSERT INTO typestable VALUES (
			999, -128, -32768, -2147483648, -9223372036854775808,
			0, 0, 0, 0,
			1.401298464324817070923729583289916131280e-45, 4.940656458412465441765687928682213723651e-324,
			'0000-00-00 00:00:00', '0000-00-00',
			'', false, '""', ''
			);`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect: []sql.Row{{
			int64(999), int8(-math.MaxInt8 - 1), int16(-math.MaxInt16 - 1), int32(-math.MaxInt32 - 1), int64(-math.MaxInt64 - 1),
			uint8(0), uint16(0), uint32(0), uint64(0),
			float32(math.SmallestNonzeroFloat32), float64(math.SmallestNonzeroFloat64),
			sql.Timestamp.Zero(), sql.Date.Zero(),
			"", sql.False, sql.MustJSON(`""`), []byte(""),
		}},
	},
	{
		WriteQuery: `INSERT INTO typestable SET
			id = 999, i8 = -128, i16 = -32768, i32 = -2147483648, i64 = -9223372036854775808,
			u8 = 0, u16 = 0, u32 = 0, u64 = 0,
			f32 = 1.401298464324817070923729583289916131280e-45, f64 = 4.940656458412465441765687928682213723651e-324,
			ti = '0000-00-00 00:00:00', da = '0000-00-00',
			te = '', bo = false, js = '""', bl = ''
			;`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect: []sql.Row{{
			int64(999), int8(-math.MaxInt8 - 1), int16(-math.MaxInt16 - 1), int32(-math.MaxInt32 - 1), int64(-math.MaxInt64 - 1),
			uint8(0), uint16(0), uint32(0), uint64(0),
			float32(math.SmallestNonzeroFloat32), float64(math.SmallestNonzeroFloat64),
			sql.Timestamp.Zero(), sql.Date.Zero(),
			"", sql.False, sql.MustJSON(`""`), []byte(""),
		}},
	},
	{
		WriteQuery: `INSERT INTO typestable SET
			id = 999, i8 = -128, i16 = -32768, i32 = -2147483648, i64 = -9223372036854775808,
			u8 = 0, u16 = 0, u32 = 0, u64 = 0,
			f32 = 1.401298464324817070923729583289916131280e-45, f64 = 4.940656458412465441765687928682213723651e-324,
			ti = '2037-04-05 12:51:36 -0000 UTC', da = '0000-00-00',
			te = '', bo = false, js = '""', bl = ''
			;`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect: []sql.Row{{
			int64(999), int8(-math.MaxInt8 - 1), int16(-math.MaxInt16 - 1), int32(-math.MaxInt32 - 1), int64(-math.MaxInt64 - 1),
			uint8(0), uint16(0), uint32(0), uint64(0),
			float32(math.SmallestNonzeroFloat32), float64(math.SmallestNonzeroFloat64),
			sql.MustConvert(sql.Timestamp.Convert("2037-04-05 12:51:36")), sql.Date.Zero(),
			"", sql.False, sql.MustJSON(`""`), []byte(""),
		}},
	},
	{
		WriteQuery:          `INSERT INTO mytable (i,s) VALUES (10, 'NULL')`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i = 10;",
		ExpectedSelect:      []sql.Row{{int64(10), "NULL"}},
	},
	{
		WriteQuery: `INSERT INTO typestable VALUES (999, null, null, null, null, null, null, null, null,
			null, null, null, null, null, null, null, null);`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect:      []sql.Row{{int64(999), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}},
	},
	{
		WriteQuery:          `INSERT INTO typestable (id, ti, da) VALUES (999, '2021-09-1', '2021-9-01');`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT id, ti, da FROM typestable WHERE id = 999;",
		ExpectedSelect:      []sql.Row{{int64(999), sql.MustConvert(sql.Timestamp.Convert("2021-09-01")), sql.MustConvert(sql.Date.Convert("2021-09-01"))}},
	},
	{
		WriteQuery: `INSERT INTO typestable SET id=999, i8=null, i16=null, i32=null, i64=null, u8=null, u16=null, u32=null, u64=null,
			f32=null, f64=null, ti=null, da=null, te=null, bo=null, js=null, bl=null;`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM typestable WHERE id = 999;",
		ExpectedSelect:      []sql.Row{{int64(999), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}},
	},
	{
		WriteQuery:          "INSERT INTO mytable SELECT i+100,s FROM mytable",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY i",
		ExpectedSelect: []sql.Row{
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
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM emptytable ORDER BY i",
		ExpectedSelect: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
	},
	{
		WriteQuery:          "INSERT INTO emptytable SELECT * FROM mytable where mytable.i > 2",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM emptytable ORDER BY i",
		ExpectedSelect: []sql.Row{
			{int64(3), "third row"},
		},
	},
	{
		WriteQuery:          "INSERT INTO niltable (i,f) SELECT i+10, NULL FROM mytable where mytable.i > 2",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM niltable where i > 10 ORDER BY i",
		ExpectedSelect: []sql.Row{
			{13, nil, nil, nil},
		},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) SELECT i+10, 'new' FROM mytable",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY i",
		ExpectedSelect: []sql.Row{
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
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY i,s",
		ExpectedSelect: []sql.Row{
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
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM emptytable ORDER BY i,s",
		ExpectedSelect: []sql.Row{
			{int64(1), "third"},
			{int64(2), "second"},
			{int64(3), "first"},
		},
	},
	{
		WriteQuery:          "INSERT INTO emptytable (s,i) SELECT concat(m.s, o.s2), m.i FROM othertable o JOIN mytable m ON m.i=o.i2",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM emptytable ORDER BY i,s",
		ExpectedSelect: []sql.Row{
			{int64(1), "first rowthird"},
			{int64(2), "second rowsecond"},
			{int64(3), "third rowfirst"},
		},
	},
	{
		WriteQuery: `INSERT INTO emptytable (s,i) SELECT s,i from mytable where i = 1 
			union select s,i from mytable where i = 3`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM emptytable ORDER BY i,s",
		ExpectedSelect: []sql.Row{
			{int64(1), "first row"},
			{int64(3), "third row"},
		},
	},
	{
		WriteQuery: `INSERT INTO emptytable (s,i) SELECT s,i from mytable where i = 1 
			union select s,i from mytable where i = 3 
			union select s,i from mytable where i > 2`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM emptytable ORDER BY i,s",
		ExpectedSelect: []sql.Row{
			{int64(1), "first row"},
			{int64(3), "third row"},
		},
	},
	{
		WriteQuery: `INSERT INTO emptytable (s,i) 
			SELECT s,i from mytable where i = 1 
			union all select s,i+1 from mytable where i < 2 
			union all select s,i+2 from mytable where i in (1)`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM emptytable ORDER BY i,s",
		ExpectedSelect: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "first row"},
			{int64(3), "first row"},
		},
	},
	{
		WriteQuery:          "INSERT INTO emptytable (s,i) SELECT distinct s,i from mytable",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM emptytable ORDER BY i,s",
		ExpectedSelect: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) SELECT (i + 10.0) / 10.0 + 10 + i, concat(s, ' new') FROM mytable",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY i, s",
		ExpectedSelect: []sql.Row{
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
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY i, s",
		ExpectedSelect: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
			{9, "numrows: 2"},
			{10, "numrows: 1"},
		},
	},
	// TODO: this doesn't match MySQL. MySQL requires giving an alias to the expression to use it in a HAVING clause,
	//  but that causes an error in our engine. Needs work
	{
		WriteQuery:          "INSERT INTO mytable (i,s) SELECT CHAR_LENGTH(s), concat('numrows: ', count(*)) from mytable group by 1 HAVING CHAR_LENGTH(s)  > 9",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY i, s",
		ExpectedSelect: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
			{10, "numrows: 1"},
		},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) SELECT i * 2, concat(s,s) from mytable order by 1 desc limit 1",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY i, s",
		ExpectedSelect: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
			{6, "third rowthird row"},
		},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) SELECT i + 3, concat(s,s) from mytable order by 1 desc",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY i, s",
		ExpectedSelect: []sql.Row{
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
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable where i > 10 ORDER BY i, s",
		ExpectedSelect: []sql.Row{
			{11, "third"},
			{12, "second"},
			{13, "first"},
		},
	},
	{
		WriteQuery: `INSERT INTO mytable (i,s) SELECT sub.i + 10, ot.s2 
				FROM (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub
				INNER JOIN othertable ot ON sub.i = ot.i2 order by 1`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(3)}},
		SelectQuery:         "SELECT * FROM mytable where i > 10 ORDER BY i, s",
		ExpectedSelect: []sql.Row{
			{11, "third"},
			{12, "second"},
			{13, "first"},
		},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (1, 'hello') ON DUPLICATE KEY UPDATE s='hello'",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i = 1",
		ExpectedSelect:      []sql.Row{{int64(1), "hello"}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (1, 'hello2') ON DUPLICATE KEY UPDATE s='hello3'",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i = 1",
		ExpectedSelect:      []sql.Row{{int64(1), "hello3"}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (1, 'hello') ON DUPLICATE KEY UPDATE i=10",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i = 10",
		ExpectedSelect:      []sql.Row{{int64(10), "first row"}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (1, 'hello2') ON DUPLICATE KEY UPDATE s='hello3'",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i = 1",
		ExpectedSelect:      []sql.Row{{int64(1), "hello3"}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (1, 'hello2'), (2, 'hello3'), (4, 'no conflict') ON DUPLICATE KEY UPDATE s='hello4'",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(5)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY 1",
		ExpectedSelect: []sql.Row{
			{1, "hello4"},
			{2, "hello4"},
			{3, "third row"},
			{4, "no conflict"},
		},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (10, 'hello') ON DUPLICATE KEY UPDATE s='hello'",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM mytable ORDER BY 1",
		ExpectedSelect: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
			{10, "hello"},
		},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (1,'hi') ON DUPLICATE KEY UPDATE s=VALUES(s)",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i = 1",
		ExpectedSelect:      []sql.Row{{int64(1), "hi"}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (s,i) values ('dup',1) ON DUPLICATE KEY UPDATE s=CONCAT(VALUES(s), 'licate')",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i = 1",
		ExpectedSelect:      []sql.Row{{int64(1), "duplicate"}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (1,'mar'), (2,'par') ON DUPLICATE KEY UPDATE s=CONCAT(VALUES(s), 'tial')",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(4)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i IN (1,2) ORDER BY i",
		ExpectedSelect:      []sql.Row{{int64(1), "martial"}, {int64(2), "partial"}},
	},
	{
		WriteQuery:          "INSERT INTO mytable (i,s) values (1,'maybe') ON DUPLICATE KEY UPDATE i=VALUES(i)+8000, s=VALUES(s)",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM mytable WHERE i = 8001",
		ExpectedSelect:      []sql.Row{{int64(8001), "maybe"}},
	},
	{
		WriteQuery:          "INSERT INTO auto_increment_tbl (c0) values (44)",
		ExpectedWriteResult: []sql.Row{{sql.OkResult{RowsAffected: 1, InsertID: 4}}},
		SelectQuery:         "SELECT * FROM auto_increment_tbl ORDER BY pk",
		ExpectedSelect: []sql.Row{
			{1, 11},
			{2, 22},
			{3, 33},
			{4, 44},
		},
	},
	{
		WriteQuery:          "INSERT INTO auto_increment_tbl (c0) values (44),(55)",
		ExpectedWriteResult: []sql.Row{{sql.OkResult{RowsAffected: 2, InsertID: 4}}},
		SelectQuery:         "SELECT * FROM auto_increment_tbl ORDER BY pk",
		ExpectedSelect: []sql.Row{
			{1, 11},
			{2, 22},
			{3, 33},
			{4, 44},
			{5, 55},
		},
	},
	{
		WriteQuery:          "INSERT INTO auto_increment_tbl values (NULL, 44)",
		ExpectedWriteResult: []sql.Row{{sql.OkResult{RowsAffected: 1, InsertID: 4}}},
		SelectQuery:         "SELECT * FROM auto_increment_tbl ORDER BY pk",
		ExpectedSelect: []sql.Row{
			{1, 11},
			{2, 22},
			{3, 33},
			{4, 44},
		},
	},
	{
		WriteQuery:          "INSERT INTO auto_increment_tbl values (0, 44)",
		ExpectedWriteResult: []sql.Row{{sql.OkResult{RowsAffected: 1, InsertID: 4}}},
		SelectQuery:         "SELECT * FROM auto_increment_tbl ORDER BY pk",
		ExpectedSelect: []sql.Row{
			{1, 11},
			{2, 22},
			{3, 33},
			{4, 44},
		},
	},
	{
		WriteQuery:          "INSERT INTO auto_increment_tbl values (5, 44)",
		ExpectedWriteResult: []sql.Row{{sql.OkResult{RowsAffected: 1, InsertID: 5}}},
		SelectQuery:         "SELECT * FROM auto_increment_tbl ORDER BY pk",
		ExpectedSelect: []sql.Row{
			{1, 11},
			{2, 22},
			{3, 33},
			{5, 44},
		},
	},
	{
		WriteQuery: "INSERT INTO auto_increment_tbl values " +
			"(NULL, 44), (NULL, 55), (9, 99), (NULL, 110), (NULL, 121)",
		ExpectedWriteResult: []sql.Row{{sql.OkResult{RowsAffected: 5, InsertID: 4}}},
		SelectQuery:         "SELECT * FROM auto_increment_tbl ORDER BY pk",
		ExpectedSelect: []sql.Row{
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
		ExpectedWriteResult: []sql.Row{{sql.OkResult{RowsAffected: 1, InsertID: 4}}},
		SelectQuery:         "SELECT * FROM auto_increment_tbl",
		ExpectedSelect: []sql.Row{
			{1, 11},
			{2, 22},
			{3, 33},
			{4, 44},
		},
	},
	{
		WriteQuery:          `INSERT INTO othertable VALUES ("fourth", 1) ON DUPLICATE KEY UPDATE s2="fourth"`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         "SELECT * FROM othertable",
		ExpectedSelect: []sql.Row{
			sql.NewRow("first", int64(3)),
			sql.NewRow("second", int64(2)),
			sql.NewRow("fourth", int64(1)),
		},
	},
	{
		WriteQuery:          `INSERT INTO othertable(S2,I2) values ('fourth',0)`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         `SELECT * FROM othertable where s2='fourth'`,
		ExpectedSelect: []sql.Row{
			{"fourth", 0},
		},
	},
	{
		WriteQuery: `INSERT INTO auto_increment_tbl VALUES ('4', 44)`,
		ExpectedWriteResult: []sql.Row{
			{sql.OkResult{InsertID: 4, RowsAffected: 1}},
		},
		SelectQuery: `SELECT * from auto_increment_tbl where pk=4`,
		ExpectedSelect: []sql.Row{
			{4, 44},
		},
	},
	{
		WriteQuery:          `INSERT INTO keyless (c0, c1) SELECT * from keyless where c0=0 and c1=0`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         `SELECT * from keyless where c0=0`,
		ExpectedSelect: []sql.Row{
			{0, 0},
			{0, 0},
		},
	},
	{
		WriteQuery:          `insert into keyless (c0, c1) select a.c0, a.c1 from (select 1, 1) as a(c0, c1) join keyless on a.c0 = keyless.c0`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
		SelectQuery:         `SELECT * from keyless where c0=1`,
		ExpectedSelect: []sql.Row{
			{1, 1},
			{1, 1},
			{1, 1},
			{1, 1},
		},
	},
}

var SpatialInsertQueries = []WriteQueryTest{
	{
		WriteQuery:          "INSERT INTO point_table VALUES (1, POINT(1,1));",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM point_table;",
		ExpectedSelect:      []sql.Row{{5, sql.Point{X: 1, Y: 2}}, {1, sql.Point{X: 1, Y: 1}}},
	},
	{
		WriteQuery:          "INSERT INTO point_table VALUES (1, 0x000000000101000000000000000000F03F0000000000000040);",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM point_table;",
		ExpectedSelect:      []sql.Row{{5, sql.Point{X: 1, Y: 2}}, {1, sql.Point{X: 1, Y: 2}}},
	},
	{
		WriteQuery:          "INSERT INTO line_table VALUES (2, LINESTRING(POINT(1,2),POINT(3,4)));",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM line_table;",
		ExpectedSelect:      []sql.Row{{0, sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}}, {1, sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}, {X: 5, Y: 6}}}}, {2, sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}}},
	},
	{
		WriteQuery:          "INSERT INTO line_table VALUES (2, 0x00000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040);",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM line_table;",
		ExpectedSelect:      []sql.Row{{0, sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}}, {1, sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}, {X: 5, Y: 6}}}}, {2, sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}}},
	},
	{
		WriteQuery:          "INSERT INTO polygon_table VALUES (2, POLYGON(LINESTRING(POINT(1,1),POINT(1,-1),POINT(-1,-1),POINT(-1,1),POINT(1,1))));",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM polygon_table;",
		ExpectedSelect: []sql.Row{
			{0, sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{1, sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}, {Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{2, sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 1, Y: 1}, {X: 1, Y: -1}, {X: -1, Y: -1}, {X: -1, Y: 1}, {X: 1, Y: 1}}}}}},
		},
	},
	{
		WriteQuery:          "INSERT INTO polygon_table VALUES (2, 0x0000000001030000000100000005000000000000000000F03F000000000000F03F000000000000F03F000000000000F0BF000000000000F0BF000000000000F0BF000000000000F0BF000000000000F03F000000000000F03F000000000000F03F);",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM polygon_table;",
		ExpectedSelect: []sql.Row{
			{0, sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{1, sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}, {Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{2, sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 1, Y: 1}, {X: 1, Y: -1}, {X: -1, Y: -1}, {X: -1, Y: 1}, {X: 1, Y: 1}}}}}}},
	},
	{
		WriteQuery:          "INSERT INTO geometry_table VALUES (7, POINT(123.456,7.89));",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM geometry_table;",
		ExpectedSelect: []sql.Row{
			{1, sql.Point{X: 1, Y: 2}},
			{2, sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{3, sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{4, sql.Point{SRID: 4326, X: 1, Y: 2}},
			{5, sql.LineString{SRID: 4326, Points: []sql.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{6, sql.Polygon{SRID: 4326, Lines: []sql.LineString{{SRID: 4326, Points: []sql.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}},
			{7, sql.Point{X: 123.456, Y: 7.89}},
		},
	},
	{
		WriteQuery:          "INSERT INTO geometry_table VALUES (7, 0x00000000010100000077BE9F1A2FDD5E408FC2F5285C8F1F40);",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM geometry_table;",
		ExpectedSelect: []sql.Row{
			{1, sql.Point{X: 1, Y: 2}},
			{2, sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{3, sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{4, sql.Point{SRID: 4326, X: 1, Y: 2}},
			{5, sql.LineString{SRID: 4326, Points: []sql.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{6, sql.Polygon{SRID: 4326, Lines: []sql.LineString{{SRID: 4326, Points: []sql.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}},
			{7, sql.Point{X: 123.456, Y: 7.89}},
		},
	},
	{
		WriteQuery:          "INSERT INTO geometry_table VALUES (7, LINESTRING(POINT(1,2),POINT(3,4)));",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM geometry_table;",
		ExpectedSelect: []sql.Row{
			{1, sql.Point{X: 1, Y: 2}},
			{2, sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{3, sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{4, sql.Point{SRID: 4326, X: 1, Y: 2}},
			{5, sql.LineString{SRID: 4326, Points: []sql.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{6, sql.Polygon{SRID: 4326, Lines: []sql.LineString{{SRID: 4326, Points: []sql.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}},
			{7, sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
		},
	},
	{
		WriteQuery:          "INSERT INTO geometry_table VALUES (7, 0x00000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040);",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM geometry_table;",
		ExpectedSelect: []sql.Row{
			{1, sql.Point{X: 1, Y: 2}},
			{2, sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{3, sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{4, sql.Point{SRID: 4326, X: 1, Y: 2}},
			{5, sql.LineString{SRID: 4326, Points: []sql.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{6, sql.Polygon{SRID: 4326, Lines: []sql.LineString{{SRID: 4326, Points: []sql.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}},
			{7, sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
		},
	},
	{
		WriteQuery:          "INSERT INTO geometry_table VALUES (7, POLYGON(LINESTRING(POINT(1,1),POINT(1,-1),POINT(-1,-1),POINT(-1,1),POINT(1,1))));",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM geometry_table;",
		ExpectedSelect: []sql.Row{
			{1, sql.Point{X: 1, Y: 2}},
			{2, sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{3, sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{4, sql.Point{SRID: 4326, X: 1, Y: 2}},
			{5, sql.LineString{SRID: 4326, Points: []sql.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{6, sql.Polygon{SRID: 4326, Lines: []sql.LineString{{SRID: 4326, Points: []sql.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}},
			{7, sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 1, Y: 1}, {X: 1, Y: -1}, {X: -1, Y: -1}, {X: -1, Y: 1}, {X: 1, Y: 1}}}}}},
		},
	},
	{
		WriteQuery:          "INSERT INTO geometry_table VALUES (7, 0x0000000001030000000100000005000000000000000000F03F000000000000F03F000000000000F03F000000000000F0BF000000000000F0BF000000000000F0BF000000000000F0BF000000000000F03F000000000000F03F000000000000F03F);",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM geometry_table;",
		ExpectedSelect: []sql.Row{
			{1, sql.Point{X: 1, Y: 2}},
			{2, sql.LineString{Points: []sql.Point{{X: 1, Y: 2}, {X: 3, Y: 4}}}},
			{3, sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 0, Y: 0}, {X: 0, Y: 1}, {X: 1, Y: 1}, {X: 0, Y: 0}}}}}},
			{4, sql.Point{SRID: 4326, X: 1, Y: 2}},
			{5, sql.LineString{SRID: 4326, Points: []sql.Point{{SRID: 4326, X: 1, Y: 2}, {SRID: 4326, X: 3, Y: 4}}}},
			{6, sql.Polygon{SRID: 4326, Lines: []sql.LineString{{SRID: 4326, Points: []sql.Point{{SRID: 4326, X: 0, Y: 0}, {SRID: 4326, X: 0, Y: 1}, {SRID: 4326, X: 1, Y: 1}, {SRID: 4326, X: 0, Y: 0}}}}}},
			{7, sql.Polygon{Lines: []sql.LineString{{Points: []sql.Point{{X: 1, Y: 1}, {X: 1, Y: -1}, {X: -1, Y: -1}, {X: -1, Y: 1}, {X: 1, Y: 1}}}}}},
		},
	},
}

var InsertIntoKeylessUnique = []WriteQueryTest{
	{
		WriteQuery:          "INSERT INTO unique_keyless VALUES (3, 3);",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM unique_keyless order by c0;",
		ExpectedSelect:      []sql.Row{{0, 0}, {1, 1}, {2, 2}, {3, 3}},
	},
	{
		WriteQuery:          "INSERT INTO unique_keyless VALUES (3, 4);",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM unique_keyless order by c0;",
		ExpectedSelect:      []sql.Row{{0, 0}, {1, 1}, {2, 2}, {3, 4}},
	},
}

var InsertIntoKeylessUniqueError = []GenericErrorQueryTest{
	{
		Name:  "Try to insert into a unique keyless table",
		Query: "INSERT INTO unique_keyless (100, 2)",
	},
	{
		Name:  "Try to insert into a unique keyless table",
		Query: "INSERT INTO unique_keyless (1, 1)",
	},
}

var InsertScripts = []ScriptTest{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
			"insert into auto values (NULL,90)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from auto order by 1",
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{float64(1)}, {float64(10)}, {float64(11)},
				},
			},
		},
	},
	{
		Name: "explicit DEFAULT",
		SetUpScript: []string{
			"CREATE TABLE t1(id int DEFAULT '2', vc varchar(255) DEFAULT '2');",
			"CREATE TABLE t2(id varchar(100) DEFAULT (uuid()));",
			"CREATE TABLE t3(a int DEFAULT '1', b int default (2 * a));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO T1 values (DEFAULT, DEFAULT)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "INSERT INTO t1 (id, VC) values (DEFAULT, DEFAULT)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "INSERT INTO t1 (vc, ID) values (DEFAULT, DEFAULT)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "INSERT INTO t1 (ID) values (DEFAULT), (3)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2}}},
			},
			{
				Query:    "INSERT INTO t1 (vc) values (DEFAULT), ('3')",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2}}},
			},
			{
				Query:    "INSERT INTO t1 values (100, '100'), (DEFAULT, DEFAULT)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2}}},
			},
			{
				Query:    "INSERT INTO t1 (id, vc) values (100, '100'), (DEFAULT, DEFAULT)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2}}},
			},
			{
				Query:    "INSERT INTO t1 (id) values (10), (DEFAULT)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2}}},
			},
			{
				Query:    "INSERT INTO t1 (VC) values ('10'), (DEFAULT)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2}}},
			},
			{
				Query:    "INSERT INTO t2 values ('10'), (DEFAULT)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2}}},
			},
			{
				Query:    "INSERT INTO t2 (id) values (DEFAULT), ('11'), (DEFAULT)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 3}}},
			},
			{
				Query:    "select count(distinct id) from t2",
				Expected: []sql.Row{{5}},
			},
			{
				Query:    "INSERT INTO t3 (a) values (DEFAULT), ('2'), (DEFAULT)",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 3}}},
			},
			{
				Query:    "SELECT b from t3 order by b asc",
				Expected: []sql.Row{{2}, {2}, {4}},
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
				Expected: []sql.Row{
					{1, 1}, {2, 2}, {3, 3},
				},
			},
			{
				Query: "INSERT IGNORE INTO y VALUES (1, 2), (4,4)",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 1}},
				},
				ExpectedWarning: mysql.ERDupEntry,
			},
			{
				Query: "INSERT IGNORE INTO y VALUES (5, NULL)",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 1}},
				},
				ExpectedWarning: mysql.ERBadNullError,
			},
			{
				Query: "INSERT IGNORE INTO y SELECT * FROM y WHERE pk=(SELECT pk FROM y WHERE pk > 1);",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 0}},
				},
				ExpectedWarning: mysql.ERSubqueryNo1Row,
			},
			{
				Query: "INSERT IGNORE INTO y SELECT 10, 0 FROM dual WHERE 1=(SELECT 1 FROM dual UNION SELECT 2 FROM dual);",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 0}},
				},
				ExpectedWarning: mysql.ERSubqueryNo1Row,
			},
			{
				Query: "INSERT IGNORE INTO y SELECT 11, 0 FROM dual WHERE 1=(SELECT 1 FROM dual UNION SELECT 2 FROM dual) UNION SELECT 12, 0 FROM dual;",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 1}},
				},
				ExpectedWarning: mysql.ERSubqueryNo1Row,
			},
			{
				Query: "INSERT IGNORE INTO y SELECT 13, 0 FROM dual UNION SELECT 14, 0 FROM dual WHERE 1=(SELECT 1 FROM dual UNION SELECT 2 FROM dual);",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 1}},
				},
				ExpectedWarning: mysql.ERSubqueryNo1Row,
			},
			{
				Query: "INSERT IGNORE INTO y VALUES (3, 8)",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 0}},
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
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1}}},
			},
			{
				Query: `INSERT INTO test VALUES (1,1)`,
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 1}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 2}},
				},
			},
			{
				Query: "insert into nodes(id,owner,status,timestamp) values('id2','dabe','off',3) on duplicate key update owner='milo',status='on'",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "select * from nodes",
				Expected: []sql.Row{
					{"id1", "milo", "on", 1},
					{"id2", "dabe", "off", 3},
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
		ExpectedErr: sql.ErrLengthBeyondLimit,
	},
	{
		Name: "try inserting varbinary larger than max limit",
		SetUpScript: []string{
			"create table bad (vb varbinary(65535))",
		},
		Query:       "insert into bad values (repeat('0', 65536))",
		ExpectedErr: sql.ErrLengthBeyondLimit,
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
				Expected: []sql.Row{
					{1, ""},
				},
			},
			{
				Query: "SELECT * FROM y",
				Expected: []sql.Row{
					{1, 0},
				},
			},
			{
				Query: "INSERT IGNORE INTO y VALUES (2, NULL)",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 1}},
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
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 1}},
				},
				ExpectedWarning: mysql.ERTruncatedWrongValueForField,
			},
			{
				Query: "SELECT * FROM t1",
				Expected: []sql.Row{
					{1, 0},
				},
			},
			{
				Query: "INSERT IGNORE INTO t2 values (1, 'adsda')",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 1}},
				},
				ExpectedWarning: mysql.ERUnknownError,
			},
			{
				Query: "SELECT * FROM t2",
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 2}},
				},
				ExpectedWarning: mysql.ERTruncatedWrongValueForField,
			},
			{
				Query: "SELECT * FROM t1",
				Expected: []sql.Row{
					{1, 0},
				},
			},
			{
				Query: "INSERT IGNORE INTO t2 values (1, 'adsda')",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 1}},
				},
				ExpectedWarning: mysql.ERUnknownError,
			},
			{
				Query: "SELECT * FROM t2",
				Expected: []sql.Row{
					{1, "a"},
				},
			},
			{
				Query:    "INSERT IGNORE INTO t2 VALUES (1, 's') ON DUPLICATE KEY UPDATE pk = 1000", // violates constraint
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 0}}},
			},
			{
				Query: "SELECT * FROM t2",
				Expected: []sql.Row{
					{1, "a"},
				},
			},
		},
	},
}

var InsertBrokenScripts = []ScriptTest{
	// TODO: Support unique keys and FK violations in memory implementation
	{
		Name: "Test that INSERT IGNORE INTO works with unique keys",
		SetUpScript: []string{
			"CREATE TABLE mytable(pk int PRIMARY KEY, value varchar(10) UNIQUE)",
			"INSERT INTO mytable values (1,'one')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "INSERT IGNORE INTO mytable VALUES (2, 'one')",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 0}},
				},
				ExpectedWarning: mysql.ERDupEntry,
			},
		},
	},
	{
		Name: "Test that INSERT IGNORE works with FK Violations",
		SetUpScript: []string{
			"CREATE TABLE t1 (id INT PRIMARY KEY, v int);",
			"CREATE TABLE t2 (id INT PRIMARY KEY, v2 int, CONSTRAINT mfk FOREIGN KEY (v2) REFERENCES t1(id));",
			"INSERT INTO t1 values (1,1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "INSERT IGNORE INTO t2 VALUES (1,2);",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 0}},
				},
				ExpectedWarning: mysql.ErNoReferencedRow2,
			},
		},
	},
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
				Expected: []sql.Row{
					{1, "one"}, {2, 1}, {3, "three"},
				},
			},
			{
				Query: "SELECT * FROM y",
				Expected: []sql.Row{
					{1, 1}, {2, 0}, {3, 3},
				},
			},
			{
				Query: `INSERT IGNORE INTO y VALUES (4, "four")`,
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 1}},
				},
				ExpectedWarning: mysql.ERTruncatedWrongValueForField,
			},
		},
	},
}
