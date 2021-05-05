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

package enginetest

import (
	"math"

	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/parse"
)

var InsertQueries = []WriteQueryTest{
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
		WriteQuery:          "INSERT INTO mytable VALUES (?, ?);",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT i FROM mytable WHERE s = 'x';",
		ExpectedSelect:      []sql.Row{{int64(999)}},
		Bindings: map[string]sql.Expression{
			"v1": expression.NewLiteral(int64(999), sql.Int64),
			"v2": expression.NewLiteral("x", sql.Text),
		},
	},
	{
		WriteQuery:          "INSERT INTO mytable VALUES (:col1, :col2);",
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT i FROM mytable WHERE s = 'x';",
		ExpectedSelect:      []sql.Row{{int64(999)}},
		Bindings: map[string]sql.Expression{
			"col1": expression.NewLiteral(int64(999), sql.Int64),
			"col2": expression.NewLiteral("x", sql.Text),
		},
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
			"random text", sql.True, sql.MustJSON(`{"key":"value"}`), "blobdata",
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
			"random text", sql.True, sql.MustJSON(`{"key":"value"}`), "blobdata",
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
			"", sql.False, sql.MustJSON(`""`), "",
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
			"", sql.False, sql.MustJSON(`""`), "",
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
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
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
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(2)}},
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
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
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
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
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
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
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
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(5)}},
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
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(1)}},
		SelectQuery:         "SELECT * FROM auto_increment_tbl",
		ExpectedSelect: []sql.Row{
			{1, 11},
			{2, 22},
			{3, 33},
			{4, 44},
		},
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
}

var InsertErrorTests = []GenericErrorQueryTest{
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
		ExpectedErr: parse.ErrInvalidAutoIncCols,
	},
	{
		Name:        "create multiple auto_increment columns",
		Query:       "create table bad (pk1 int auto_increment, pk2 int auto_increment, primary key (pk1,pk2));",
		ExpectedErr: parse.ErrInvalidAutoIncCols,
	},
	{
		Name:        "create auto_increment column with default",
		Query:       "create table bad (pk1 int auto_increment default 10, c0 int);",
		ExpectedErr: parse.ErrInvalidAutoIncCols,
	},
}

var InsertIgnoreScripts = []ScriptTest{
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
				Query: "INSERT IGNORE INTO y VALUES (3, 8)",
				Expected: []sql.Row{
					{sql.OkResult{RowsAffected: 0}},
				},
				ExpectedWarning: mysql.ERDupEntry,
			},
		},
	},
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
	//{
	//	Name: "Test that INSERT IGNORE assigns the closest dataype correctly",
	//	SetUpScript: []string{
	//		"CREATE TABLE x (pk int primary key, c1 varchar(20) NOT NULL);",
	//		`INSERT IGNORE INTO x VALUES (1, "one"), (2, TRUE), (3, "three")`,
	//		"CREATE TABLE y (pk int primary key, c1 int NOT NULL);",
	//		`INSERT IGNORE INTO y VALUES (1, 1), (2, "two"), (3,3);`,
	//	},
	//	Assertions: []ScriptTestAssertion{
	//		{
	//			Query: "SELECT * FROM x",
	//			Expected: []sql.Row{
	//				{1, "one"}, {2, 1}, {3, "three"},
	//			},
	//		},
	//		{
	//			Query: "SELECT * FROM y",
	//			Expected: []sql.Row{
	//				{1, 1}, {2, 0}, {3, 3},
	//			},
	//		},
	//		{
	//			Query: `INSERT IGNORE INTO y VALUES (4, "four")`,
	//			Expected: []sql.Row{
	//				{sql.OkResult{RowsAffected: 1}},
	//			},
	//			ExpectedWarning: mysql.ERTruncatedWrongValueForField,
	//		},
	//	},
	//},
}
