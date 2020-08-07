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
	"math"
	"time"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

type QueryTest struct {
	Query    string
	Expected []sql.Row
}

var QueryTests = []QueryTest{
	{
		"SELECT * FROM mytable;",
		[]sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
	},
	{
		"SELECT * FROM mytable ORDER BY i DESC;",
		[]sql.Row{
			{int64(3), "third row"},
			{int64(2), "second row"},
			{int64(1), "first row"},
		},
	},
	{
		"SELECT * FROM mytable GROUP BY i,s;",
		[]sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
	},
	{
		"SELECT pk DIV 2, SUM(c3) FROM one_pk GROUP BY 1 ORDER BY 1",
		[]sql.Row{
			{int64(0), float64(10)},
			{int64(1), float64(50)},
		},
	},
	{
		"SELECT pk1, SUM(c1) FROM two_pk GROUP BY pk1 ORDER BY pk1;",
		[]sql.Row{
			{0, 10.0},
			{1, 50.0},
		},
	},
	{
		"SELECT pk1, SUM(c1) FROM two_pk WHERE pk1 = 0",
		[]sql.Row{{0, 10.0}},
	},
	{
		"SELECT i FROM mytable;",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		"SELECT i AS x FROM mytable ORDER BY i DESC",
		[]sql.Row{{3}, {2}, {1}},
	},
	{
		"SELECT i AS x FROM mytable ORDER BY x DESC",
		[]sql.Row{{3}, {2}, {1}},
	},
	{
		"SELECT i FROM mytable AS mt;",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		"SELECT s,i FROM mytable;",
		[]sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		"SELECT mytable.s,i FROM mytable;",
		[]sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		"SELECT t.s,i FROM mytable AS t;",
		[]sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		"SELECT s,i FROM (select i,s FROM mytable) mt;",
		[]sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		"SELECT s,i FROM MyTable ORDER BY 2",
		[]sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		"SELECT S,I FROM MyTable ORDER BY 2",
		[]sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		"SELECT mt.s,mt.i FROM MyTable MT ORDER BY 2;",
		[]sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		"SELECT mT.S,Mt.I FROM MyTable MT ORDER BY 2;",
		[]sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		"SELECT mt.* FROM MyTable MT ORDER BY mT.I;",
		[]sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"}},
	},
	{
		"SELECT MyTABLE.s,myTable.i FROM MyTable ORDER BY 2;",
		[]sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		"SELECT myTable.* FROM MYTABLE ORDER BY myTable.i;",
		[]sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"}},
	},
	{
		"SELECT MyTABLE.S,myTable.I FROM MyTable ORDER BY mytable.i;",
		[]sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		"SELECT i, 1 AS foo, 2 AS bar FROM MyTable WHERE bar = 2 ORDER BY foo, i;",
		[]sql.Row{
			{1, 1, 2},
			{2, 1, 2},
			{3, 1, 2}},
	},
	{
		"SELECT i, 1 AS foo, 2 AS bar FROM (SELECT i FROM mYtABLE WHERE i = 2) AS a ORDER BY foo, i",
		[]sql.Row{
			{2, 1, 2}},
	},
	{
		"SELECT i, 1 AS foo, 2 AS bar FROM MyTable WHERE bar = 1 ORDER BY foo, i;",
		[]sql.Row{},
	},
	{
		"SELECT timestamp FROM reservedWordsTable;",
		[]sql.Row{{"1"}},
	},
	{
		"SELECT RW.TIMESTAMP FROM reservedWordsTable rw;",
		[]sql.Row{{"1"}},
	},
	{
		"SELECT `AND`, RW.`Or`, `SEleCT` FROM reservedWordsTable rw;",
		[]sql.Row{{"1.1", "aaa", "create"}},
	},
	{
		"SELECT reservedWordsTable.AND, reservedWordsTABLE.Or, reservedwordstable.SEleCT FROM reservedWordsTable;",
		[]sql.Row{{"1.1", "aaa", "create"}},
	},
	{
		"SELECT i + 1 FROM mytable;",
		[]sql.Row{{int64(2)}, {int64(3)}, {int64(4)}},
	},
	{
		"SELECT i div 2 FROM mytable order by 1;",
		[]sql.Row{{int64(0)}, {int64(1)}, {int64(1)}},
	},
	{
		"SELECT i DIV 2 FROM mytable order by 1;",
		[]sql.Row{{int64(0)}, {int64(1)}, {int64(1)}},
	},
	{
		"SELECT -i FROM mytable;",
		[]sql.Row{{int64(-1)}, {int64(-2)}, {int64(-3)}},
	},
	{
		"SELECT +i FROM mytable;",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		"SELECT + - i FROM mytable;",
		[]sql.Row{{int64(-1)}, {int64(-2)}, {int64(-3)}},
	},
	{
		"SELECT i FROM mytable WHERE -i = -2;",
		[]sql.Row{{int64(2)}},
	},
	{
		"SELECT i FROM mytable WHERE i = 2;",
		[]sql.Row{{int64(2)}},
	},
	{
		"SELECT i FROM mytable WHERE 2 = i;",
		[]sql.Row{{int64(2)}},
	},
	{
		"SELECT i FROM mytable WHERE i > 2;",
		[]sql.Row{{int64(3)}},
	},
	{
		"SELECT i FROM mytable WHERE 2 < i;",
		[]sql.Row{{int64(3)}},
	},
	{
		"SELECT i FROM mytable WHERE i < 2;",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT i FROM mytable WHERE 2 > i;",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT i FROM mytable WHERE i <> 2;",
		[]sql.Row{{int64(1)}, {int64(3)}},
	},
	{
		"SELECT i FROM mytable WHERE i IN (1, 3)",
		[]sql.Row{{int64(1)}, {int64(3)}},
	},
	{
		"SELECT i FROM mytable WHERE i = 1 OR i = 3",
		[]sql.Row{{int64(1)}, {int64(3)}},
	},
	{
		"SELECT i FROM mytable WHERE i >= 2 ORDER BY 1",
		[]sql.Row{{int64(2)}, {int64(3)}},
	},
	{
		"SELECT i FROM mytable WHERE 2 <= i ORDER BY 1",
		[]sql.Row{{int64(2)}, {int64(3)}},
	},
	{
		"SELECT i FROM mytable WHERE i <= 2 ORDER BY 1",
		[]sql.Row{{int64(1)}, {int64(2)}},
	},
	{
		"SELECT i FROM mytable WHERE 2 >= i ORDER BY 1",
		[]sql.Row{{int64(1)}, {int64(2)}},
	},
	{
		"SELECT i FROM mytable WHERE i > 2",
		[]sql.Row{{int64(3)}},
	},
	{
		"SELECT i FROM mytable WHERE i < 2",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT i FROM mytable WHERE i >= 2 OR i = 1 ORDER BY 1",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		"SELECT f32 FROM floattable WHERE f64 = 2.0;",
		[]sql.Row{{float32(2.0)}},
	},
	{
		"SELECT f32 FROM floattable WHERE f64 < 2.0;",
		[]sql.Row{{float32(-1.0)}, {float32(-1.5)}, {float32(1.0)}, {float32(1.5)}},
	},
	{
		"SELECT f32 FROM floattable WHERE f64 > 2.0;",
		[]sql.Row{{float32(2.5)}},
	},
	{
		"SELECT f32 FROM floattable WHERE f64 <> 2.0;",
		[]sql.Row{{float32(-1.0)}, {float32(-1.5)}, {float32(1.0)}, {float32(1.5)}, {float32(2.5)}},
	},
	{
		"SELECT f64 FROM floattable WHERE f32 = 2.0;",
		[]sql.Row{{float64(2.0)}},
	},
	{
		"SELECT f64 FROM floattable WHERE f32 = -1.5;",
		[]sql.Row{{float64(-1.5)}},
	},
	{
		"SELECT f64 FROM floattable WHERE -f32 = -2.0;",
		[]sql.Row{{float64(2.0)}},
	},
	{
		"SELECT f64 FROM floattable WHERE f32 < 2.0;",
		[]sql.Row{{float64(-1.0)}, {float64(-1.5)}, {float64(1.0)}, {float64(1.5)}},
	},
	{
		"SELECT f64 FROM floattable WHERE f32 > 2.0;",
		[]sql.Row{{float64(2.5)}},
	},
	{
		"SELECT f64 FROM floattable WHERE f32 <> 2.0;",
		[]sql.Row{{float64(-1.0)}, {float64(-1.5)}, {float64(1.0)}, {float64(1.5)}, {float64(2.5)}},
	},
	{
		"SELECT f32 FROM floattable ORDER BY f64;",
		[]sql.Row{{float32(-1.5)}, {float32(-1.0)}, {float32(1.0)}, {float32(1.5)}, {float32(2.0)}, {float32(2.5)}},
	},
	{
		"SELECT i FROM mytable ORDER BY i DESC;",
		[]sql.Row{{int64(3)}, {int64(2)}, {int64(1)}},
	},
	{
		"SELECT i FROM mytable WHERE 'hello';",
		nil,
	},
	{
		"SELECT i FROM mytable WHERE NOT 'hello';",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		"SELECT i FROM mytable WHERE s = 'first row' ORDER BY i DESC;",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT i FROM mytable WHERE s = 'first row' ORDER BY i DESC LIMIT 1;",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT i FROM mytable ORDER BY i LIMIT 1 OFFSET 1;",
		[]sql.Row{{int64(2)}},
	},
	{
		"SELECT i FROM mytable ORDER BY i LIMIT 1,1;",
		[]sql.Row{{int64(2)}},
	},
	{
		"SELECT i FROM mytable ORDER BY i LIMIT 3,1;",
		nil,
	},
	{
		"SELECT i FROM mytable ORDER BY i LIMIT 2,100;",
		[]sql.Row{{int64(3)}},
	},
	{
		"SELECT i FROM niltable WHERE b IS NULL",
		[]sql.Row{{int64(1)}, {int64(4)}},
	},
	{
		"SELECT i FROM niltable WHERE b IS NOT NULL",
		[]sql.Row{
			{int64(2)}, {int64(3)},
			{int64(5)}, {int64(6)},
		},
	},
	{
		"SELECT i FROM niltable WHERE b",
		[]sql.Row{
			{int64(2)},
			{int64(5)},
		},
	},
	{
		"SELECT i FROM niltable WHERE NOT b",
		[]sql.Row{
			{int64(3)},
			{int64(6)},
		},
	},
	{
		"SELECT i FROM niltable WHERE b IS TRUE",
		[]sql.Row{{int64(2)}, {int64(5)}},
	},
	{
		"SELECT i FROM niltable WHERE b IS NOT TRUE",
		[]sql.Row{
			{int64(1)}, {int64(3)},
			{int64(4)}, {int64(6)},
		},
	},
	{
		"SELECT f FROM niltable WHERE b IS FALSE",
		[]sql.Row{{nil}, {6.0}},
	},
	{
		"SELECT i FROM niltable WHERE f < 5",
		[]sql.Row{{int64(4)}},
	},
	{
		"SELECT i FROM niltable WHERE f > 5",
		[]sql.Row{{int64(6)}},
	},
	{
		"SELECT i FROM niltable WHERE b IS NOT FALSE",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(4)}, {int64(5)}},
	},
	{
		"SELECT COUNT(*) FROM mytable;",
		[]sql.Row{{int64(3)}},
	},
	{
		"SELECT COUNT(*) FROM mytable LIMIT 1;",
		[]sql.Row{{int64(3)}},
	},
	{
		"SELECT COUNT(*) AS c FROM mytable;",
		[]sql.Row{{int64(3)}},
	},
	{
		"SELECT substring(s, 2, 3) FROM mytable",
		[]sql.Row{{"irs"}, {"eco"}, {"hir"}},
	},
	{
		`SELECT substring("foo", 2, 2)`,
		[]sql.Row{{"oo"}},
	},
	{
		`SELECT SUBSTRING_INDEX('a.b.c.d.e.f', '.', 2)`,
		[]sql.Row{
			{"a.b"},
		},
	},
	{
		`SELECT SUBSTRING_INDEX('a.b.c.d.e.f', '.', -2)`,
		[]sql.Row{
			{"e.f"},
		},
	},
	{
		`SELECT SUBSTRING_INDEX(SUBSTRING_INDEX('source{d}', '{d}', 1), 'r', -1)`,
		[]sql.Row{
			{"ce"},
		},
	},
	{
		`SELECT SUBSTRING_INDEX(mytable.s, "d", 1) AS s FROM mytable INNER JOIN othertable ON (SUBSTRING_INDEX(mytable.s, "d", 1) = SUBSTRING_INDEX(othertable.s2, "d", 1)) GROUP BY 1 HAVING s = 'secon'`,
		[]sql.Row{{"secon"}},
	},
	{
		"SELECT YEAR('2007-12-11') FROM mytable",
		[]sql.Row{{int32(2007)}, {int32(2007)}, {int32(2007)}},
	},
	{
		"SELECT MONTH('2007-12-11') FROM mytable",
		[]sql.Row{{int32(12)}, {int32(12)}, {int32(12)}},
	},
	{
		"SELECT DAY('2007-12-11') FROM mytable",
		[]sql.Row{{int32(11)}, {int32(11)}, {int32(11)}},
	},
	{
		"SELECT HOUR('2007-12-11 20:21:22') FROM mytable",
		[]sql.Row{{int32(20)}, {int32(20)}, {int32(20)}},
	},
	{
		"SELECT MINUTE('2007-12-11 20:21:22') FROM mytable",
		[]sql.Row{{int32(21)}, {int32(21)}, {int32(21)}},
	},
	{
		"SELECT SECOND('2007-12-11 20:21:22') FROM mytable",
		[]sql.Row{{int32(22)}, {int32(22)}, {int32(22)}},
	},
	{
		"SELECT DAYOFYEAR('2007-12-11 20:21:22') FROM mytable",
		[]sql.Row{{int32(345)}, {int32(345)}, {int32(345)}},
	},
	{
		"SELECT SECOND('2007-12-11T20:21:22Z') FROM mytable",
		[]sql.Row{{int32(22)}, {int32(22)}, {int32(22)}},
	},
	{
		"SELECT DAYOFYEAR('2007-12-11') FROM mytable",
		[]sql.Row{{int32(345)}, {int32(345)}, {int32(345)}},
	},
	{
		"SELECT DAYOFYEAR('20071211') FROM mytable",
		[]sql.Row{{int32(345)}, {int32(345)}, {int32(345)}},
	},
	{
		"SELECT YEARWEEK('0000-01-01')",
		[]sql.Row{{int32(1)}},
	},
	{
		"SELECT YEARWEEK('9999-12-31')",
		[]sql.Row{{int32(999952)}},
	},
	{
		"SELECT YEARWEEK('2008-02-20', 1)",
		[]sql.Row{{int32(200808)}},
	},
	{
		"SELECT YEARWEEK('1987-01-01')",
		[]sql.Row{{int32(198652)}},
	},
	{
		"SELECT YEARWEEK('1987-01-01', 20), YEARWEEK('1987-01-01', 1), YEARWEEK('1987-01-01', 2), YEARWEEK('1987-01-01', 3), YEARWEEK('1987-01-01', 4), YEARWEEK('1987-01-01', 5), YEARWEEK('1987-01-01', 6), YEARWEEK('1987-01-01', 7)",
		[]sql.Row{{int32(198653), int32(198701), int32(198652), int32(198701), int32(198653), int32(198652), int32(198653), int32(198652)}},
	},
	{
		"SELECT i FROM mytable WHERE i BETWEEN 1 AND 2",
		[]sql.Row{{int64(1)}, {int64(2)}},
	},
	{
		"SELECT i FROM mytable WHERE i NOT BETWEEN 1 AND 2",
		[]sql.Row{{int64(3)}},
	},
	{
		"SELECT id FROM typestable WHERE ti > '2019-12-31'",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT id FROM typestable WHERE da > '2019-12-31'",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT id FROM typestable WHERE ti < '2019-12-31'",
		nil,
	},
	{
		"SELECT id FROM typestable WHERE da < '2019-12-31'",
		nil,
	},
	{
		"SELECT id FROM typestable WHERE ti > date_add('2019-12-30', INTERVAL 1 day)",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT id FROM typestable WHERE da > date_add('2019-12-30', INTERVAL 1 DAY)",
		nil,
	},
	{
		"SELECT id FROM typestable WHERE da >= date_add('2019-12-30', INTERVAL 1 DAY)",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT id FROM typestable WHERE ti < date_add('2019-12-30', INTERVAL 1 DAY)",
		nil,
	},
	{
		"SELECT id FROM typestable WHERE da < date_add('2019-12-30', INTERVAL 1 DAY)",
		nil,
	},
	{
		"SELECT id FROM typestable WHERE ti > date_sub('2020-01-01', INTERVAL 1 DAY)",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT id FROM typestable WHERE da > date_sub('2020-01-01', INTERVAL 1 DAY)",
		nil,
	},
	{
		"SELECT id FROM typestable WHERE da >= date_sub('2020-01-01', INTERVAL 1 DAY)",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT id FROM typestable WHERE ti < date_sub('2020-01-01', INTERVAL 1 DAY)",
		nil,
	},
	{
		"SELECT id FROM typestable WHERE da < date_sub('2020-01-01', INTERVAL 1 DAY)",
		nil,
	}, {
		"SELECT i,v from stringandtable WHERE i",
		[]sql.Row{
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{int64(5), nil},
		},
	},
	{
		"SELECT i,v from stringandtable WHERE i AND i",
		[]sql.Row{
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{int64(5), nil},
		},
	},
	{
		"SELECT i,v from stringandtable WHERE i OR i",
		[]sql.Row{
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{int64(5), nil},
		},
	},
	{
		"SELECT i,v from stringandtable WHERE NOT i",
		[]sql.Row{{int64(0), "0"}},
	},
	{
		"SELECT i,v from stringandtable WHERE NOT i AND NOT i",
		[]sql.Row{{int64(0), "0"}},
	},
	{
		"SELECT i,v from stringandtable WHERE NOT i OR NOT i",
		[]sql.Row{{int64(0), "0"}},
	},
	{
		"SELECT i,v from stringandtable WHERE i OR NOT i",
		[]sql.Row{
			{int64(0), "0"},
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{int64(5), nil},
		},
	},
	{
		"SELECT i,v from stringandtable WHERE v",
		[]sql.Row{{int64(1), "1"}, {nil, "2"}},
	},
	{
		"SELECT i,v from stringandtable WHERE v AND v",
		[]sql.Row{{int64(1), "1"}, {nil, "2"}},
	},
	{
		"SELECT i,v from stringandtable WHERE v OR v",
		[]sql.Row{{int64(1), "1"}, {nil, "2"}},
	},
	{
		"SELECT i,v from stringandtable WHERE NOT v",
		[]sql.Row{
			{int64(0), "0"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
		},
	},
	{
		"SELECT i,v from stringandtable WHERE NOT v AND NOT v",
		[]sql.Row{
			{int64(0), "0"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
		},
	},
	{
		"SELECT i,v from stringandtable WHERE NOT v OR NOT v",
		[]sql.Row{
			{int64(0), "0"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
		},
	},
	{
		"SELECT i,v from stringandtable WHERE v OR NOT v",
		[]sql.Row{
			{int64(0), "0"},
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{nil, "2"},
		},
	},
	{
		"SELECT substring(mytable.s, 1, 5) AS s FROM mytable INNER JOIN othertable ON (substring(mytable.s, 1, 5) = SUBSTRING(othertable.s2, 1, 5)) GROUP BY 1",
		[]sql.Row{
			{"third"},
			{"secon"},
			{"first"},
		},
	},
	{
		"SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 ORDER BY i",
		[]sql.Row{
			{int64(1), int64(1), "third"},
			{int64(2), int64(2), "second"},
			{int64(3), int64(3), "first"},
		},
	},
	// TODO: this should work, but generates a table name conflict right now
	// {
	// 	"SELECT i, i2, s2 FROM mytable as OTHERTABLE INNER JOIN othertable as MYTABLE ON i = i2 ORDER BY i",
	// 	[]sql.Row{
	// 		{int64(1), int64(1), "third"},
	// 		{int64(2), int64(2), "second"},
	// 		{int64(3), int64(3), "first"},
	// 	},
	// },
	{
		"SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i = i2 ORDER BY i",
		[]sql.Row{
			{"third", int64(1), int64(1)},
			{"second", int64(2), int64(2)},
			{"first", int64(3), int64(3)},
		},
	},
	{
		"SELECT i, i2, s2 FROM othertable JOIN mytable  ON i = i2 ORDER BY i",
		[]sql.Row{
			{int64(1), int64(1), "third"},
			{int64(2), int64(2), "second"},
			{int64(3), int64(3), "first"},
		},
	},
	{
		"SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2 ORDER BY i",
		[]sql.Row{
			{"third", int64(1), int64(1)},
			{"second", int64(2), int64(2)},
			{"first", int64(3), int64(3)},
		},
	},
	{
		"SELECT substring(s2, 1), substring(s2, 2), substring(s2, 3) FROM othertable ORDER BY i2",
		[]sql.Row{
			{"third", "hird", "ird"},
			{"second", "econd", "cond"},
			{"first", "irst", "rst"},
		},
	},
	{
		`SELECT substring("first", 1), substring("second", 2), substring("third", 3)`,
		[]sql.Row{
			{"first", "econd", "ird"},
		},
	},
	{
		"SELECT substring(s2, -1), substring(s2, -2), substring(s2, -3) FROM othertable ORDER BY i2",
		[]sql.Row{
			{"d", "rd", "ird"},
			{"d", "nd", "ond"},
			{"t", "st", "rst"},
		},
	},
	{
		`SELECT substring("first", -1), substring("second", -2), substring("third", -3)`,
		[]sql.Row{
			{"t", "nd", "ird"},
		},
	},
	{
		"SELECT s FROM mytable INNER JOIN othertable " +
			"ON substring(s2, 1, 2) != '' AND i = i2 ORDER BY 1",
		[]sql.Row{
			{"first row"},
			{"second row"},
			{"third row"},
		},
	},
	{
		`SELECT i FROM mytable NATURAL JOIN tabletest`,
		[]sql.Row{
			{int64(1)},
			{int64(2)},
			{int64(3)},
		},
	},
	{
		`SELECT i FROM mytable AS t NATURAL JOIN tabletest AS test`,
		[]sql.Row{
			{int64(1)},
			{int64(2)},
			{int64(3)},
		},
	},
	// TODO: this should work: either table alias should be usable in the select clause
	// {
	// 	`SELECT t.i, test.s FROM mytable AS t NATURAL JOIN tabletest AS test`,
	// 	[]sql.Row{
	// 		{int64(1), "first row"},
	// 		{int64(2), "second row"},
	// 		{int64(3), "third row"},
	// 	},
	// },
	{
		`SELECT COUNT(*) AS cnt, fi FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY fi`,
		[]sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		`SELECT fi, COUNT(*) FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY fi
		ORDER BY COUNT(*) ASC`,
		[]sql.Row{
			{"first row", int64(1)},
			{"second row", int64(1)},
			{"third row", int64(1)},
		},
	},
	{
		`SELECT COUNT(*), fi  FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY fi
		ORDER BY COUNT(*) ASC`,
		[]sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		`SELECT COUNT(*) AS cnt, fi FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY 2`,
		[]sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		`SELECT COUNT(*) AS cnt, s AS fi FROM mytable GROUP BY fi`,
		[]sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		`SELECT COUNT(*) AS cnt, s AS fi FROM mytable GROUP BY 2`,
		[]sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		"SELECT CAST(-3 AS UNSIGNED) FROM mytable",
		[]sql.Row{
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
		},
	},
	{
		"SELECT CONVERT(-3, UNSIGNED) FROM mytable",
		[]sql.Row{
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
		},
	},
	{
		"SELECT '3' > 2 FROM tabletest",
		[]sql.Row{
			{true},
			{true},
			{true},
		},
	},
	{
		"SELECT s > 2 FROM tabletest",
		[]sql.Row{
			{false},
			{false},
			{false},
		},
	},
	{
		"SELECT * FROM tabletest WHERE s > 0",
		nil,
	},
	{
		"SELECT * FROM tabletest WHERE s = 0",
		[]sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
	},
	{
		"SELECT * FROM tabletest WHERE s = 'first row'",
		[]sql.Row{
			{int64(1), "first row"},
		},
	},
	{
		"SELECT s FROM mytable WHERE i IN (1, 2, 5)",
		[]sql.Row{
			{"first row"},
			{"second row"},
		},
	},
	{
		"SELECT s FROM mytable WHERE i NOT IN (1, 2, 5)",
		[]sql.Row{
			{"third row"},
		},
	},
	{
		"SELECT 1 + 2",
		[]sql.Row{
			{int64(3)},
		},
	},
	{
		`SELECT i AS foo FROM mytable WHERE foo NOT IN (1, 2, 5)`,
		[]sql.Row{{int64(3)}},
	},
	{
		`SELECT * FROM tabletest, mytable mt INNER JOIN othertable ot ON mt.i = ot.i2`,
		[]sql.Row{
			{int64(1), "first row", int64(1), "first row", "third", int64(1)},
			{int64(1), "first row", int64(2), "second row", "second", int64(2)},
			{int64(1), "first row", int64(3), "third row", "first", int64(3)},
			{int64(2), "second row", int64(1), "first row", "third", int64(1)},
			{int64(2), "second row", int64(2), "second row", "second", int64(2)},
			{int64(2), "second row", int64(3), "third row", "first", int64(3)},
			{int64(3), "third row", int64(1), "first row", "third", int64(1)},
			{int64(3), "third row", int64(2), "second row", "second", int64(2)},
			{int64(3), "third row", int64(3), "third row", "first", int64(3)},
		},
	},
	{
		`SELECT split(s," ") FROM mytable`,
		[]sql.Row{
			sql.NewRow([]interface{}{"first", "row"}),
			sql.NewRow([]interface{}{"second", "row"}),
			sql.NewRow([]interface{}{"third", "row"}),
		},
	},
	{
		`SELECT split(s,"s") FROM mytable`,
		[]sql.Row{
			sql.NewRow([]interface{}{"fir", "t row"}),
			sql.NewRow([]interface{}{"", "econd row"}),
			sql.NewRow([]interface{}{"third row"}),
		},
	},
	{
		`SELECT SUM(i) FROM mytable`,
		[]sql.Row{{float64(6)}},
	},
	{
		`SELECT GET_LOCK("test", 0)`,
		[]sql.Row{{int8(1)}},
	},
	{
		`SELECT IS_FREE_LOCK("test")`,
		[]sql.Row{{int8(0)}},
	},
	{
		`SELECT RELEASE_LOCK("test")`,
		[]sql.Row{{int8(1)}},
	},
	{
		`SELECT RELEASE_ALL_LOCKS()`,
		[]sql.Row{{int32(0)}},
	},
	{
		`SELECT * FROM mytable mt INNER JOIN othertable ot ON mt.i = ot.i2 AND mt.i > 2`,
		[]sql.Row{
			{int64(3), "third row", "first", int64(3)},
		},
	},
	{
		`SELECT i AS foo FROM mytable ORDER BY i DESC`,
		[]sql.Row{
			{int64(3)},
			{int64(2)},
			{int64(1)},
		},
	},
	{
		`SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY i ORDER BY i DESC`,
		[]sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		`SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY 2 ORDER BY 2 DESC`,
		[]sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		`SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY i ORDER BY foo DESC`,
		[]sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		`SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY 2 ORDER BY foo DESC`,
		[]sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		`SELECT COUNT(*) c, i AS i FROM mytable GROUP BY 2`,
		[]sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		`SELECT i AS i FROM mytable GROUP BY 1`,
		[]sql.Row{
			{int64(3)},
			{int64(2)},
			{int64(1)},
		},
	},
	{
		`SELECT CONCAT("a", "b", "c")`,
		[]sql.Row{
			{string("abc")},
		},
	},
	{
		`SELECT COALESCE(NULL, NULL, NULL, 'example', NULL, 1234567890)`,
		[]sql.Row{
			{string("example")},
		},
	},
	{
		`SELECT COALESCE(NULL, NULL, NULL, COALESCE(NULL, 1234567890))`,
		[]sql.Row{
			{int32(1234567890)},
		},
	},
	{
		"SELECT concat(s, i) FROM mytable",
		[]sql.Row{
			{string("first row1")},
			{string("second row2")},
			{string("third row3")},
		},
	},
	{
		"SELECT version()",
		[]sql.Row{
			{string("8.0.11")},
		},
	},
	{
		`SELECT RAND(100)`,
		[]sql.Row{
			{float64(0.8165026937796166)},
		},
	},
	{
		`SELECT RAND(100) = RAND(100)`,
		[]sql.Row{
			{true},
		},
	},
	{
		`SELECT RAND() = RAND()`,
		[]sql.Row{
			{false},
		},
	},
	{
		"SELECT * FROM mytable WHERE 1 > 5",
		nil,
	},
	{
		"SELECT SUM(i) + 1, i FROM mytable GROUP BY i ORDER BY i",
		[]sql.Row{
			{float64(2), int64(1)},
			{float64(3), int64(2)},
			{float64(4), int64(3)},
		},
	},
	{
		"SELECT SUM(i), i FROM mytable GROUP BY i ORDER BY 1+SUM(i) ASC",
		[]sql.Row{
			{float64(1), int64(1)},
			{float64(2), int64(2)},
			{float64(3), int64(3)},
		},
	},
	{
		"SELECT i, SUM(i) FROM mytable GROUP BY i ORDER BY SUM(i) DESC",
		[]sql.Row{
			{int64(3), float64(3)},
			{int64(2), float64(2)},
			{int64(1), float64(1)},
		},
	},
	{
		"SELECT i FROM mytable UNION SELECT i+10 FROM mytable;",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}, {int64(11)}, {int64(12)}, {int64(13)}},
	},
	{
		"SELECT i FROM mytable UNION DISTINCT SELECT i+10 FROM mytable;",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}, {int64(11)}, {int64(12)}, {int64(13)}},
	},
	{
		"SELECT i FROM mytable UNION SELECT i FROM mytable;",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}, {int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		"SELECT i FROM mytable UNION DISTINCT SELECT i FROM mytable;",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		"SELECT i FROM mytable UNION SELECT s FROM mytable;",
		[]sql.Row{
			{"1"},
			{"2"},
			{"3"},
			{"first row"},
			{"second row"},
			{"third row"},
		},
	},
	{
		`/*!40101 SET NAMES utf8 */`,
		nil,
	},
	{
		`SHOW DATABASES`,
		[]sql.Row{{"mydb"}, {"foo"}, {"information_schema"}},
	},
	{
		`SHOW SCHEMAS`,
		[]sql.Row{{"mydb"}, {"foo"}, {"information_schema"}},
	},
	{
		`SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA`,
		[]sql.Row{
			{"information_schema", "utf8mb4", "utf8_bin"},
			{"mydb", "utf8mb4", "utf8_bin"},
			{"foo", "utf8mb4", "utf8_bin"},
		},
	},
	{
		`SELECT s FROM mytable WHERE s LIKE '%d row'`,
		[]sql.Row{
			{"second row"},
			{"third row"},
		},
	},
	{
		`SELECT SUBSTRING(s, -3, 3) AS s FROM mytable WHERE s LIKE '%d row' GROUP BY 1`,
		[]sql.Row{
			{"row"},
		},
	},
	{
		`SELECT s FROM mytable WHERE s NOT LIKE '%d row'`,
		[]sql.Row{
			{"first row"},
		},
	},
	{
		`SELECT * FROM foo.other_table`,
		[]sql.Row{
			{"a", int32(4)},
			{"b", int32(2)},
			{"c", int32(0)},
		},
	},
	{
		`SELECT AVG(23.222000)`,
		[]sql.Row{
			{float64(23.222)},
		},
	},
	{
		`SELECT DATABASE()`,
		[]sql.Row{
			{"mydb"},
		},
	},
	{
		`SELECT USER()`,
		[]sql.Row{
			{"user"},
		},
	},
	{
		`SHOW VARIABLES`,
		[]sql.Row{
			{"auto_increment_increment", int64(1)},
			{"time_zone", "SYSTEM"},
			{"system_time_zone", time.Now().UTC().Location().String()},
			{"max_allowed_packet", math.MaxInt32},
			{"sql_mode", ""},
			{"gtid_mode", int32(0)},
			{"collation_database", "utf8_bin"},
			{"ndbinfo_version", ""},
			{"sql_select_limit", math.MaxInt32},
			{"transaction_isolation", "READ UNCOMMITTED"},
			{"version", ""},
			{"version_comment", ""},
		},
	},
	{
		`SHOW VARIABLES LIKE 'gtid_mode`,
		[]sql.Row{
			{"gtid_mode", int32(0)},
		},
	},
	{
		`SHOW VARIABLES LIKE 'gtid%`,
		[]sql.Row{
			{"gtid_mode", int32(0)},
		},
	},
	{
		`SHOW GLOBAL VARIABLES LIKE '%mode`,
		[]sql.Row{
			{"sql_mode", ""},
			{"gtid_mode", int32(0)},
		},
	},
	{
		`SELECT JSON_EXTRACT("foo", "$")`,
		[]sql.Row{{"foo"}},
	},
	{
		`SELECT JSON_UNQUOTE('"foo"')`,
		[]sql.Row{{"foo"}},
	},
	{
		`SELECT JSON_UNQUOTE('[1, 2, 3]')`,
		[]sql.Row{{"[1, 2, 3]"}},
	},
	{
		`SELECT JSON_UNQUOTE('"\\t\\u0032"')`,
		[]sql.Row{{"\t2"}},
	},
	{
		`SELECT JSON_UNQUOTE('"\t\\u0032"')`,
		[]sql.Row{{"\t2"}},
	},
	{
		`SELECT CONNECTION_ID()`,
		[]sql.Row{{uint32(1)}},
	},
	{
		`SHOW CREATE DATABASE mydb`,
		[]sql.Row{{
			"mydb",
			"CREATE DATABASE `mydb` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8_bin */",
		}},
	},
	{
		`SHOW CREATE TABLE two_pk`,
		[]sql.Row{{
			"two_pk",
			"CREATE TABLE `two_pk` (\n" +
				"  `pk1` tinyint NOT NULL,\n" +
				"  `pk2` tinyint NOT NULL,\n" +
				"  `c1` tinyint NOT NULL,\n" +
				"  `c2` tinyint NOT NULL,\n" +
				"  `c3` tinyint NOT NULL,\n" +
				"  `c4` tinyint NOT NULL,\n" +
				"  `c5` tinyint NOT NULL,\n" +
				"  PRIMARY KEY (`pk1`,`pk2`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		}},
	},
	{
		`SHOW CREATE TABLE myview`,
		[]sql.Row{{
			"myview",
			"CREATE VIEW `myview` AS SELECT * FROM mytable",
		}},
	},
	{
		`SHOW CREATE VIEW myview`,
		[]sql.Row{{
			"myview",
			"CREATE VIEW `myview` AS SELECT * FROM mytable",
		}},
	},
	{
		`SELECT -1`,
		[]sql.Row{{int8(-1)}},
	},
	{
		`
		SHOW WARNINGS
		`,
		nil,
	},
	{
		`SHOW WARNINGS LIMIT 0`,
		nil,
	},
	{
		`SET SESSION NET_READ_TIMEOUT= 700, SESSION NET_WRITE_TIMEOUT= 700`,
		nil,
	},
	{
		`SELECT NULL`,
		[]sql.Row{
			{nil},
		},
	},
	{
		`SELECT nullif('abc', NULL)`,
		[]sql.Row{
			{"abc"},
		},
	},
	{
		`SELECT nullif(NULL, NULL)`,
		[]sql.Row{
			{sql.Null},
		},
	},
	{
		`SELECT nullif(NULL, 123)`,
		[]sql.Row{
			{nil},
		},
	},
	{
		`SELECT nullif(123, 123)`,
		[]sql.Row{
			{sql.Null},
		},
	},
	{
		`SELECT nullif(123, 321)`,
		[]sql.Row{
			{int8(123)},
		},
	},
	{
		`SELECT ifnull(123, NULL)`,
		[]sql.Row{
			{int8(123)},
		},
	},
	{
		`SELECT ifnull(NULL, NULL)`,
		[]sql.Row{
			{nil},
		},
	},
	{
		`SELECT ifnull(NULL, 123)`,
		[]sql.Row{
			{int8(123)},
		},
	},
	{
		`SELECT ifnull(123, 123)`,
		[]sql.Row{
			{int8(123)},
		},
	},
	{
		`SELECT ifnull(123, 321)`,
		[]sql.Row{
			{int8(123)},
		},
	},
	{
		`SELECT if(123 = 123, "a", "b")`,
		[]sql.Row{
			{"a"},
		},
	},
	{
		`SELECT if(123 = 123, NULL, "b")`,
		[]sql.Row{
			{nil},
		},
	},
	{
		`SELECT if(123 > 123, "a", "b")`,
		[]sql.Row{
			{"b"},
		},
	},
	{
		`SELECT if(NULL, "a", "b")`,
		[]sql.Row{
			{"b"},
		},
	},
	{
		`SELECT if("a", "a", "b")`,
		[]sql.Row{
			{"b"},
		},
	},
	{
		"SELECT i FROM mytable WHERE NULL > 10;",
		nil,
	},
	{
		"SELECT i FROM mytable WHERE NULL IN (10);",
		nil,
	},
	{
		"SELECT i FROM mytable WHERE NULL IN (NULL, NULL);",
		nil,
	},
	{
		"SELECT i FROM mytable WHERE NOT NULL NOT IN (NULL);",
		nil,
	},
	{
		"SELECT i FROM mytable WHERE NOT (NULL) <> 10;",
		nil,
	},
	{
		"SELECT i FROM mytable WHERE NOT NULL <> NULL;",
		nil,
	},
	{
		`SELECT round(15728640/1024/1024)`,
		[]sql.Row{
			{int64(15)},
		},
	},
	{
		`SELECT round(15, 1)`,
		[]sql.Row{
			{int8(15)},
		},
	},
	{
		`SELECT CASE i WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM mytable`,
		[]sql.Row{
			{"one"},
			{"two"},
			{"other"},
		},
	},
	{
		`SELECT CASE WHEN i > 2 THEN 'more than two' WHEN i < 2 THEN 'less than two' ELSE 'two' END FROM mytable`,
		[]sql.Row{
			{"less than two"},
			{"two"},
			{"more than two"},
		},
	},
	{
		`SELECT CASE i WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM mytable`,
		[]sql.Row{
			{"one"},
			{"two"},
			{nil},
		},
	},
	{
		`SHOW COLLATION`,
		[]sql.Row{{"utf8_bin", "utf8mb4", int64(1), "Yes", "Yes", int64(1), "PAD SPACE"}},
	},
	{
		`SHOW COLLATION LIKE 'foo'`,
		nil,
	},
	{
		`SHOW COLLATION LIKE 'utf8%'`,
		[]sql.Row{{"utf8_bin", "utf8mb4", int64(1), "Yes", "Yes", int64(1), "PAD SPACE"}},
	},
	{
		`SHOW COLLATION WHERE charset = 'foo'`,
		nil,
	},
	{
		"SHOW COLLATION WHERE `Default` = 'Yes'",
		[]sql.Row{{"utf8_bin", "utf8mb4", int64(1), "Yes", "Yes", int64(1), "PAD SPACE"}},
	},
	{
		"ROLLBACK",
		nil,
	},
	{
		"SELECT substring(s, 1, 1) FROM mytable ORDER BY substring(s, 1, 1)",
		[]sql.Row{{"f"}, {"s"}, {"t"}},
	},
	{
		"SELECT substring(s, 1, 1), count(*) FROM mytable GROUP BY substring(s, 1, 1)",
		[]sql.Row{{"f", int64(1)}, {"s", int64(1)}, {"t", int64(1)}},
	},
	{
		"SELECT left(s, 1) as l FROM mytable ORDER BY l",
		[]sql.Row{{"f"}, {"s"}, {"t"}},
	},
	{
		"SELECT left(s, 2) as l FROM mytable ORDER BY l",
		[]sql.Row{{"fi"}, {"se"}, {"th"}},
	},
	{
		"SELECT left(s, 0) as l FROM mytable ORDER BY l",
		[]sql.Row{{""}, {""}, {""}},
	},
	{
		"SELECT left(s, NULL) as l FROM mytable ORDER BY l",
		[]sql.Row{{nil}, {nil}, {nil}},
	},
	{
		"SELECT left(s, 100) as l FROM mytable ORDER BY l",
		[]sql.Row{{"first row"}, {"second row"}, {"third row"}},
	},
	{
		"SELECT instr(s, 'row') as l FROM mytable ORDER BY i",
		[]sql.Row{{int64(7)}, {int64(8)}, {int64(7)}},
	},
	{
		"SELECT instr(s, 'first') as l FROM mytable ORDER BY i",
		[]sql.Row{{int64(1)}, {int64(0)}, {int64(0)}},
	},
	{
		"SELECT instr(s, 'o') as l FROM mytable ORDER BY i",
		[]sql.Row{{int64(8)}, {int64(4)}, {int64(8)}},
	},
	{
		"SELECT instr(s, NULL) as l FROM mytable ORDER BY l",
		[]sql.Row{{nil}, {nil}, {nil}},
	},
	{
		"SELECT SLEEP(0.5)",
		[]sql.Row{{int(0)}},
	},
	{
		"SELECT TO_BASE64('foo')",
		[]sql.Row{{string("Zm9v")}},
	},
	{
		"SELECT FROM_BASE64('YmFy')",
		[]sql.Row{{string("bar")}},
	},
	{
		"SELECT DATE_ADD('2018-05-02', INTERVAL 1 day)",
		[]sql.Row{{time.Date(2018, time.May, 3, 0, 0, 0, 0, time.UTC)}},
	},
	{
		"SELECT DATE_SUB('2018-05-02', INTERVAL 1 DAY)",
		[]sql.Row{{time.Date(2018, time.May, 1, 0, 0, 0, 0, time.UTC)}},
	},
	{
		"SELECT '2018-05-02' + INTERVAL 1 DAY",
		[]sql.Row{{time.Date(2018, time.May, 3, 0, 0, 0, 0, time.UTC)}},
	},
	{
		"SELECT '2018-05-02' - INTERVAL 1 DAY",
		[]sql.Row{{time.Date(2018, time.May, 1, 0, 0, 0, 0, time.UTC)}},
	},
	{
		`SELECT i AS i FROM mytable ORDER BY i`,
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		`
		SELECT
			i,
			foo
		FROM (
			SELECT
				i,
				COUNT(s) AS foo
			FROM mytable
			GROUP BY i
		) AS q
		ORDER BY foo DESC
		`,
		[]sql.Row{
			{int64(1), int64(1)},
			{int64(2), int64(1)},
			{int64(3), int64(1)},
		},
	},
	{
		"SELECT n, COUNT(n) FROM bigtable GROUP BY n HAVING COUNT(n) > 2",
		[]sql.Row{{int64(1), int64(3)}, {int64(2), int64(3)}},
	},
	{
		"SELECT n, MAX(n) FROM bigtable GROUP BY n HAVING COUNT(n) > 2",
		[]sql.Row{{int64(1), int64(1)}, {int64(2), int64(2)}},
	},
	{
		"SELECT substring(mytable.s, 1, 5) AS s FROM mytable INNER JOIN othertable ON (substring(mytable.s, 1, 5) = SUBSTRING(othertable.s2, 1, 5)) GROUP BY 1 HAVING s = \"secon\"",
		[]sql.Row{{"secon"}},
	},
	{
		"SELECT s,  i FROM mytable GROUP BY i ORDER BY SUBSTRING(s, 1, 1) DESC",
		[]sql.Row{
			{string("third row"), int64(3)},
			{string("second row"), int64(2)},
			{string("first row"), int64(1)},
		},
	},
	{
		"SELECT s, i FROM mytable GROUP BY i HAVING count(*) > 0 ORDER BY SUBSTRING(s, 1, 1) DESC",
		[]sql.Row{
			{string("third row"), int64(3)},
			{string("second row"), int64(2)},
			{string("first row"), int64(1)},
		},
	},
	{
		"SELECT CONVERT('9999-12-31 23:59:59', DATETIME)",
		[]sql.Row{{time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)}},
	},
	{
		"SELECT DATETIME('9999-12-31 23:59:59')",
		[]sql.Row{{time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)}},
	},
	{
		"SELECT TIMESTAMP('2020-12-31 23:59:59')",
		[]sql.Row{{time.Date(2020, time.December, 31, 23, 59, 59, 0, time.UTC)}},
	},
	{
		"SELECT CONVERT('10000-12-31 23:59:59', DATETIME)",
		[]sql.Row{{nil}},
	},
	{
		"SELECT '9999-12-31 23:59:59' + INTERVAL 1 DAY",
		[]sql.Row{{nil}},
	},
	{
		"SELECT DATE_ADD('9999-12-31 23:59:59', INTERVAL 1 DAY)",
		[]sql.Row{{nil}},
	},
	{
		`SELECT t.date_col FROM (SELECT CONVERT('2019-06-06 00:00:00', DATETIME) AS date_col) t WHERE t.date_col > '0000-01-01 00:00:00'`,
		[]sql.Row{{time.Date(2019, time.June, 6, 0, 0, 0, 0, time.UTC)}},
	},
	{
		`SELECT t.date_col FROM (SELECT CONVERT('2019-06-06 00:00:00', DATETIME) as date_col) t GROUP BY t.date_col`,
		[]sql.Row{{time.Date(2019, time.June, 6, 0, 0, 0, 0, time.UTC)}},
	},
	{
		`SELECT i AS foo FROM mytable ORDER BY mytable.i`,
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		`SELECT JSON_EXTRACT('[1, 2, 3]', '$.[0]')`,
		[]sql.Row{{float64(1)}},
	},
	{
		`SELECT ARRAY_LENGTH(JSON_EXTRACT('[1, 2, 3]', '$'))`,
		[]sql.Row{{int32(3)}},
	},
	{
		`SELECT ARRAY_LENGTH(JSON_EXTRACT('[{"i":0}, {"i":1, "y":"yyy"}, {"i":2, "x":"xxx"}]', '$.i'))`,
		[]sql.Row{{int32(3)}},
	},
	{
		`SELECT GREATEST(1, 2, 3, 4)`,
		[]sql.Row{{int64(4)}},
	},
	{
		`SELECT GREATEST(1, 2, "3", 4)`,
		[]sql.Row{{float64(4)}},
	},
	{
		`SELECT GREATEST(1, 2, "9", "foo999")`,
		[]sql.Row{{float64(9)}},
	},
	{
		`SELECT GREATEST("aaa", "bbb", "ccc")`,
		[]sql.Row{{"ccc"}},
	},
	{
		`SELECT GREATEST(i, s) FROM mytable`,
		[]sql.Row{{float64(1)}, {float64(2)}, {float64(3)}},
	},
	{
		`SELECT GREATEST(CAST("1920-02-03 07:41:11" AS DATETIME), CAST("1980-06-22 14:32:56" AS DATETIME))`,
		[]sql.Row{{time.Date(1980, 6, 22, 14, 32, 56, 0, time.UTC)}},
	},
	{
		`SELECT LEAST(1, 2, 3, 4)`,
		[]sql.Row{{int64(1)}},
	},
	{
		`SELECT LEAST(1, 2, "3", 4)`,
		[]sql.Row{{float64(1)}},
	},
	{
		`SELECT LEAST(1, 2, "9", "foo999")`,
		[]sql.Row{{float64(1)}},
	},
	{
		`SELECT LEAST("aaa", "bbb", "ccc")`,
		[]sql.Row{{"aaa"}},
	},
	{
		`SELECT LEAST(i, s) FROM mytable`,
		[]sql.Row{{float64(1)}, {float64(2)}, {float64(3)}},
	},
	{
		`SELECT LEAST(CAST("1920-02-03 07:41:11" AS DATETIME), CAST("1980-06-22 14:32:56" AS DATETIME))`,
		[]sql.Row{{time.Date(1920, 2, 3, 7, 41, 11, 0, time.UTC)}},
	},
	{
		"SELECT i, i2, s2 FROM mytable LEFT JOIN othertable ON i = i2 - 1",
		[]sql.Row{
			{int64(1), int64(2), "second"},
			{int64(2), int64(3), "first"},
			{int64(3), nil, nil},
		},
	},
	{
		"SELECT i, i2, s2 FROM mytable RIGHT JOIN othertable ON i = i2 - 1",
		[]sql.Row{
			{nil, int64(1), "third"},
			{int64(1), int64(2), "second"},
			{int64(2), int64(3), "first"},
		},
	},
	{
		"SELECT i, i2, s2 FROM mytable LEFT OUTER JOIN othertable ON i = i2 - 1",
		[]sql.Row{
			{int64(1), int64(2), "second"},
			{int64(2), int64(3), "first"},
			{int64(3), nil, nil},
		},
	},
	{
		"SELECT i, i2, s2 FROM mytable RIGHT OUTER JOIN othertable ON i = i2 - 1",
		[]sql.Row{
			{nil, int64(1), "third"},
			{int64(1), int64(2), "second"},
			{int64(2), int64(3), "first"},
		},
	},
	{
		`SELECT CHAR_LENGTH('áé'), LENGTH('àè')`,
		[]sql.Row{{int32(2), int32(4)}},
	},
	{
		"SELECT i, COUNT(i) AS `COUNT(i)` FROM (SELECT i FROM mytable) t GROUP BY i ORDER BY i, `COUNT(i)` DESC",
		[]sql.Row{{int64(1), int64(1)}, {int64(2), int64(1)}, {int64(3), int64(1)}},
	},
	{
		"SELECT i FROM mytable WHERE NOT s ORDER BY 1 DESC",
		[]sql.Row{
			{int64(3)},
			{int64(2)},
			{int64(1)},
		},
	},
	{
		"SELECT i FROM mytable WHERE NOT(NOT i) ORDER BY 1 DESC",
		[]sql.Row{
			{int64(3)},
			{int64(2)},
			{int64(1)},
		},
	},
	{
		`SELECT NOW() - NOW()`,
		[]sql.Row{{int64(0)}},
	},
	{
		`SELECT DATETIME(NOW()) - NOW()`,
		[]sql.Row{{int64(0)}},
	},
	{
		`SELECT TIMESTAMP(NOW()) - NOW()`,
		[]sql.Row{{int64(0)}},
	},
	{
		`SELECT NOW() - (NOW() - INTERVAL 1 SECOND)`,
		[]sql.Row{{int64(1)}},
	},
	{
		`SELECT SUBSTR(SUBSTRING('0123456789ABCDEF', 1, 10), -4)`,
		[]sql.Row{{"6789"}},
	},
	{
		`SELECT CASE i WHEN 1 THEN i ELSE NULL END FROM mytable`,
		[]sql.Row{{int64(1)}, {nil}, {nil}},
	},
	{
		`SELECT (NULL+1)`,
		[]sql.Row{{nil}},
	},
	{
		`SELECT ARRAY_LENGTH(null)`,
		[]sql.Row{{nil}},
	},
	{
		`SELECT ARRAY_LENGTH("foo")`,
		[]sql.Row{{nil}},
	},
	{
		`SELECT * FROM mytable WHERE NULL AND i = 3`,
		nil,
	},
	{
		`SELECT 1 FROM mytable GROUP BY i HAVING i > 1`,
		[]sql.Row{{int8(1)}, {int8(1)}},
	},
	{
		`SELECT avg(i) FROM mytable GROUP BY i HAVING avg(i) > 1`,
		[]sql.Row{{float64(2)}, {float64(3)}},
	},
	{
		`SELECT s AS s, COUNT(*) AS count,  AVG(i) AS ` + "`AVG(i)`" + `
		FROM  (
			SELECT * FROM mytable
		) AS expr_qry
		GROUP BY s
		HAVING ((AVG(i) > 0))
		ORDER BY count DESC
		LIMIT 10000`,
		[]sql.Row{
			{"first row", int64(1), float64(1)},
			{"second row", int64(1), float64(2)},
			{"third row", int64(1), float64(3)},
		},
	},
	{
		`SELECT FIRST(i) FROM (SELECT i FROM mytable ORDER BY i) t`,
		[]sql.Row{{int64(1)}},
	},
	{
		`SELECT LAST(i) FROM (SELECT i FROM mytable ORDER BY i) t`,
		[]sql.Row{{int64(3)}},
	},
	{
		`SELECT COUNT(DISTINCT t.i) FROM tabletest t, mytable t2`,
		[]sql.Row{{int64(3)}},
	},
	{
		`SELECT CASE WHEN NULL THEN "yes" ELSE "no" END AS test`,
		[]sql.Row{{"no"}},
	},
	{
		`SELECT
			table_schema,
			table_name,
			CASE
				WHEN table_type = 'BASE TABLE' THEN
					CASE
						WHEN table_schema = 'mysql'
							OR table_schema = 'performance_schema' THEN 'SYSTEM TABLE'
						ELSE 'TABLE'
					END
				WHEN table_type = 'TEMPORARY' THEN 'LOCAL_TEMPORARY'
				ELSE table_type
			END AS TABLE_TYPE
		FROM information_schema.tables
		WHERE table_schema = 'mydb'
			AND table_name = 'mytable'
		HAVING table_type IN ('TABLE', 'VIEW')
		ORDER BY table_type, table_schema, table_name`,
		[]sql.Row{{"mydb", "mytable", "TABLE"}},
	},
	{
		`SELECT REGEXP_MATCHES("bopbeepbop", "bop")`,
		[]sql.Row{{[]interface{}{"bop", "bop"}}},
	},
	{
		`SELECT EXPLODE(REGEXP_MATCHES("bopbeepbop", "bop"))`,
		[]sql.Row{{"bop"}, {"bop"}},
	},
	{
		`SELECT EXPLODE(REGEXP_MATCHES("helloworld", "bop"))`,
		nil,
	},
	{
		`SELECT EXPLODE(REGEXP_MATCHES("", ""))`,
		[]sql.Row{{""}},
	},
	{
		`SELECT REGEXP_MATCHES(NULL, "")`,
		[]sql.Row{{nil}},
	},
	{
		`SELECT REGEXP_MATCHES("", NULL)`,
		[]sql.Row{{nil}},
	},
	{
		`SELECT REGEXP_MATCHES("", "", NULL)`,
		[]sql.Row{{nil}},
	},
	{
		"SELECT * FROM newlinetable WHERE s LIKE '%text%'",
		[]sql.Row{
			{int64(1), "\nthere is some text in here"},
			{int64(2), "there is some\ntext in here"},
			{int64(3), "there is some text\nin here"},
			{int64(4), "there is some text in here\n"},
			{int64(5), "there is some text in here"},
		},
	},
	{
		`SELECT i FROM mytable WHERE i = (SELECT 1)`,
		[]sql.Row{{int64(1)}},
	},
	{
		`SELECT i FROM mytable WHERE i IN (SELECT i FROM mytable) ORDER BY i`,
		[]sql.Row{
			{int64(1)},
			{int64(2)},
			{int64(3)},
		},
	},
	{
		`SELECT i FROM mytable WHERE i IN (SELECT i FROM mytable ORDER BY i ASC LIMIT 2) ORDER BY i`,
		[]sql.Row{
			{int64(1)},
			{int64(2)},
		},
	},
	{
		`SELECT i FROM mytable WHERE i NOT IN (SELECT i FROM mytable ORDER BY i ASC LIMIT 2)`,
		[]sql.Row{
			{int64(3)},
		},
	},
	{
		`SELECT i FROM mytable WHERE i NOT IN (SELECT i FROM mytable ORDER BY i ASC LIMIT 1) ORDER BY i`,
		[]sql.Row{
			{2},
			{3},
		},
	},
	{
		`SELECT (SELECT i FROM mytable ORDER BY i ASC LIMIT 1) AS x`,
		[]sql.Row{{int64(1)}},
	},
	{
		`SELECT (SELECT s FROM mytable ORDER BY i ASC LIMIT 1) AS x`,
		[]sql.Row{{"first row"}},
	},
	{
		`SELECT pk, (SELECT pk FROM one_pk WHERE pk < opk.pk ORDER BY 1 DESC LIMIT 1) FROM one_pk opk ORDER BY 1`,
		[]sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		`SELECT pk, (SELECT c3 FROM one_pk WHERE pk < opk.pk ORDER BY 1 DESC LIMIT 1) FROM one_pk opk ORDER BY 1`,
		[]sql.Row{
			{0, nil},
			{1, 0},
			{2, 10},
			{3, 20},
		},
	},
	{
		`SELECT pk, (SELECT c5 FROM one_pk WHERE c5 < opk.c5 ORDER BY 1 DESC LIMIT 1) FROM one_pk opk ORDER BY 1`,
		[]sql.Row{
			{0, nil},
			{1, 0},
			{2, 10},
			{3, 20},
		},
	},
	{
		`SELECT pk, (SELECT pk FROM one_pk WHERE c1 < opk.c1 ORDER BY 1 DESC LIMIT 1) FROM one_pk opk ORDER BY 1;`,
		[]sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		`SELECT pk, (SELECT c3 FROM one_pk WHERE c4 < opk.c2 ORDER BY 1 DESC LIMIT 1) FROM one_pk opk ORDER BY 1;`,
		[]sql.Row{
			{0, nil},
			{1, 0},
			{2, 10},
			{3, 20},
		},
	},
	{
		`SELECT pk,
					(SELECT c3 FROM one_pk WHERE c4 < opk.c2 ORDER BY 1 DESC LIMIT 1),
					(SELECT c5 + 1 FROM one_pk WHERE c5 < opk.c5 ORDER BY 1 DESC LIMIT 1)
					FROM one_pk opk ORDER BY 1;`,
		[]sql.Row{
			{0, nil, nil},
			{1, 0, 1},
			{2, 10, 11},
			{3, 20, 21},
		},
	},
	{
		`SELECT pk, 
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk),
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk)
					FROM one_pk opk ORDER BY 1;`,
		[]sql.Row{
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
			{3, 2, nil},
		},
	},
	{
		`SELECT pk, 
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk 
					WHERE (SELECT min(pk) FROM one_pk WHERE pk > opk.pk) IS NOT NULL
					ORDER BY max;`,
		[]sql.Row{
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		`SELECT pk, 
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min 
					FROM one_pk opk
					WHERE (SELECT max(pk) FROM one_pk WHERE pk >= opk.pk) > 0
					ORDER BY min;`,
		[]sql.Row{
			{3, 2, nil},
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		`SELECT pk, 
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min 
					FROM one_pk opk
					WHERE (SELECT max(pk) FROM one_pk WHERE pk > opk.pk) > 0
					ORDER BY min;`,
		[]sql.Row{
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		`SELECT pk, 
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min 
					FROM one_pk opk
					WHERE (SELECT max(pk) FROM one_pk WHERE pk > opk.pk) > 0
					ORDER BY max;`,
		[]sql.Row{
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		`SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk
					WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) IS NOT NULL
					ORDER BY min;`,
		[]sql.Row{
			{3, 2, nil},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		`SELECT pk, 
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min 
					FROM one_pk opk ORDER BY min;`,
		[]sql.Row{
			{3, 2, nil},
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x FROM one_pk opk GROUP BY x ORDER BY x`,
		[]sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		`SELECT pk, 
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min 
					FROM one_pk opk
					WHERE (SELECT max(pk) FROM one_pk WHERE pk >= opk.pk)
					ORDER BY min;`,
		[]sql.Row{
			{3, 2, nil},
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		`SELECT pk FROM one_pk
					WHERE (SELECT max(pk1) FROM two_pk WHERE pk1 >= pk) IS NOT NULL
					ORDER BY 1;`,
		[]sql.Row{
			{0},
			{1},
		},
	},
	{
		`SELECT pk FROM one_pk opk
					WHERE (SELECT count(*) FROM two_pk where pk1 * 10 <= opk.c1) > 2
					ORDER BY 1;`,
		[]sql.Row{
			{1},
			{2},
			{3},
		},
	},
	{
		`SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk
					WHERE (SELECT max(pk) FROM one_pk WHERE pk >= opk.pk) > 0
					ORDER BY min;`,
		[]sql.Row{
			{3, 2, nil},
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk WHERE one_pk.pk * 10 <= opk.c1) FROM one_pk opk ORDER BY 1`,
		[]sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk <= opk.pk) FROM one_pk opk ORDER BY 1`,
		[]sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) FROM one_pk opk ORDER BY 1`,
		[]sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) FROM one_pk opk ORDER BY 2`,
		[]sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x FROM one_pk opk ORDER BY x`,
		[]sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x 
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) IS NOT NULL ORDER BY x`,
		[]sql.Row{
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max 
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) IS NOT NULL ORDER BY max`,
		[]sql.Row{
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x 
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) > 0 ORDER BY x`,
		[]sql.Row{
			{2, 1},
			{3, 2},
		},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x 
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) > 0 
						GROUP BY x ORDER BY x`,
		[]sql.Row{
			{2, 1},
			{3, 2},
		},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x 
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) > 0 
						GROUP BY (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) ORDER BY x`,
		[]sql.Row{
			{2, 1},
			{3, 2},
		},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x 
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk > opk.pk) > 0 ORDER BY x`,
		[]sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
		},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x 
						FROM one_pk opk WHERE (SELECT min(pk) FROM one_pk WHERE pk < opk.pk) > 0 ORDER BY x`,
		[]sql.Row{},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x 
						FROM one_pk opk WHERE (SELECT min(pk) FROM one_pk WHERE pk > opk.pk) > 0 ORDER BY x`,
		[]sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
		},
	},
	{
		`SELECT pk, 
					(SELECT max(pk1) FROM two_pk WHERE pk1 < pk) AS max,
					(SELECT min(pk2) FROM two_pk WHERE pk2 > pk) AS min 
					FROM one_pk ORDER BY min, pk;`,
		[]sql.Row{
			{1, 0, nil},
			{2, 1, nil},
			{3, 1, nil},
			{0, nil, 1},
		},
	},
	{
		`SELECT pk,
						(SELECT max(pk1) FROM two_pk tpk WHERE pk1 IN (SELECT pk1 FROM two_pk WHERE pk1 = tpk.pk2)) AS one,
						(SELECT min(pk2) FROM two_pk tpk WHERE pk2 IN (SELECT pk2 FROM two_pk WHERE pk2 = tpk.pk1)) AS zero
						FROM one_pk ORDER BY pk;`,
		[]sql.Row{
			{0, 1, 0},
			{1, 1, 0},
			{2, 1, 0},
			{3, 1, 0},
		},
	},
	{
		`SELECT pk,
						(SELECT sum(pk1+pk2) FROM two_pk WHERE pk1+pk2 IN (SELECT pk1+pk2 FROM two_pk WHERE pk1+pk2 = pk)) AS sum,
						(SELECT min(pk2) FROM two_pk WHERE pk2 IN (SELECT pk2 FROM two_pk WHERE pk2 = pk)) AS equal
						FROM one_pk ORDER BY pk;`,
		[]sql.Row{
			{0, 0.0, 0},
			{1, 2.0, 1},
			{2, 2.0, nil},
			{3, nil, nil},
		},
	},
	{
		`SELECT pk,
						(SELECT sum(c1) FROM two_pk WHERE c1 IN (SELECT c4 FROM two_pk WHERE c3 > opk.c5)) AS sum,
						(SELECT sum(c1) FROM two_pk WHERE pk2 IN (SELECT pk2 FROM two_pk WHERE c1 < opk.c2)) AS sum2
					FROM one_pk opk ORDER BY pk`,
		[]sql.Row{
			{0, 60.0, nil},
			{1, 50.0, 20.0},
			{2, 30.0, 60.0},
			{3, nil, 60.0},
		},
	},
	{
		`SELECT pk, (SELECT min(pk) FROM one_pk WHERE pk > opk.pk) FROM one_pk opk ORDER BY 1`,
		[]sql.Row{
			{0, 1},
			{1, 2},
			{2, 3},
			{3, nil},
		},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk WHERE one_pk.pk <= one_pk.pk) FROM one_pk ORDER BY 1`,
		[]sql.Row{
			{0, 3},
			{1, 3},
			{2, 3},
			{3, 3},
		},
	},
	{
		`SELECT pk as a, (SELECT max(pk) FROM one_pk WHERE pk <= a) FROM one_pk ORDER BY 1`,
		[]sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		`SELECT pk as a, (SELECT max(pk) FROM one_pk WHERE pk <= a) FROM one_pk opk ORDER BY 1`,
		[]sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk b WHERE b.pk <= opk.pk) FROM one_pk opk ORDER BY 1`,
		[]sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk <= pk) FROM one_pk opk ORDER BY 1`,
		[]sql.Row{
			{0, 3},
			{1, 3},
			{2, 3},
			{3, 3},
		},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk b WHERE b.pk <= pk) FROM one_pk opk ORDER BY 1`,
		[]sql.Row{
			{0, 3},
			{1, 3},
			{2, 3},
			{3, 3},
		},
	},
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk b WHERE b.pk <= one_pk.pk) FROM one_pk ORDER BY 1`,
		[]sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		`SELECT DISTINCT n FROM bigtable ORDER BY t`,
		[]sql.Row{
			{int64(1)},
			{int64(9)},
			{int64(7)},
			{int64(3)},
			{int64(2)},
			{int64(8)},
			{int64(6)},
			{int64(5)},
			{int64(4)},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk, two_pk ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{0, 1, 0},
			{0, 1, 1},
			{1, 0, 0},
			{1, 0, 1},
			{1, 1, 0},
			{1, 1, 1},
			{2, 0, 0},
			{2, 0, 1},
			{2, 1, 0},
			{2, 1, 1},
			{3, 0, 0},
			{3, 0, 1},
			{3, 1, 0},
			{3, 1, 1},
		},
	},
	{
		"SELECT t1.c1,t2.c2 FROM one_pk t1, two_pk t2 WHERE pk1=1 AND pk2=1 ORDER BY 1,2",
		[]sql.Row{
			{0, 30},
			{10, 30},
			{20, 30},
			{30, 30},
		},
	},
	{
		"SELECT t1.c1,t2.c2 FROM one_pk t1, two_pk t2 WHERE t2.pk1=1 AND t2.pk2=1 ORDER BY 1,2",
		[]sql.Row{
			{0, 30},
			{10, 30},
			{20, 30},
			{30, 30},
		},
	},
	{
		"SELECT t1.c1,t2.c2 FROM one_pk t1, two_pk t2 WHERE pk1=1 OR pk2=1 ORDER BY 1,2",
		[]sql.Row{
			{0, 10},
			{0, 20},
			{0, 30},
			{10, 10},
			{10, 20},
			{10, 30},
			{20, 10},
			{20, 20},
			{20, 30},
			{30, 10},
			{30, 20},
			{30, 30},
		},
	},
	{
		"SELECT pk,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2",
		[]sql.Row{
			{1, 1},
			{1, 1},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk,two_pk WHERE pk=0 AND pk1=0 OR pk2=1 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{0, 1, 1},
			{1, 0, 1},
			{1, 1, 1},
			{2, 0, 1},
			{2, 1, 1},
			{3, 0, 1},
			{3, 1, 1},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk,two_pk WHERE one_pk.c1=two_pk.c1 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{1, 0, 1},
			{2, 1, 0},
			{3, 1, 1},
		},
	},
	{
		"SELECT one_pk.c5,pk1,pk2 FROM one_pk,two_pk WHERE pk=pk1 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{10, 1, 0},
			{10, 1, 1},
		},
	},
	{
		"SELECT opk.c5,pk1,pk2 FROM one_pk opk, two_pk tpk WHERE pk=pk1 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{10, 1, 0},
			{10, 1, 1},
		},
	},
	{
		"SELECT one_pk.c5,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{10, 1, 0},
			{10, 1, 1},
		},
	},
	{
		"SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=pk1 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{10, 1, 0},
			{10, 1, 1},
		},
	},
	{
		"SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{10, 1, 0},
			{10, 1, 1},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE pk=1 ORDER BY 1,2,3",
		[]sql.Row{
			{1, 0, 1},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{1, 1, 1},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{1, 1, 1},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=tpk.pk1 AND pk=tpk.pk2 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{1, 1, 1},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{1, 1, 1},
			{2, nil, nil},
			{3, nil, nil},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
		[]sql.Row{
			{nil, 0, 1},
			{nil, 1, 0},
			{0, 0, 0},
			{1, 1, 1},
		},
	},
	{
		"SELECT i,pk1,pk2 FROM mytable JOIN two_pk ON i-1=pk1 AND i-2=pk2 ORDER BY 1,2,3",
		[]sql.Row{
			{int64(2), 1, 0},
		},
	},
	{
		"SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0, 0},
			{0, 1, 1, 0},
			{1, 0, 0, 1},
			{1, 1, 1, 1},
		},
	},
	{
		"SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0, 0},
			{0, 1, 0, 1},
			{1, 0, 1, 0},
			{1, 1, 1, 1},
		},
	},
	{
		"SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0, 0},
			{0, 1, 0, 1},
			{1, 0, 1, 0},
			{1, 1, 1, 1},
		},
	},
	{
		"SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON b.pk1=a.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0, 0},
			{0, 1, 0, 1},
			{1, 0, 1, 0},
			{1, 1, 1, 1},
		},
	},
	{
		"SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1+1=b.pk1 AND a.pk2+1=b.pk2 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 1, 1},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{1, 1, 0},
			{1, 1, 1},
			{2, nil, nil},
			{3, nil, nil},
		},
	},
	{
		"SELECT pk,i2,f FROM one_pk LEFT JOIN niltable ON pk=i2 ORDER BY 1",
		[]sql.Row{
			{0, nil, nil},
			{1, nil, nil},
			{2, int64(2), nil},
			{3, nil, nil},
		},
	},
	{
		"SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i2 ORDER BY 2,3",
		[]sql.Row{
			{nil, nil, nil},
			{nil, nil, nil},
			{nil, nil, 5.0},
			{2, int64(2), nil},
			{nil, int64(4), 4.0},
			{nil, int64(6), 6.0},
		},
	},
	{
		"SELECT pk,i2,f FROM one_pk LEFT JOIN niltable ON pk=i2 AND f IS NOT NULL ORDER BY 1", // AND clause causes right table join miss
		[]sql.Row{
			{0, nil, nil},
			{1, nil, nil},
			{2, nil, nil},
			{3, nil, nil},
		},
	},
	{
		"SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i2 and pk > 0 ORDER BY 2,3", // > 0 clause in join condition is ignored
		[]sql.Row{
			{nil, nil, nil},
			{nil, nil, nil},
			{nil, nil, 5.0},
			{2, int64(2), nil},
			{nil, int64(4), 4.0},
			{nil, int64(6), 6.0},
		},
	},
	{
		"SELECT pk,i2,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i2 IS NOT NULL ORDER BY 1",
		[]sql.Row{
			{2, int64(2), nil},
		},
	},
	{
		"SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 2,3",
		[]sql.Row{
			{nil, nil, 5.0},
			{nil, int64(4), 4.0},
			{nil, int64(6), 6.0},
		},
	},
	{
		"SELECT pk,i2,f FROM one_pk LEFT JOIN niltable ON pk=i2 WHERE pk > 1 ORDER BY 1",
		[]sql.Row{
			{2, int64(2), nil},
			{3, nil, nil},
		},
	},
	{
		"SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i2 WHERE pk > 0 ORDER BY 2,3",
		[]sql.Row{
			{2, int64(2), nil},
		},
	},
	{
		"SELECT GREATEST(CAST(i AS CHAR), CAST(b AS CHAR)) FROM niltable order by i",
		[]sql.Row{
			{nil},
			{"2"},
			{"3"},
			{nil},
			{"5"},
			{"6"},
		},
	},
	{
		"SELECT pk,pk1,pk2,one_pk.c1 AS foo, two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0, 0, 0},
			{1, 0, 1, 10, 10},
			{2, 1, 0, 20, 20},
			{3, 1, 1, 30, 30},
		},
	},
	{
		"SELECT pk,pk1,pk2,one_pk.c1 AS foo,two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE one_pk.c1=10",
		[]sql.Row{
			{1, 0, 1, 10, 10},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk1-pk>0 AND pk2<1",
		[]sql.Row{
			{0, 1, 0},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{0, 1, 0},
			{0, 1, 1},
			{1, 0, 0},
			{1, 0, 1},
			{1, 1, 0},
			{1, 1, 1},
			{2, 0, 0},
			{2, 0, 1},
			{2, 1, 0},
			{2, 1, 1},
			{3, 0, 0},
			{3, 0, 1},
			{3, 1, 0},
			{3, 1, 1},
		},
	},
	{
		"SELECT a.pk,b.pk FROM one_pk a JOIN one_pk b ON a.pk = b.pk order by a.pk",
		[]sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		"SELECT a.pk,b.pk FROM one_pk a, one_pk b WHERE a.pk = b.pk order by a.pk",
		[]sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		"SELECT one_pk.pk,b.pk FROM one_pk JOIN one_pk b ON one_pk.pk = b.pk order by one_pk.pk",
		[]sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		"SELECT one_pk.pk,b.pk FROM one_pk, one_pk b WHERE one_pk.pk = b.pk order by one_pk.pk",
		[]sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		"SELECT 2.0 + CAST(5 AS DECIMAL)",
		[]sql.Row{{float64(7)}},
	},
	{
		"SELECT (CASE WHEN i THEN i ELSE 0 END) as cases_i from mytable",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{"SELECT 1/0 FROM dual",
		[]sql.Row{{sql.Null}},
	},
	{"SELECT 0/0 FROM dual",
		[]sql.Row{{sql.Null}},
	},
	{"SELECT 1.0/0.0 FROM dual",
		[]sql.Row{{sql.Null}},
	},
	{"SELECT 0.0/0.0 FROM dual",
		[]sql.Row{{sql.Null}},
	},
	{"SELECT 1 div 0 FROM dual",
		[]sql.Row{{sql.Null}},
	},
	{"SELECT 1.0 div 0.0 FROM dual",
		[]sql.Row{{sql.Null}},
	},
	{"SELECT 0 div 0 FROM dual",
		[]sql.Row{{sql.Null}},
	},
	{"SELECT 0.0 div 0.0 FROM dual",
		[]sql.Row{{sql.Null}},
	},
	{"SELECT POW(2,3) FROM dual",
		[]sql.Row{{float64(8)}},
	},
}

// Queries that are known to be broken in the engine.
var BrokenQueries = []QueryTest{
	{
		"SELECT pk1, SUM(c1) FROM two_pk",
		[]sql.Row{{0, 60.0}},
	},
	// this doesn't parse in MySQL (can't use an alias in a where clause), panics in engine
	{
		`SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x 
						FROM one_pk opk WHERE x > 0 ORDER BY x`,
		[]sql.Row{
			{2, 1},
			{3, 2},
		},
	},
	{
		`SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS min,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS max
					FROM one_pk opk
					WHERE max > 1
					ORDER BY max;`,
		[]sql.Row{
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	// AVG gives the wrong result for the first row
	{
		`SELECT pk,
						(SELECT sum(c1) FROM two_pk WHERE c1 IN (SELECT c4 FROM two_pk WHERE c3 > opk.c5)) AS sum,
						(SELECT avg(c1) FROM two_pk WHERE pk2 IN (SELECT pk2 FROM two_pk WHERE c1 < opk.c2)) AS avg
					FROM one_pk opk ORDER BY pk`,
		[]sql.Row{
			{0, 60.0, nil},
			{1, 50.0, 10.0},
			{2, 30.0, 15.0},
			{3, nil, 15.0},
		},
	},
}

var VersionedQueries = []QueryTest{
	{
		"SELECT *  FROM myhistorytable AS OF '2019-01-01' AS foo ORDER BY i",
		[]sql.Row{
			{int64(1), "first row, 1"},
			{int64(2), "second row, 1"},
			{int64(3), "third row, 1"},
		},
	},
	{
		"SELECT *  FROM myhistorytable AS OF '2019-01-02' foo ORDER BY i",
		[]sql.Row{
			{int64(1), "first row, 2"},
			{int64(2), "second row, 2"},
			{int64(3), "third row, 2"},
		},
	},
	{
		"SELECT *  FROM myhistorytable ORDER BY i",
		[]sql.Row{
			{int64(1), "first row, 2"},
			{int64(2), "second row, 2"},
			{int64(3), "third row, 2"},
		},
	},
}

var InfoSchemaQueries = []QueryTest{
	{
		`SHOW TABLE STATUS FROM mydb`,
		[]sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"othertable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"tabletest", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"bigtable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"floattable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"fk_tbl", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"niltable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"newlinetable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
		},
	},
	{
		`SHOW TABLE STATUS LIKE '%table'`,
		[]sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"othertable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"bigtable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"floattable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"niltable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"newlinetable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
		},
	},
	{
		`SHOW TABLE STATUS WHERE Name = 'mytable'`,
		[]sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
		},
	},
	{
		`SHOW TABLE STATUS`,
		[]sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"othertable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"tabletest", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"bigtable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"fk_tbl", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"floattable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"niltable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"newlinetable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
		},
	},
	{
		"SHOW TABLES",
		[]sql.Row{
			{"bigtable"},
			{"floattable"},
			{"fk_tbl"},
			{"mytable"},
			{"myview"},
			{"newlinetable"},
			{"niltable"},
			{"othertable"},
			{"tabletest"},
		},
	},
	{
		"SHOW FULL TABLES",
		[]sql.Row{
			{"bigtable", "BASE TABLE"},
			{"fk_tbl", "BASE TABLE"},
			{"floattable", "BASE TABLE"},
			{"mytable", "BASE TABLE"},
			{"myview", "VIEW"},
			{"newlinetable", "BASE TABLE"},
			{"niltable", "BASE TABLE"},
			{"othertable", "BASE TABLE"},
			{"tabletest", "BASE TABLE"},
		},
	},
	{
		"SHOW TABLES FROM foo",
		[]sql.Row{
			{"other_table"},
		},
	},
	{
		"SHOW TABLES LIKE '%table'",
		[]sql.Row{
			{"mytable"},
			{"othertable"},
			{"bigtable"},
			{"floattable"},
			{"niltable"},
			{"newlinetable"},
		},
	},
	{
		`SHOW COLUMNS FROM mytable`,
		[]sql.Row{
			{"i", "bigint", "NO", "PRI", "", ""},
			{"s", "varchar(20)", "NO", "UNI", "", ""},
		},
	},
	{
		`DESCRIBE mytable`,
		[]sql.Row{
			{"i", "bigint", "NO", "PRI", "", ""},
			{"s", "varchar(20)", "NO", "UNI", "", ""},
		},
	},
	{
		`DESC mytable`,
		[]sql.Row{
			{"i", "bigint", "NO", "PRI", "", ""},
			{"s", "varchar(20)", "NO", "UNI", "", ""},
		},
	},
	{
		`SHOW COLUMNS FROM mytable WHERE Field = 'i'`,
		[]sql.Row{
			{"i", "bigint", "NO", "PRI", "", ""},
		},
	},
	{
		`SHOW COLUMNS FROM mytable LIKE 'i'`,
		[]sql.Row{
			{"i", "bigint", "NO", "PRI", "", ""},
		},
	},
	{
		`SHOW FULL COLUMNS FROM mytable`,
		[]sql.Row{
			{"i", "bigint", nil, "NO", "PRI", "", "", "", ""},
			{"s", "varchar(20)", "utf8_bin", "NO", "UNI", "", "", "", "column s"},
		},
	},
	{
		"SHOW TABLES WHERE `Table` = 'mytable'",
		[]sql.Row{
			{"mytable"},
		},
	},
	{
		`
		SELECT
			LOGFILE_GROUP_NAME, FILE_NAME, TOTAL_EXTENTS, INITIAL_SIZE, ENGINE, EXTRA
		FROM INFORMATION_SCHEMA.FILES
		WHERE FILE_TYPE = 'UNDO LOG'
			AND FILE_NAME IS NOT NULL
			AND LOGFILE_GROUP_NAME IS NOT NULL
		GROUP BY LOGFILE_GROUP_NAME, FILE_NAME, ENGINE, TOTAL_EXTENTS, INITIAL_SIZE
		ORDER BY LOGFILE_GROUP_NAME
		`,
		nil,
	},
	{
		`
		SELECT DISTINCT
			TABLESPACE_NAME, FILE_NAME, LOGFILE_GROUP_NAME, EXTENT_SIZE, INITIAL_SIZE, ENGINE
		FROM INFORMATION_SCHEMA.FILES
		WHERE FILE_TYPE = 'DATAFILE'
		ORDER BY TABLESPACE_NAME, LOGFILE_GROUP_NAME
		`,
		nil,
	},
	{
		`
		SELECT
			COLUMN_NAME,
			JSON_EXTRACT(HISTOGRAM, '$."number-of-buckets-specified"')
		FROM information_schema.COLUMN_STATISTICS
		WHERE SCHEMA_NAME = 'mydb'
		AND TABLE_NAME = 'mytable'
		`,
		nil,
	},
	{
		`
		SELECT TABLE_NAME FROM information_schema.TABLES
		WHERE TABLE_SCHEMA='mydb' AND (TABLE_TYPE='BASE TABLE' OR TABLE_TYPE='VIEW')
		ORDER BY 1
		`,
		[]sql.Row{
			{"bigtable"},
			{"floattable"},
			{"fk_tbl"},
			{"mytable"},
			{"myview"},
			{"newlinetable"},
			{"niltable"},
			{"othertable"},
			{"tabletest"},
		},
	},
	// TODO: these type names should be upper cased
	{
		`
		SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA='mydb' AND TABLE_NAME='mytable'
		`,
		[]sql.Row{
			{"s", "varchar(20)"},
			{"i", "bigint"},
		},
	},
	{
		`
		SELECT COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE '%table'
		GROUP BY COLUMN_NAME
		`,
		[]sql.Row{
			{"s"},
			{"i"},
			{"s2"},
			{"i2"},
			{"t"},
			{"n"},
			{"f32"},
			{"f64"},
			{"b"},
			{"f"},
		},
	},
	{
		`
		SELECT COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE '%table'
		GROUP BY 1
		`,
		[]sql.Row{
			{"s"},
			{"i"},
			{"s2"},
			{"i2"},
			{"t"},
			{"n"},
			{"f32"},
			{"f64"},
			{"b"},
			{"f"},
		},
	},
	{
		`
		SELECT COLUMN_NAME AS COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE '%table'
		GROUP BY 1
		`,
		[]sql.Row{
			{"s"},
			{"i"},
			{"s2"},
			{"i2"},
			{"t"},
			{"n"},
			{"f32"},
			{"f64"},
			{"b"},
			{"f"},
		},
	},
	{
		`
		SELECT COLUMN_NAME AS COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE '%table'
		GROUP BY 1 HAVING SUBSTRING(COLUMN_NAME, 1, 1) = "s"
		`,
		[]sql.Row{{"s"}, {"s2"}},
	},
	{
		`SHOW INDEXES FROM mytaBLE`,
		[]sql.Row{
			{"mytable", 0, "PRIMARY", 1, "i", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 0, "mytable_s", 1, "s", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 1, "mytable_i_s", 1, "i", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 1, "mytable_i_s", 2, "s", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
		},
	},
	{
		`SHOW CREATE TABLE mytaBLE`,
		[]sql.Row{
			{"mytable", "CREATE TABLE `mytable` (\n" +
				"  `i` bigint NOT NULL,\n" +
				"  `s` varchar(20) NOT NULL COMMENT 'column s',\n" +
				"  PRIMARY KEY (`i`),\n" +
				"  KEY `mytable_i_s` (`i`,`s`),\n" +
				"  UNIQUE KEY `mytable_s` (`s`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"},
		},
	},
	{
		`SHOW CREATE TABLE fk_TBL`,
		[]sql.Row{
			{"fk_tbl", "CREATE TABLE `fk_tbl` (\n" +
				"  `pk` bigint NOT NULL,\n" +
				"  `a` bigint,\n" +
				"  `b` varchar(20),\n" +
				"  PRIMARY KEY (`pk`),\n" +
				"  CONSTRAINT `fk1` FOREIGN KEY (`a`,`b`) REFERENCES `mytable` (`i`,`s`) ON DELETE CASCADE\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"},
		},
	},
}

var ExplodeQueries = []QueryTest{
	{
		`SELECT a, EXPLODE(b), c FROM t`,
		[]sql.Row{
			{int64(1), "a", "first"},
			{int64(1), "b", "first"},
			{int64(2), "c", "second"},
			{int64(2), "d", "second"},
			{int64(3), "e", "third"},
			{int64(3), "f", "third"},
		},
	},
	{
		`SELECT a, EXPLODE(b) AS x, c FROM t`,
		[]sql.Row{
			{int64(1), "a", "first"},
			{int64(1), "b", "first"},
			{int64(2), "c", "second"},
			{int64(2), "d", "second"},
			{int64(3), "e", "third"},
			{int64(3), "f", "third"},
		},
	},
	{
		`SELECT EXPLODE(SPLIT(c, "")) FROM t LIMIT 5`,
		[]sql.Row{
			{"f"},
			{"i"},
			{"r"},
			{"s"},
			{"t"},
		},
	},
	{
		`SELECT a, EXPLODE(b) AS x, c FROM t WHERE x = 'e'`,
		[]sql.Row{
			{int64(3), "e", "third"},
		},
	},
}

type QueryErrorTest struct {
	Query       string
	ExpectedErr *errors.Kind
}

var errorQueries = []QueryErrorTest{
	{
		Query:       "select foo.i from mytable as a",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "select foo.i from mytable",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "select foo.* from mytable",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "select foo.* from mytable as a",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "select x from mytable",
		ExpectedErr: sql.ErrColumnNotFound,
	},
	{
		Query:       "select mytable.x from mytable",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Query:       "select a.x from mytable as a",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Query:       "select a from notable",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "select myTable.i from mytable as mt", // alias overwrites the original table name
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "select myTable.* from mytable as mt", // alias overwrites the original table name
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT one_pk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON one_pk.pk=two_pk.pk1 ORDER BY 1,2,3", // alias overwrites the original table name
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON one_pk.pk=two_pk.pk1 AND opk.pk=tpk.pk2 ORDER BY 1,2,3", // alias overwrites the original table name
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT t.i, myview1.s FROM myview AS t ORDER BY i", // alias overwrites the original view name
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT * FROM mytable AS t, othertable as t", // duplicate alias
		ExpectedErr: sql.ErrDuplicateAliasOrTable,
	},
	{
		Query:       "SELECT * FROM mytable AS OTHERTABLE, othertable", // alias / table conflict
		ExpectedErr: sql.ErrDuplicateAliasOrTable,
	},
	{
		Query:       `SELECT * FROM mytable WHERE s REGEXP("*main.go")`,
		ExpectedErr: expression.ErrInvalidRegexp,
	},
	{
		Query:       `SELECT SUBSTRING(s, 1, 10) AS sub_s, SUBSTRING(sub_s, 2, 3) AS sub_sub_s FROM mytable`,
		ExpectedErr: sql.ErrMisusedAlias,
	},
	{
		Query:       "SELECT pk, (SELECT max(pk) FROM one_pk b WHERE b.pk <= one_pk.pk) FROM one_pk opk ORDER BY 1",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT pk, (SELECT max(pk) FROM one_pk WHERE b.pk <= one_pk.pk) FROM one_pk opk ORDER BY 1",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT pk, (SELECT max(pk) FROM one_pk WHERE b.pk <= one_pk.pk) FROM one_pk opk ORDER BY 1",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT pk, (SELECT max(pk) FROM two_pk WHERE pk <= one_pk.pk3) FROM one_pk ORDER BY 1",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Query:       "SELECT pk, (SELECT max(pk) FROM dne WHERE pk <= one_pk.pk3) FROM one_pk ORDER BY 1",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT pk, (SELECT max(pk) FROM two_pk WHERE pk <= c6) FROM one_pk ORDER BY 1",
		ExpectedErr: sql.ErrColumnNotFound,
	},
	// TODO: Bug: the having column must appear in the select list
	// {
	// 	Query:       "SELECT pk1, sum(c1) FROM two_pk GROUP BY 1 having c1 > 10;",
	// 	ExpectedErr: sql.ErrColumnNotFound,
	// },
}

// WriteQueryTest is a query test for INSERT, UPDATE, etc. statements. It has a query to run and a select query to
// validate the results.
type WriteQueryTest struct {
	WriteQuery          string
	ExpectedWriteResult []sql.Row
	SelectQuery         string
	ExpectedSelect      []sql.Row
}

// GenericErrorQueryTest is a query test that is used to assert an error occurs for some query, without specifying what
// the error was.
type GenericErrorQueryTest struct {
	Name  string
	Query string
}

var ViewTests = []QueryTest{
	{
		"SELECT * FROM myview ORDER BY i",
		[]sql.Row{
			sql.NewRow(int64(1), "first row"),
			sql.NewRow(int64(2), "second row"),
			sql.NewRow(int64(3), "third row"),
		},
	},
	{
		"SELECT myview.* FROM myview ORDER BY i",
		[]sql.Row{
			sql.NewRow(int64(1), "first row"),
			sql.NewRow(int64(2), "second row"),
			sql.NewRow(int64(3), "third row"),
		},
	},
	{
		"SELECT i FROM myview ORDER BY i",
		[]sql.Row{
			sql.NewRow(int64(1)),
			sql.NewRow(int64(2)),
			sql.NewRow(int64(3)),
		},
	},
	{
		"SELECT t.* FROM myview AS t ORDER BY i",
		[]sql.Row{
			sql.NewRow(int64(1), "first row"),
			sql.NewRow(int64(2), "second row"),
			sql.NewRow(int64(3), "third row"),
		},
	},
	{
		"SELECT t.i FROM myview AS t ORDER BY i",
		[]sql.Row{
			sql.NewRow(int64(1)),
			sql.NewRow(int64(2)),
			sql.NewRow(int64(3)),
		},
	},
	{
		"SELECT * FROM myview2",
		[]sql.Row{
			sql.NewRow(int64(1), "first row"),
		},
	},
	{
		"SELECT i FROM myview2",
		[]sql.Row{
			sql.NewRow(int64(1)),
		},
	},
	{
		"SELECT myview2.i FROM myview2",
		[]sql.Row{
			sql.NewRow(int64(1)),
		},
	},
	{
		"SELECT myview2.* FROM myview2",
		[]sql.Row{
			sql.NewRow(int64(1), "first row"),
		},
	},
	{
		"SELECT t.* FROM myview2 as t",
		[]sql.Row{
			sql.NewRow(int64(1), "first row"),
		},
	},
	{
		"SELECT t.i FROM myview2 as t",
		[]sql.Row{
			sql.NewRow(int64(1)),
		},
	},
	// info schema support
	{
		"select * from information_schema.views where table_schema = 'mydb'",
		[]sql.Row{
			sql.NewRow("def", "mydb", "myview", "SELECT * FROM mytable", "NONE", "YES", "", "DEFINER", "utf8mb4", "utf8_bin"),
			sql.NewRow("def", "mydb", "myview2", "SELECT * FROM myview WHERE i = 1", "NONE", "YES", "", "DEFINER", "utf8mb4", "utf8_bin"),
		},
	},
	{
		"select table_name from information_schema.tables where table_schema = 'mydb' and table_type = 'VIEW' order by 1",
		[]sql.Row{
			sql.NewRow("myview"),
			sql.NewRow("myview2"),
		},
	},
}

var VersionedViewTests = []QueryTest{
	{
		"SELECT * FROM myview1 ORDER BY i",
		[]sql.Row{
			sql.NewRow(int64(1), "first row, 2"),
			sql.NewRow(int64(2), "second row, 2"),
			sql.NewRow(int64(3), "third row, 2"),
		},
	},
	{
		"SELECT t.* FROM myview1 AS t ORDER BY i",
		[]sql.Row{
			sql.NewRow(int64(1), "first row, 2"),
			sql.NewRow(int64(2), "second row, 2"),
			sql.NewRow(int64(3), "third row, 2"),
		},
	},
	{
		"SELECT t.i FROM myview1 AS t ORDER BY i",
		[]sql.Row{
			sql.NewRow(int64(1)),
			sql.NewRow(int64(2)),
			sql.NewRow(int64(3)),
		},
	},
	{
		"SELECT * FROM myview1 AS OF '2019-01-01' ORDER BY i",
		[]sql.Row{
			sql.NewRow(int64(1), "first row, 1"),
			sql.NewRow(int64(2), "second row, 1"),
			sql.NewRow(int64(3), "third row, 1"),
		},
	},
	{
		"SELECT * FROM myview2",
		[]sql.Row{
			sql.NewRow(int64(1), "first row, 2"),
		},
	},
	{
		"SELECT i FROM myview2",
		[]sql.Row{
			sql.NewRow(int64(1)),
		},
	},
	{
		"SELECT myview2.i FROM myview2",
		[]sql.Row{
			sql.NewRow(int64(1)),
		},
	},
	{
		"SELECT myview2.* FROM myview2",
		[]sql.Row{
			sql.NewRow(int64(1), "first row, 2"),
		},
	},
	{
		"SELECT t.* FROM myview2 as t",
		[]sql.Row{
			sql.NewRow(int64(1), "first row, 2"),
		},
	},
	{
		"SELECT t.i FROM myview2 as t",
		[]sql.Row{
			sql.NewRow(int64(1)),
		},
	},
	{
		"SELECT * FROM myview2 AS OF '2019-01-01'",
		[]sql.Row{
			sql.NewRow(int64(1), "first row, 1"),
		},
	},
	// info schema support
	{
		"select * from information_schema.views where table_schema = 'mydb'",
		[]sql.Row{
			sql.NewRow("def", "mydb", "myview", "SELECT * FROM mytable", "NONE", "YES", "", "DEFINER", "utf8mb4", "utf8_bin"),
			sql.NewRow("def", "mydb", "myview1", "SELECT * FROM myhistorytable", "NONE", "YES", "", "DEFINER", "utf8mb4", "utf8_bin"),
			sql.NewRow("def", "mydb", "myview2", "SELECT * FROM myview1 WHERE i = 1", "NONE", "YES", "", "DEFINER", "utf8mb4", "utf8_bin"),
		},
	},
	{
		"select table_name from information_schema.tables where table_schema = 'mydb' and table_type = 'VIEW' order by 1",
		[]sql.Row{
			sql.NewRow("myview"),
			sql.NewRow("myview1"),
			sql.NewRow("myview2"),
		},
	},
}
