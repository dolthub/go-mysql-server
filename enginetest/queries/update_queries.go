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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/vitess/go/mysql"
)

var UpdateTests = []WriteQueryTest{
	{
		WriteQuery:          "UPDATE mytable SET s = 'updated';",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "updated"}},
	},
	{
		WriteQuery:          "UPDATE mytable SET S = 'updated';",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "updated"}},
	},
	{
		WriteQuery:          "UPDATE mytable SET s = 'updated' WHERE i > 9999;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(0, 0)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "UPDATE mytable SET s = 'updated' WHERE i = 1;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "updated"}, {int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "UPDATE mytable SET s = 'updated' WHERE i <> 9999;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "updated"}},
	},
	{
		WriteQuery:          "UPDATE floattable SET f32 = f32 + f32, f64 = f32 * f64 WHERE i = 2;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM floattable WHERE i = 2;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(2), float32(3.0), float64(4.5)}},
	},
	{
		WriteQuery:          "UPDATE floattable SET f32 = 5, f32 = 4 WHERE i = 1;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT f32 FROM floattable WHERE i = 1;",
		ExpectedSelect:      []sql.UntypedSqlRow{{float32(4.0)}},
	},
	{
		WriteQuery:          "UPDATE mytable SET s = 'first row' WHERE i = 1;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 0)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "UPDATE niltable SET b = NULL WHERE f IS NULL;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(3, 2)}},
		SelectQuery:         "SELECT i,b FROM niltable WHERE f IS NULL;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), nil}, {int64(2), nil}, {int64(3), nil}},
	},
	{
		WriteQuery:          "UPDATE mytable SET s = 'updated' ORDER BY i ASC LIMIT 2;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(2, 2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "UPDATE mytable SET s = 'updated' ORDER BY i DESC LIMIT 2;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(2, 2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "first row"}, {int64(2), "updated"}, {int64(3), "updated"}},
	},
	{
		WriteQuery:          "UPDATE mytable SET s = 'updated' ORDER BY i LIMIT 1 OFFSET 1;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "first row"}, {int64(2), "updated"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "UPDATE mytable SET s = 'updated';",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "updated"}},
	},
	{
		WriteQuery:          "UPDATE mytable SET s = _binary 'updated' WHERE i = 3;",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "updated"}},
	},
	{
		WriteQuery:          "UPDATE typestable SET ti = '2020-03-06 00:00:00';",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM typestable;",
		ExpectedSelect: []sql.UntypedSqlRow{{
			int64(1),
			int8(2),
			int16(3),
			int32(4),
			int64(5),
			uint8(6),
			uint16(7),
			uint32(8),
			uint64(9),
			float32(10),
			float64(11),
			sql.MustConvert(types.Timestamp.Convert("2020-03-06 00:00:00")),
			sql.MustConvert(types.Date.Convert("2019-12-31")),
			"fourteen",
			0,
			nil,
			nil,
			"", ""}},
	},
	{
		WriteQuery:          "UPDATE typestable SET ti = '2020-03-06 00:00:00', da = '2020-03-06';",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM typestable;",
		ExpectedSelect: []sql.UntypedSqlRow{{
			int64(1),
			int8(2),
			int16(3),
			int32(4),
			int64(5),
			uint8(6),
			uint16(7),
			uint32(8),
			uint64(9),
			float32(10),
			float64(11),
			sql.MustConvert(types.Timestamp.Convert("2020-03-06 00:00:00")),
			sql.MustConvert(types.Date.Convert("2020-03-06")),
			"fourteen",
			0,
			nil,
			nil,
			"", ""}},
	},
	{
		SkipServerEngine:    true, // datetime returned is non-zero over the wire
		WriteQuery:          "UPDATE typestable SET da = '0000-00-00', ti = '0000-00-00 00:00:00';",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM typestable;",
		ExpectedSelect: []sql.UntypedSqlRow{{
			int64(1),
			int8(2),
			int16(3),
			int32(4),
			int64(5),
			uint8(6),
			uint16(7),
			uint32(8),
			uint64(9),
			float32(10),
			float64(11),
			types.Timestamp.Zero(),
			types.Date.Zero(),
			"fourteen",
			0,
			nil,
			nil,
			"", ""}},
	},
	{
		WriteQuery:          `UPDATE one_pk INNER JOIN two_pk on one_pk.pk = two_pk.pk1 SET two_pk.c1 = two_pk.c1 + 1`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(4, 4)}},
		SelectQuery:         "SELECT * FROM two_pk;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{0, 0, 1, 1, 2, 3, 4},
			{0, 1, 11, 11, 12, 13, 14},
			{1, 0, 21, 21, 22, 23, 24},
			{1, 1, 31, 31, 32, 33, 34},
		},
	},
	{
		WriteQuery:          "UPDATE mytable INNER JOIN one_pk ON mytable.i = one_pk.c5 SET mytable.i = mytable.i * 10",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(0, 0)}},
		SelectQuery:         "SELECT * FROM mytable",
		ExpectedSelect: []sql.UntypedSqlRow{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
	},
	{
		WriteQuery:          `UPDATE one_pk INNER JOIN two_pk on one_pk.pk = two_pk.pk1 SET two_pk.c1 = two_pk.c1 + 1 WHERE one_pk.c5 < 10`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(2, 2)}},
		SelectQuery:         "SELECT * FROM two_pk;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{0, 0, 1, 1, 2, 3, 4},
			{0, 1, 11, 11, 12, 13, 14},
			{1, 0, 20, 21, 22, 23, 24},
			{1, 1, 30, 31, 32, 33, 34},
		},
	},
	{
		WriteQuery:          `UPDATE one_pk INNER JOIN two_pk on one_pk.pk = two_pk.pk1 INNER JOIN othertable on othertable.i2 = two_pk.pk2 SET one_pk.c1 = one_pk.c1 + 1`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(2, 2)}},
		SelectQuery:         "SELECT * FROM one_pk;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{0, 1, 1, 2, 3, 4},
			{1, 11, 11, 12, 13, 14},
			{2, 20, 21, 22, 23, 24},
			{3, 30, 31, 32, 33, 34},
		},
	},
	{
		WriteQuery:          `UPDATE one_pk INNER JOIN (SELECT * FROM two_pk order by pk1, pk2) as t2 on one_pk.pk = t2.pk1 SET one_pk.c1 = t2.c1 + 1 where one_pk.pk < 1`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM one_pk where pk < 1",
		ExpectedSelect: []sql.UntypedSqlRow{
			{0, 1, 1, 2, 3, 4},
		},
	},
	{
		WriteQuery:          `UPDATE one_pk INNER JOIN two_pk on one_pk.pk = two_pk.pk1 SET one_pk.c1 = one_pk.c1 + 1`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(2, 2)}},
		SelectQuery:         "SELECT * FROM one_pk;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{0, 1, 1, 2, 3, 4},
			{1, 11, 11, 12, 13, 14},
			{2, 20, 21, 22, 23, 24},
			{3, 30, 31, 32, 33, 34},
		},
	},
	{
		WriteQuery:          `UPDATE one_pk INNER JOIN two_pk on one_pk.pk = two_pk.pk1 SET one_pk.c1 = one_pk.c1 + 1, one_pk.c2 = one_pk.c2 + 1 ORDER BY one_pk.pk`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(2, 2)}},
		SelectQuery:         "SELECT * FROM one_pk;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{0, 1, 2, 2, 3, 4},
			{1, 11, 12, 12, 13, 14},
			{2, 20, 21, 22, 23, 24},
			{3, 30, 31, 32, 33, 34},
		},
	},
	{
		WriteQuery:          `UPDATE one_pk INNER JOIN two_pk on one_pk.pk = two_pk.pk1 SET one_pk.c1 = one_pk.c1 + 1, two_pk.c1 = two_pk.c2 + 1`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(6, 6)}},
		SelectQuery:         "SELECT * FROM two_pk;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{0, 0, 2, 1, 2, 3, 4},
			{0, 1, 12, 11, 12, 13, 14},
			{1, 0, 22, 21, 22, 23, 24},
			{1, 1, 32, 31, 32, 33, 34},
		},
	},
	{
		WriteQuery:          `update mytable h join mytable on h.i = mytable.i and h.s <> mytable.s set h.i = mytable.i+1;`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(0, 0)}},
		SelectQuery:         "select * from mytable",
		ExpectedSelect:      []sql.UntypedSqlRow{{1, "first row"}, {2, "second row"}, {3, "third row"}},
	},
	{
		WriteQuery:          `UPDATE othertable CROSS JOIN tabletest set othertable.i2 = othertable.i2 * 10`, // cross join
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM othertable order by i2",
		ExpectedSelect: []sql.UntypedSqlRow{
			{"third", 10},
			{"second", 20},
			{"first", 30},
		},
	},
	{
		WriteQuery:          `UPDATE tabletest cross join tabletest as t2 set tabletest.i = tabletest.i * 10`, // cross join
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM tabletest order by i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{10, "first row"},
			{20, "second row"},
			{30, "third row"},
		},
	},
	{
		WriteQuery:          `UPDATE othertable cross join tabletest set tabletest.i = tabletest.i * 10`, // cross join
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM tabletest order by i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{10, "first row"},
			{20, "second row"},
			{30, "third row"},
		},
	},
	{
		WriteQuery:          `UPDATE one_pk INNER JOIN two_pk on one_pk.pk = two_pk.pk1 INNER JOIN two_pk a1 on one_pk.pk = two_pk.pk2 SET two_pk.c1 = two_pk.c1 + 1`, // cross join
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(2, 2)}},
		SelectQuery:         "SELECT * FROM two_pk order by pk1 ASC, pk2 ASC;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{0, 0, 1, 1, 2, 3, 4},
			{0, 1, 10, 11, 12, 13, 14},
			{1, 0, 20, 21, 22, 23, 24},
			{1, 1, 31, 31, 32, 33, 34},
		},
	},
	{
		WriteQuery:          `UPDATE othertable INNER JOIN tabletest on othertable.i2=3 and tabletest.i=3 SET othertable.s2 = 'fourth'`, // cross join
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM othertable order by i2",
		ExpectedSelect: []sql.UntypedSqlRow{
			{"third", 1},
			{"second", 2},
			{"fourth", 3},
		},
	},
	{
		WriteQuery:          `UPDATE tabletest cross join tabletest as t2 set t2.i = t2.i * 10`, // cross join
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM tabletest order by i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{10, "first row"},
			{20, "second row"},
			{30, "third row"},
		},
	},
	{
		WriteQuery:          `UPDATE othertable LEFT JOIN tabletest on othertable.i2=3 and tabletest.i=3 SET othertable.s2 = 'fourth'`, // left join
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM othertable order by i2",
		ExpectedSelect: []sql.UntypedSqlRow{
			{"fourth", 1},
			{"fourth", 2},
			{"fourth", 3},
		},
	},
	{
		WriteQuery:          `UPDATE othertable LEFT JOIN tabletest on othertable.i2=3 and tabletest.i=3 SET tabletest.s = 'fourth row', tabletest.i = tabletest.i + 1`, // left join
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM tabletest order by i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, "first row"},
			{2, "second row"},
			{4, "fourth row"},
		},
	},
	{
		WriteQuery:          `UPDATE othertable LEFT JOIN tabletest t3 on othertable.i2=3 and t3.i=3 SET t3.s = 'fourth row', t3.i = t3.i + 1`, // left join
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM tabletest order by i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, "first row"},
			{2, "second row"},
			{4, "fourth row"},
		},
	},
	{
		WriteQuery:          `UPDATE othertable LEFT JOIN tabletest on othertable.i2=3 and tabletest.i=3 LEFT JOIN one_pk on othertable.i2 = one_pk.pk SET one_pk.c1 = one_pk.c1 + 1`, // left join
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM one_pk order by pk",
		ExpectedSelect: []sql.UntypedSqlRow{
			{0, 0, 1, 2, 3, 4},
			{1, 11, 11, 12, 13, 14},
			{2, 21, 21, 22, 23, 24},
			{3, 31, 31, 32, 33, 34},
		},
	},
	{
		WriteQuery:          `UPDATE othertable LEFT JOIN tabletest on othertable.i2=3 and tabletest.i=3 LEFT JOIN one_pk on othertable.i2 = one_pk.pk SET one_pk.c1 = one_pk.c1 + 1 where one_pk.pk > 4`, // left join
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(0, 0)}},
		SelectQuery:         "SELECT * FROM one_pk order by pk",
		ExpectedSelect: []sql.UntypedSqlRow{
			{0, 0, 1, 2, 3, 4},
			{1, 10, 11, 12, 13, 14},
			{2, 20, 21, 22, 23, 24},
			{3, 30, 31, 32, 33, 34},
		},
	},
	{
		WriteQuery:          `UPDATE othertable LEFT JOIN tabletest on othertable.i2=3 and tabletest.i=3 LEFT JOIN one_pk on othertable.i2 = 1 and one_pk.pk = 1 SET one_pk.c1 = one_pk.c1 + 1`, // left join
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM one_pk order by pk",
		ExpectedSelect: []sql.UntypedSqlRow{
			{0, 0, 1, 2, 3, 4},
			{1, 11, 11, 12, 13, 14},
			{2, 20, 21, 22, 23, 24},
			{3, 30, 31, 32, 33, 34},
		},
	},
	{
		WriteQuery:          `UPDATE othertable RIGHT JOIN tabletest on othertable.i2=3 and tabletest.i=3 SET othertable.s2 = 'fourth'`, // right join
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM othertable order by i2",
		ExpectedSelect: []sql.UntypedSqlRow{
			{"third", 1},
			{"second", 2},
			{"fourth", 3},
		},
	},
	{
		WriteQuery:          `UPDATE othertable RIGHT JOIN tabletest on othertable.i2=3 and tabletest.i=3 SET othertable.i2 = othertable.i2 + 1`, // right join
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM othertable order by i2",
		ExpectedSelect: []sql.UntypedSqlRow{
			{"third", 1},
			{"second", 2},
			{"first", 4},
		},
	},
	{
		WriteQuery:          `UPDATE othertable LEFT JOIN tabletest on othertable.i2=tabletest.i RIGHT JOIN one_pk on othertable.i2 = 1 and one_pk.pk = 1 SET tabletest.s = 'updated';`, // right join
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM tabletest order by i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, "updated"},
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		WriteQuery:          `UPDATE IGNORE one_pk INNER JOIN two_pk on one_pk.pk = two_pk.pk1 SET two_pk.c1 = two_pk.c1 + 1`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(4, 4)}},
		SelectQuery:         "SELECT * FROM two_pk;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{0, 0, 1, 1, 2, 3, 4},
			{0, 1, 11, 11, 12, 13, 14},
			{1, 0, 21, 21, 22, 23, 24},
			{1, 1, 31, 31, 32, 33, 34},
		},
	},
	{
		WriteQuery:          `UPDATE IGNORE one_pk JOIN one_pk one_pk2 on one_pk.pk = one_pk2.pk SET one_pk.pk = 10`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 4, Updated: 1, Warnings: 0}}}},
		SelectQuery:         "SELECT * FROM one_pk;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, 10, 11, 12, 13, 14},
			{2, 20, 21, 22, 23, 24},
			{3, 30, 31, 32, 33, 34},
			{10, 0, 1, 2, 3, 4},
		},
	},
	{
		WriteQuery:          "with t (n) as (select (1) from dual) UPDATE mytable set s = concat('updated ', i) where i in (select n from t)",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "select * from mytable order by i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, "updated 1"},
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		WriteQuery:          "with recursive t (n) as (select (1) from dual union all select n + 1 from t where n < 2) UPDATE mytable set s = concat('updated ', i) where i in (select n from t)",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(2, 2)}},
		SelectQuery:         "select * from mytable order by i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, "updated 1"},
			{2, "updated 2"},
			{3, "third row"},
		},
	},
}

var SpatialUpdateTests = []WriteQueryTest{
	{
		WriteQuery:          "UPDATE point_table SET p = point(123.456,789);",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM point_table;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(5), types.Point{X: 123.456, Y: 789}}},
	},
	{
		WriteQuery:          "UPDATE line_table SET l = linestring(point(1.2,3.4),point(5.6,7.8));",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(2, 2)}},
		SelectQuery:         "SELECT * FROM line_table;",
		ExpectedSelect:      []sql.UntypedSqlRow{{int64(0), types.LineString{Points: []types.Point{{X: 1.2, Y: 3.4}, {X: 5.6, Y: 7.8}}}}, {int64(1), types.LineString{Points: []types.Point{{X: 1.2, Y: 3.4}, {X: 5.6, Y: 7.8}}}}},
	},
	{
		WriteQuery:          "UPDATE polygon_table SET p = polygon(linestring(point(1,1),point(1,-1),point(-1,-1),point(-1,1),point(1,1)));",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(2, 2)}},
		SelectQuery:         "SELECT * FROM polygon_table;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{int64(0), types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 1}, {X: 1, Y: -1}, {X: -1, Y: -1}, {X: -1, Y: 1}, {X: 1, Y: 1}}}}}},
			{int64(1), types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 1}, {X: 1, Y: -1}, {X: -1, Y: -1}, {X: -1, Y: 1}, {X: 1, Y: 1}}}}}},
		},
	},
}

// These tests return the correct select query answer but the wrong write result.
var SkippedUpdateTests = []WriteQueryTest{
	{
		WriteQuery:          `UPDATE one_pk INNER JOIN two_pk on one_pk.pk = two_pk.pk1 SET one_pk.c1 = one_pk.c1 + 1, two_pk.c1 = two_pk.c2 + 1`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(8, 6)}}, // TODO: Should be matched = 6
		SelectQuery:         "SELECT * FROM two_pk;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{0, 0, 2, 1, 2, 3, 4},
			{0, 1, 12, 11, 12, 13, 14},
			{1, 0, 22, 21, 22, 23, 24},
			{1, 1, 32, 31, 32, 33, 34},
		},
	},
	{
		WriteQuery:          `UPDATE othertable INNER JOIN tabletest on othertable.i2=3 and tabletest.i=3 SET othertable.s2 = 'fourth'`,
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM othertable;",
		ExpectedSelect: []sql.UntypedSqlRow{
			{"third", 1},
			{"second", 2},
			{"fourth", 3},
		},
	},
}

func newUpdateResult(matched, updated int) types.OkResult {
	return types.OkResult{
		RowsAffected: uint64(updated),
		Info:         plan.UpdateInfo{matched, updated, 0},
	}
}

var GenericUpdateErrorTests = []GenericErrorQueryTest{
	{
		Name:  "invalid table",
		Query: "UPDATE doesnotexist SET i = 0;",
	},
	{
		Name:  "missing binding",
		Query: "UPDATE mytable SET i = ?;",
	},
	{
		Name:  "wrong number of columns",
		Query: `UPDATE mytable SET i = ("one", "two");`,
	},
	{
		Name:  "type mismatch: string -> int",
		Query: `UPDATE mytable SET i = "one"`,
	},
	{
		Name:  "type mismatch: string -> float",
		Query: `UPDATE floattable SET f64 = "one"`,
	},
	{
		Name:  "type mismatch: string -> uint",
		Query: `UPDATE typestable SET f64 = "one"`,
	},
	{
		Name:  "invalid column set",
		Query: "UPDATE mytable SET z = 0;",
	},
	{
		Name:  "invalid column set value",
		Query: "UPDATE mytable SET i = z;",
	},
	{
		Name:  "invalid column where",
		Query: "UPDATE mytable SET s = 'hi' WHERE z = 1;",
	},
	{
		Name:  "invalid column order by",
		Query: "UPDATE mytable SET s = 'hi' ORDER BY z;",
	},
	{
		Name:  "negative limit",
		Query: "UPDATE mytable SET s = 'hi' LIMIT -1;",
	},
	{
		Name:  "negative offset",
		Query: "UPDATE mytable SET s = 'hi' LIMIT 1 OFFSET -1;",
	},
	{
		Name:  "set null on non-nullable",
		Query: "UPDATE mytable SET s = NULL;",
	},
	{
		Name:  "targets join",
		Query: "UPDATE mytable one, mytable two SET s = NULL;",
	},
	{
		Name:  "targets subquery alias",
		Query: "UPDATE (SELECT * FROM mytable) mytable SET s = NULL;",
	},
}

var UpdateIgnoreTests = []WriteQueryTest{
	{
		WriteQuery:          "UPDATE IGNORE mytable SET i = 2 where i = 1",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 0)}},
		SelectQuery:         "SELECT * FROM mytable order by i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		WriteQuery:          "UPDATE IGNORE mytable SET i = i+1 where i = 1",
		ExpectedWriteResult: []sql.UntypedSqlRow{{newUpdateResult(1, 0)}},
		SelectQuery:         "SELECT * FROM mytable order by i",
		ExpectedSelect: []sql.UntypedSqlRow{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
		},
	},
}

var UpdateIgnoreScripts = []ScriptTest{
	{
		Name: "UPDATE IGNORE with primary keys and indexes",
		SetUpScript: []string{
			"CREATE TABLE pkTable(pk int, val int, primary key(pk, val))",
			"CREATE TABLE idxTable(pk int primary key, val int UNIQUE)",
			"INSERT INTO pkTable VALUES (1, 1), (2, 2), (3, 3)",
			"INSERT INTO idxTable VALUES (1, 1), (2, 2), (3, 3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "UPDATE IGNORE pkTable set pk = pk + 1, val = val + 1",
				Expected:        []sql.UntypedSqlRow{{newUpdateResult(3, 1)}},
				ExpectedWarning: mysql.ERDupEntry,
			},
			{
				Query:    "SELECT * FROM pkTable order by pk",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {4, 4}},
			},
			{
				Query:           "UPDATE IGNORE idxTable set val = val + 1",
				Expected:        []sql.UntypedSqlRow{{newUpdateResult(3, 1)}},
				ExpectedWarning: mysql.ERDupEntry,
			},
			{
				Query:    "SELECT * FROM idxTable order by pk",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 4}},
			},
			{
				Query:    "UPDATE IGNORE pkTable set val = val + 1 where pk = 2",
				Expected: []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
			},
			{
				Query:    "SELECT * FROM pkTable order by pk",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 3}, {4, 4}},
			},
			{
				Query:           "UPDATE IGNORE pkTable SET pk = NULL",
				Expected:        []sql.UntypedSqlRow{{newUpdateResult(3, 3)}},
				ExpectedWarning: mysql.ERBadNullError,
			},
			{
				Query:    "SELECT * FROM pkTable order by pk",
				Expected: []sql.UntypedSqlRow{{0, 1}, {0, 3}, {0, 4}},
			},
			{
				Query:    "UPDATE IGNORE pkTable SET val = NULL",
				Expected: []sql.UntypedSqlRow{{newUpdateResult(3, 1)}},
			},
			{
				Query:    "SELECT * FROM pkTable order by pk",
				Expected: []sql.UntypedSqlRow{{0, 0}, {0, 3}, {0, 4}},
			},
			{
				Query:           "UPDATE IGNORE idxTable set pk = pk + 1, val = val + 1", // two bad updates
				Expected:        []sql.UntypedSqlRow{{newUpdateResult(3, 1)}},
				ExpectedWarning: mysql.ERDupEntry,
			},
			{
				Query:    "SELECT * FROM idxTable order by pk",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {4, 5}},
			},
		},
	},
	{
		Name: "UPDATE IGNORE with type conversions",
		SetUpScript: []string{
			"CREATE TABLE t1 (pk int primary key, v1 int, v2 int)",
			"INSERT INTO t1 VALUES (1, 1, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "UPDATE IGNORE t1 SET v1 = 'dsddads'",
				Expected:        []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
				ExpectedWarning: mysql.ERTruncatedWrongValueForField,
			},
			{
				Query:    "SELECT * FROM t1",
				Expected: []sql.UntypedSqlRow{{1, 0, 1}},
			},
			{
				Query:           "UPDATE IGNORE t1 SET pk = 'dasda', v2 = 'dsddads'",
				Expected:        []sql.UntypedSqlRow{{newUpdateResult(1, 1)}},
				ExpectedWarning: mysql.ERTruncatedWrongValueForField,
			},
			{
				Query:    "SELECT * FROM t1",
				Expected: []sql.UntypedSqlRow{{0, 0, 0}},
			},
		},
	},
	{
		Name: "UPDATE IGNORE with foreign keys",
		SetUpScript: []string{
			"CREATE TABLE colors ( id INT NOT NULL, color VARCHAR(32) NOT NULL, PRIMARY KEY (id), INDEX color_index(color));",
			"CREATE TABLE objects (id INT NOT NULL, name VARCHAR(64) NOT NULL,color VARCHAR(32), PRIMARY KEY(id),FOREIGN KEY (color) REFERENCES colors(color));",
			"INSERT INTO colors (id,color) VALUES (1,'red'),(2,'green'),(3,'blue'),(4,'purple');",
			"INSERT INTO objects (id,name,color) VALUES (1,'truck','red'),(2,'ball','green'),(3,'shoe','blue');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "UPDATE IGNORE objects SET color = 'orange' where id = 2",
				Expected:        []sql.UntypedSqlRow{{newUpdateResult(1, 0)}},
				ExpectedWarning: mysql.ErNoReferencedRow2,
			},
			{
				Query:    "SELECT * FROM objects ORDER BY id",
				Expected: []sql.UntypedSqlRow{{1, "truck", "red"}, {2, "ball", "green"}, {3, "shoe", "blue"}},
			},
		},
	},
	{
		Name: "UPDATE IGNORE with check constraints",
		SetUpScript: []string{
			"CREATE TABLE checksTable(pk int primary key)",
			"ALTER TABLE checksTable ADD CONSTRAINT mycx CHECK (pk < 5)",
			"INSERT INTO checksTable VALUES (1),(2),(3),(4)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "UPDATE IGNORE checksTable SET pk = pk + 1 where pk = 4",
				Expected:        []sql.UntypedSqlRow{{newUpdateResult(1, 0)}},
				ExpectedWarning: mysql.ERUnknownError,
			},
			{
				Query:    "SELECT * from checksTable ORDER BY pk",
				Expected: []sql.UntypedSqlRow{{1}, {2}, {3}, {4}},
			},
		},
	},
}

var UpdateErrorTests = []QueryErrorTest{
	{
		Query:       `UPDATE keyless INNER JOIN one_pk on keyless.c0 = one_pk.pk SET keyless.c0 = keyless.c0 + 1`,
		ExpectedErr: sql.ErrUnsupportedFeature,
	},
	{
		Query:       `UPDATE people set height_inches = null where height_inches < 100`,
		ExpectedErr: sql.ErrInsertIntoNonNullableProvidedNull,
	},
	{
		Query:       `UPDATE people SET height_inches = IF(SUM(height_inches) % 2 = 0, 42, height_inches)`,
		ExpectedErr: sql.ErrAggregationUnsupported,
	},
	{
		Query:       `UPDATE people SET height_inches = IF(SUM(*) % 2 = 0, 42, height_inches)`,
		ExpectedErr: sql.ErrStarUnsupported,
	},
	{
		Query:       `UPDATE people SET height_inches = IF(ROW_NUMBER() OVER() % 2 = 0, 42, height_inches)`,
		ExpectedErr: sql.ErrWindowUnsupported,
	},
}

var UpdateErrorScripts = []ScriptTest{
	{
		Name: "try updating string that is too long",
		SetUpScript: []string{
			"create table bad (s varchar(9))",
			"insert into bad values ('good')",
		},
		Query:       "update bad set s = '1234567890'",
		ExpectedErr: types.ErrLengthBeyondLimit,
	},
}

var ZeroTime = time.Date(0000, time.January, 1, 0, 0, 0, 0, time.UTC)
var Jan1Noon = time.Date(2000, time.January, 1, 12, 0, 0, 0, time.UTC)
var Dec15_1_30 = time.Date(2023, time.December, 15, 1, 30, 0, 0, time.UTC)
var Oct2Midnight = time.Date(2020, time.October, 2, 0, 0, 0, 0, time.UTC)
var OnUpdateExprScripts = []ScriptTest{
	{
		Name: "error cases",
		SetUpScript: []string{
			"create table t (i int, ts timestamp);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table tt (i int, j int on update (5))",
				ExpectedErrStr: "syntax error at position 42 near 'update'",
			},
			{
				Query:       "create table tt (i int, j int on update current_timestamp)",
				ExpectedErr: sql.ErrInvalidOnUpdate,
			},
			{
				Query:       "create table tt (i int, d date on update current_timestamp)",
				ExpectedErr: sql.ErrInvalidOnUpdate,
			},
			{
				Query:       "create table tt (i int, ts timestamp on update now(1))",
				ExpectedErr: sql.ErrInvalidOnUpdate,
			},
			{
				Query:       "create table tt (i int, ts timestamp(6) on update now(3))",
				ExpectedErr: sql.ErrInvalidOnUpdate,
			},
			{
				Query:       "create table tt (i int, ts timestamp(3) on update now(6))",
				ExpectedErr: sql.ErrInvalidOnUpdate,
			},
			{
				Query:       "create table tt (i int, ts timestamp on update current_timestamp(1))",
				ExpectedErr: sql.ErrInvalidOnUpdate,
			},
			{
				Query:       "create table tt (i int, ts timestamp on update current_timestamp(100))",
				ExpectedErr: sql.ErrInvalidOnUpdate,
			},
			{
				Query:       "create table tt (i int, ts timestamp on update localtime(1))",
				ExpectedErr: sql.ErrInvalidOnUpdate,
			},
			{
				Query:       "create table tt (i int, ts timestamp on update localtimestamp(1))",
				ExpectedErr: sql.ErrInvalidOnUpdate,
			},
			{
				Query:          "alter table t modify column ts timestamp on update (5)",
				ExpectedErrStr: "syntax error at position 53 near 'update'",
			},
			{
				Query:       "alter table t modify column t int on update current_timestamp",
				ExpectedErr: sql.ErrInvalidOnUpdate,
			},
			{
				Query:       "alter table t modify column t date on update current_timestamp",
				ExpectedErr: sql.ErrInvalidOnUpdate,
			},
			{
				Query:          "select current_timestamp(i) from t",
				ExpectedErrStr: "syntax error at position 27 near 'i'",
			},
		},
	},
	{
		Name: "basic case",
		SetUpScript: []string{
			"create table t (i int, ts timestamp default 0 on update current_timestamp);",
			"insert into t(i) values (1), (2), (3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{1, ZeroTime},
					{2, ZeroTime},
					{3, ZeroTime},
				},
			},
			{
				Query: "update t set i = 10 where i = 1;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{2, ZeroTime},
					{3, ZeroTime},
					{10, Dec15_1_30},
				},
			},
			{
				Query: "update t set i = 100",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 3, Info: plan.UpdateInfo{Matched: 3, Updated: 3}}},
				},
			},
			{
				Query: "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{100, Dec15_1_30},
					{100, Dec15_1_30},
					{100, Dec15_1_30},
				},
			},
			{
				// updating timestamp itself blocks on update
				Query: "update t set ts = timestamp('2020-10-2')",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 3, Info: plan.UpdateInfo{Matched: 3, Updated: 3}}},
				},
			},
			{
				Query: "select * from t;",
				Expected: []sql.UntypedSqlRow{
					{100, Oct2Midnight},
					{100, Oct2Midnight},
					{100, Oct2Midnight},
				},
			},
		},
	},
	{
		Name: "precision 3",
		SetUpScript: []string{
			"create table t (i int, ts timestamp(3) default 0 on update current_timestamp(3));",
			"insert into t(i) values (1), (2), (3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp(3) DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP(3)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{1, ZeroTime},
					{2, ZeroTime},
					{3, ZeroTime},
				},
			},
			{
				Query: "update t set i = 10 where i = 1;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{2, ZeroTime},
					{3, ZeroTime},
					{10, Dec15_1_30},
				},
			},
			{
				Query: "update t set i = 100",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 3, Info: plan.UpdateInfo{Matched: 3, Updated: 3}}},
				},
			},
			{
				Query: "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{100, Dec15_1_30},
					{100, Dec15_1_30},
					{100, Dec15_1_30},
				},
			},
			{
				// updating timestamp itself blocks on update
				Query: "update t set ts = timestamp('2020-10-2')",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 3, Info: plan.UpdateInfo{Matched: 3, Updated: 3}}},
				},
			},
			{
				Query: "select * from t;",
				Expected: []sql.UntypedSqlRow{
					{100, Oct2Midnight},
					{100, Oct2Midnight},
					{100, Oct2Midnight},
				},
			},
		},
	},
	{
		Name: "precision 6",
		SetUpScript: []string{
			"create table t (i int, ts timestamp(6) default 0 on update current_timestamp(6));",
			"insert into t(i) values (1), (2), (3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp(6) DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP(6)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{1, ZeroTime},
					{2, ZeroTime},
					{3, ZeroTime},
				},
			},
			{
				Query: "update t set i = 10 where i = 1;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{2, ZeroTime},
					{3, ZeroTime},
					{10, Dec15_1_30},
				},
			},
			{
				Query: "update t set i = 100",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 3, Info: plan.UpdateInfo{Matched: 3, Updated: 3}}},
				},
			},
			{
				Query: "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{100, Dec15_1_30},
					{100, Dec15_1_30},
					{100, Dec15_1_30},
				},
			},
			{
				// updating timestamp itself blocks on update
				Query: "update t set ts = timestamp('2020-10-2')",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 3, Info: plan.UpdateInfo{Matched: 3, Updated: 3}}},
				},
			},
			{
				Query: "select * from t;",
				Expected: []sql.UntypedSqlRow{
					{100, Oct2Midnight},
					{100, Oct2Midnight},
					{100, Oct2Midnight},
				},
			},
		},
	},
	{
		Name: "default time is current time",
		SetUpScript: []string{
			"create table t (i int, ts timestamp default current_timestamp on update current_timestamp);",
			"insert into t(i) values (1), (2), (3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{1, Jan1Noon},
					{2, Jan1Noon},
					{3, Jan1Noon},
				},
			},
			{
				Query: "update t set i = 10 where i = 1;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				Query: "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{2, Jan1Noon},
					{3, Jan1Noon},
					{10, Dec15_1_30},
				},
			},
			{
				Query: "update t set i = 100",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 3, Info: plan.UpdateInfo{Matched: 3, Updated: 3}}},
				},
			},
			{
				Query: "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{100, Dec15_1_30},
					{100, Dec15_1_30},
					{100, Dec15_1_30},
				},
			},
			{
				// updating timestamp itself blocks on update
				Query: "update t set ts = timestamp('2020-10-2')",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 3, Info: plan.UpdateInfo{Matched: 3, Updated: 3}}},
				},
			},
			{
				Query: "select * from t;",
				Expected: []sql.UntypedSqlRow{
					{100, Oct2Midnight},
					{100, Oct2Midnight},
					{100, Oct2Midnight},
				},
			},
		},
	},
	{
		Name: "alter table",
		SetUpScript: []string{
			"create table t (i int, ts timestamp);",
			"insert into t(i) values (1), (2), (3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "alter table t modify column ts timestamp default 0 on update current_timestamp;",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{1, nil},
					{2, nil},
					{3, nil},
				},
			},
			{
				Query: "update t set i = 10 where i = 1;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				Query: "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{2, nil},
					{3, nil},
					{10, Dec15_1_30},
				},
			},
			{
				Query: "update t set i = 100",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 3, Info: plan.UpdateInfo{Matched: 3, Updated: 3}}},
				},
			},
			{
				Query: "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{100, Dec15_1_30},
					{100, Dec15_1_30},
					{100, Dec15_1_30},
				},
			},
			{
				Query: "update t set ts = timestamp('2020-10-2')",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 3, Info: plan.UpdateInfo{Matched: 3, Updated: 3}}},
				},
			},
			{
				Query: "select * from t;",
				Expected: []sql.UntypedSqlRow{
					{100, Oct2Midnight},
					{100, Oct2Midnight},
					{100, Oct2Midnight},
				},
			},
		},
	},
	{
		Name: "multiple columns case",
		SetUpScript: []string{
			"create table t (i int primary key, ts timestamp default 0 on update current_timestamp, dt datetime default 0 on update current_timestamp);",
			"insert into t(i) values (1), (2), (3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int NOT NULL,\n" +
						"  `ts` timestamp DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP,\n" +
						"  `dt` datetime DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{1, ZeroTime, ZeroTime},
					{2, ZeroTime, ZeroTime},
					{3, ZeroTime, ZeroTime},
				},
			},
			{
				Query: "update t set i = 10 where i = 1;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{2, ZeroTime, ZeroTime},
					{3, ZeroTime, ZeroTime},
					{10, Dec15_1_30, Dec15_1_30},
				},
			},
			{
				Query: "update t set ts = timestamp('2020-10-2') where i = 2",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{2, Oct2Midnight, Dec15_1_30},
					{3, ZeroTime, ZeroTime},
					{10, Dec15_1_30, Dec15_1_30},
				},
			},
		},
	},
	{
		// before update triggers that update the timestamp column block the on update
		Name: "before update trigger",
		SetUpScript: []string{
			"create table t (i int primary key, ts timestamp default 0 on update current_timestamp, dt datetime default 0 on update current_timestamp);",
			"create trigger trig before update on t for each row set new.ts = timestamp('2020-10-2');",
			"insert into t(i) values (1), (2), (3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "update t set i = 10 where i = 1;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "select * from t order by i;",
				Expected: []sql.UntypedSqlRow{
					{2, ZeroTime, ZeroTime},
					{3, ZeroTime, ZeroTime},
					{10, Oct2Midnight, Dec15_1_30},
				},
			},
		},
	},
	{
		// update triggers that update other tables do not block on update
		Name: "after update trigger",
		SetUpScript: []string{
			"create table a (i int primary key);",
			"create table b (i int, ts timestamp default 0 on update current_timestamp, dt datetime default 0 on update current_timestamp);",
			"create trigger trig after update on a for each row update b set i = i + 1;",
			"insert into a values (0);",
			"insert into b(i) values (0);",
		},
		Assertions: []ScriptTestAssertion{
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "select * from b order by i;",
				Expected: []sql.UntypedSqlRow{
					{0, ZeroTime, ZeroTime},
				},
			},
			{
				Query: "update a set i = 10;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				Query: "select * from b order by i;",
				Expected: []sql.UntypedSqlRow{
					{1, Dec15_1_30, Dec15_1_30},
				},
			},
		},
	},
	{
		Name: "insert triggers",
		SetUpScript: []string{
			"create table t (i int primary key);",
			"create table a (i int, ts timestamp default 0 on update current_timestamp, dt datetime default 0 on update current_timestamp);",
			"create table b (i int, ts timestamp default 0 on update current_timestamp, dt datetime default 0 on update current_timestamp);",
			"create trigger trigA after insert on t for each row update a set i = i + 1;",
			"create trigger trigB before insert on t for each row update b set i = i + 1;",
			"insert into a(i) values (0);",
			"insert into b(i) values (0);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "insert into t values (1);",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "select * from a order by i;",
				Expected: []sql.UntypedSqlRow{
					{1, Dec15_1_30, Dec15_1_30},
				},
			},
			{
				Query: "select * from b order by i;",
				Expected: []sql.UntypedSqlRow{
					{1, Dec15_1_30, Dec15_1_30},
				},
			},
		},
	},
	{
		// Foreign Key Cascade Update does NOT trigger on update on child table
		Name: "foreign key tests",
		SetUpScript: []string{
			"create table parent (i int primary key);",
			"create table child (i int primary key, ts timestamp default 0 on update current_timestamp, foreign key (i) references parent(i) on update cascade);",
			"insert into parent values (1);",
			"insert into child(i) values (1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "update parent set i = 10;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "select * from child;",
				Expected: []sql.UntypedSqlRow{
					{10, ZeroTime},
				},
			},
		},
	},
	{
		Name: "stored procedure tests",
		SetUpScript: []string{
			"create table t (i int, ts timestamp default 0 on update current_timestamp);",
			"insert into t(i) values (0);",
			"create procedure p() update t set i = i + 1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				// call depends on stored procedure stmt for whether to use 'query' or 'exec' from go sql driver.
				SkipResultCheckOnServerEngine: true,
				Query:                         "call p();",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				Query: "select * from t;",
				Expected: []sql.UntypedSqlRow{
					{1, Dec15_1_30},
				},
			},
		},
	},
	{
		Name: "now() synonyms",
		SetUpScript: []string{
			"create table t (i int, ts timestamp);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create table t1 (i int, ts timestamp on update now())",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t1;",
				Expected: []sql.UntypedSqlRow{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "create table t2 (i int, ts timestamp on update now(0))",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t2;",
				Expected: []sql.UntypedSqlRow{
					{"t2", "CREATE TABLE `t2` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "create table t3 (i int, ts timestamp on update localtime)",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t3;",
				Expected: []sql.UntypedSqlRow{
					{"t3", "CREATE TABLE `t3` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "create table t4 (i int, ts timestamp on update localtime())",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t4;",
				Expected: []sql.UntypedSqlRow{
					{"t4", "CREATE TABLE `t4` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "create table t5 (i int, ts timestamp on update localtime(0))",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t5;",
				Expected: []sql.UntypedSqlRow{
					{"t5", "CREATE TABLE `t5` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "create table t6 (i int, ts timestamp on update localtimestamp)",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t6;",
				Expected: []sql.UntypedSqlRow{
					{"t6", "CREATE TABLE `t6` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "create table t7 (i int, ts timestamp on update localtimestamp())",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t7;",
				Expected: []sql.UntypedSqlRow{
					{"t7", "CREATE TABLE `t7` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "create table t8 (i int, ts timestamp on update localtimestamp(0))",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t8;",
				Expected: []sql.UntypedSqlRow{
					{"t8", "CREATE TABLE `t8` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "alter table t modify column ts timestamp on update now()",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t;",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "alter table t modify column ts timestamp on update now(0)",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t;",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "alter table t modify column ts timestamp on update localtime",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t;",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "alter table t modify column ts timestamp on update localtime()",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t;",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "alter table t modify column ts timestamp on update localtime(0)",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t;",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "alter table t modify column ts timestamp on update localtimestamp",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t;",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "alter table t modify column ts timestamp on update localtimestamp()",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t;",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "alter table t modify column ts timestamp on update localtimestamp(0)",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t;",
				Expected: []sql.UntypedSqlRow{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  `ts` timestamp ON UPDATE CURRENT_TIMESTAMP\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},
}
