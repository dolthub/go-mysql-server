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
	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql/analyzer/analyzererrors"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

var UpdateTests = []WriteQueryTest{
	{
		WriteQuery:          "UPDATE mytable SET s = 'updated';",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "updated"}},
	},
	{
		WriteQuery:          "UPDATE mytable SET S = 'updated';",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "updated"}},
	},
	{
		WriteQuery:          "UPDATE mytable SET s = 'updated' WHERE i > 9999;",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(0, 0)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "UPDATE mytable SET s = 'updated' WHERE i = 1;",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "updated"}, {int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "UPDATE mytable SET s = 'updated' WHERE i <> 9999;",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "updated"}},
	},
	{
		WriteQuery:          "UPDATE floattable SET f32 = f32 + f32, f64 = f32 * f64 WHERE i = 2;",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM floattable WHERE i = 2;",
		ExpectedSelect:      []sql.Row{{int64(2), float32(3.0), float64(4.5)}},
	},
	{
		WriteQuery:          "UPDATE floattable SET f32 = 5, f32 = 4 WHERE i = 1;",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT f32 FROM floattable WHERE i = 1;",
		ExpectedSelect:      []sql.Row{{float32(4.0)}},
	},
	{
		WriteQuery:          "UPDATE mytable SET s = 'first row' WHERE i = 1;",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 0)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "UPDATE niltable SET b = NULL WHERE f IS NULL;",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(3, 2)}},
		SelectQuery:         "SELECT i,b FROM niltable WHERE f IS NULL;",
		ExpectedSelect:      []sql.Row{{int64(1), nil}, {int64(2), nil}, {int64(3), nil}},
	},
	{
		WriteQuery:          "UPDATE mytable SET s = 'updated' ORDER BY i ASC LIMIT 2;",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(2, 2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "UPDATE mytable SET s = 'updated' ORDER BY i DESC LIMIT 2;",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(2, 2)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}, {int64(2), "updated"}, {int64(3), "updated"}},
	},
	{
		WriteQuery:          "UPDATE mytable SET s = 'updated' ORDER BY i LIMIT 1 OFFSET 1;",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}, {int64(2), "updated"}, {int64(3), "third row"}},
	},
	{
		WriteQuery:          "UPDATE mytable SET s = 'updated';",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "updated"}},
	},
	{
		WriteQuery:          "UPDATE mytable SET s = _binary 'updated' WHERE i = 3;",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "updated"}},
	},
	{
		WriteQuery:          "UPDATE typestable SET ti = '2020-03-06 00:00:00';",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM typestable;",
		ExpectedSelect: []sql.Row{{
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
			uint64(1),
			uint64(0)}},
	},
	{
		WriteQuery:          "UPDATE typestable SET ti = '2020-03-06 00:00:00', da = '2020-03-06';",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM typestable;",
		ExpectedSelect: []sql.Row{{
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
			uint64(1),
			uint64(0)}},
	},
	{
		WriteQuery:          "UPDATE typestable SET da = '0000-00-00', ti = '0000-00-00 00:00:00';",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM typestable;",
		ExpectedSelect: []sql.Row{{
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
			uint64(1),
			uint64(0)}},
	},
	{
		WriteQuery:          `UPDATE one_pk INNER JOIN two_pk on one_pk.pk = two_pk.pk1 SET two_pk.c1 = two_pk.c1 + 1`,
		ExpectedWriteResult: []sql.Row{{newUpdateResult(4, 4)}},
		SelectQuery:         "SELECT * FROM two_pk;",
		ExpectedSelect: []sql.Row{
			sql.NewRow(0, 0, 1, 1, 2, 3, 4),
			sql.NewRow(0, 1, 11, 11, 12, 13, 14),
			sql.NewRow(1, 0, 21, 21, 22, 23, 24),
			sql.NewRow(1, 1, 31, 31, 32, 33, 34),
		},
	},
	{
		WriteQuery:          "UPDATE mytable INNER JOIN one_pk ON mytable.i = one_pk.c5 SET mytable.i = mytable.i * 10",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(0, 0)}},
		SelectQuery:         "SELECT * FROM mytable",
		ExpectedSelect: []sql.Row{
			sql.NewRow(int64(1), "first row"),
			sql.NewRow(int64(2), "second row"),
			sql.NewRow(int64(3), "third row"),
		},
	},
	{
		WriteQuery:          `UPDATE one_pk INNER JOIN two_pk on one_pk.pk = two_pk.pk1 SET two_pk.c1 = two_pk.c1 + 1 WHERE one_pk.c5 < 10`,
		ExpectedWriteResult: []sql.Row{{newUpdateResult(2, 2)}},
		SelectQuery:         "SELECT * FROM two_pk;",
		ExpectedSelect: []sql.Row{
			sql.NewRow(0, 0, 1, 1, 2, 3, 4),
			sql.NewRow(0, 1, 11, 11, 12, 13, 14),
			sql.NewRow(1, 0, 20, 21, 22, 23, 24),
			sql.NewRow(1, 1, 30, 31, 32, 33, 34),
		},
	},
	{
		WriteQuery:          `UPDATE one_pk INNER JOIN two_pk on one_pk.pk = two_pk.pk1 INNER JOIN othertable on othertable.i2 = two_pk.pk2 SET one_pk.c1 = one_pk.c1 + 1`,
		ExpectedWriteResult: []sql.Row{{newUpdateResult(2, 2)}},
		SelectQuery:         "SELECT * FROM one_pk;",
		ExpectedSelect: []sql.Row{
			sql.NewRow(0, 1, 1, 2, 3, 4),
			sql.NewRow(1, 11, 11, 12, 13, 14),
			sql.NewRow(2, 20, 21, 22, 23, 24),
			sql.NewRow(3, 30, 31, 32, 33, 34),
		},
	},
	{
		WriteQuery:          `UPDATE one_pk INNER JOIN (SELECT * FROM two_pk order by pk1, pk2) as t2 on one_pk.pk = t2.pk1 SET one_pk.c1 = t2.c1 + 1 where one_pk.pk < 1`,
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM one_pk where pk < 1",
		ExpectedSelect: []sql.Row{
			sql.NewRow(0, 1, 1, 2, 3, 4),
		},
	},
	{
		WriteQuery:          `UPDATE one_pk INNER JOIN two_pk on one_pk.pk = two_pk.pk1 SET one_pk.c1 = one_pk.c1 + 1`,
		ExpectedWriteResult: []sql.Row{{newUpdateResult(2, 2)}},
		SelectQuery:         "SELECT * FROM one_pk;",
		ExpectedSelect: []sql.Row{
			sql.NewRow(0, 1, 1, 2, 3, 4),
			sql.NewRow(1, 11, 11, 12, 13, 14),
			sql.NewRow(2, 20, 21, 22, 23, 24),
			sql.NewRow(3, 30, 31, 32, 33, 34),
		},
	},
	{
		WriteQuery:          `UPDATE one_pk INNER JOIN two_pk on one_pk.pk = two_pk.pk1 SET one_pk.c1 = one_pk.c1 + 1, one_pk.c2 = one_pk.c2 + 1 ORDER BY one_pk.pk`,
		ExpectedWriteResult: []sql.Row{{newUpdateResult(2, 2)}},
		SelectQuery:         "SELECT * FROM one_pk;",
		ExpectedSelect: []sql.Row{
			sql.NewRow(0, 1, 2, 2, 3, 4),
			sql.NewRow(1, 11, 12, 12, 13, 14),
			sql.NewRow(2, 20, 21, 22, 23, 24),
			sql.NewRow(3, 30, 31, 32, 33, 34),
		},
	},
	{
		WriteQuery:          `UPDATE one_pk INNER JOIN two_pk on one_pk.pk = two_pk.pk1 SET one_pk.c1 = one_pk.c1 + 1, two_pk.c1 = two_pk.c2 + 1`,
		ExpectedWriteResult: []sql.Row{{newUpdateResult(8, 6)}}, // TODO: Should be matched = 6
		SelectQuery:         "SELECT * FROM two_pk;",
		ExpectedSelect: []sql.Row{
			sql.NewRow(0, 0, 2, 1, 2, 3, 4),
			sql.NewRow(0, 1, 12, 11, 12, 13, 14),
			sql.NewRow(1, 0, 22, 21, 22, 23, 24),
			sql.NewRow(1, 1, 32, 31, 32, 33, 34),
		},
	},
	{
		WriteQuery:          `update mytable h join mytable on h.i = mytable.i and h.s <> mytable.s set h.i = mytable.i;`,
		ExpectedWriteResult: []sql.Row{{newUpdateResult(0, 0)}},
	},
	{
		WriteQuery:          `UPDATE othertable CROSS JOIN tabletest set othertable.i2 = othertable.i2 * 10`, // cross join
		ExpectedWriteResult: []sql.Row{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM othertable order by i2",
		ExpectedSelect: []sql.Row{
			sql.NewRow("third", 10),
			sql.NewRow("second", 20),
			sql.NewRow("first", 30),
		},
	},
	{
		WriteQuery:          `UPDATE tabletest cross join tabletest as t2 set tabletest.i = tabletest.i * 10`, // cross join
		ExpectedWriteResult: []sql.Row{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM tabletest order by i",
		ExpectedSelect: []sql.Row{
			sql.NewRow(10, "first row"),
			sql.NewRow(20, "second row"),
			sql.NewRow(30, "third row"),
		},
	},
	{
		WriteQuery:          `UPDATE othertable cross join tabletest set tabletest.i = tabletest.i * 10`, // cross join
		ExpectedWriteResult: []sql.Row{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM tabletest order by i",
		ExpectedSelect: []sql.Row{
			sql.NewRow(10, "first row"),
			sql.NewRow(20, "second row"),
			sql.NewRow(30, "third row"),
		},
	},
	{
		WriteQuery:          `UPDATE one_pk INNER JOIN two_pk on one_pk.pk = two_pk.pk1 INNER JOIN two_pk a1 on one_pk.pk = two_pk.pk2 SET two_pk.c1 = two_pk.c1 + 1`, // cross join
		ExpectedWriteResult: []sql.Row{{newUpdateResult(2, 2)}},
		SelectQuery:         "SELECT * FROM two_pk order by pk1 ASC, pk2 ASC;",
		ExpectedSelect: []sql.Row{
			sql.NewRow(0, 0, 1, 1, 2, 3, 4),
			sql.NewRow(0, 1, 10, 11, 12, 13, 14),
			sql.NewRow(1, 0, 20, 21, 22, 23, 24),
			sql.NewRow(1, 1, 31, 31, 32, 33, 34),
		},
	},
	{
		WriteQuery:          `UPDATE othertable INNER JOIN tabletest on othertable.i2=3 and tabletest.i=3 SET othertable.s2 = 'fourth'`, // cross join
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM othertable order by i2",
		ExpectedSelect: []sql.Row{
			sql.NewRow("third", 1),
			sql.NewRow("second", 2),
			sql.NewRow("fourth", 3),
		},
	},
	{
		WriteQuery:          `UPDATE tabletest cross join tabletest as t2 set t2.i = t2.i * 10`, // cross join
		ExpectedWriteResult: []sql.Row{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM tabletest order by i",
		ExpectedSelect: []sql.Row{
			sql.NewRow(10, "first row"),
			sql.NewRow(20, "second row"),
			sql.NewRow(30, "third row"),
		},
	},
	{
		WriteQuery:          `UPDATE othertable LEFT JOIN tabletest on othertable.i2=3 and tabletest.i=3 SET othertable.s2 = 'fourth'`, // left join
		ExpectedWriteResult: []sql.Row{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM othertable order by i2",
		ExpectedSelect: []sql.Row{
			sql.NewRow("fourth", 1),
			sql.NewRow("fourth", 2),
			sql.NewRow("fourth", 3),
		},
	},
	{
		WriteQuery:          `UPDATE othertable LEFT JOIN tabletest on othertable.i2=3 and tabletest.i=3 SET tabletest.s = 'fourth row', tabletest.i = tabletest.i + 1`, // left join
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM tabletest order by i",
		ExpectedSelect: []sql.Row{
			sql.NewRow(1, "first row"),
			sql.NewRow(2, "second row"),
			sql.NewRow(4, "fourth row"),
		},
	},
	{
		WriteQuery:          `UPDATE othertable LEFT JOIN tabletest t3 on othertable.i2=3 and t3.i=3 SET t3.s = 'fourth row', t3.i = t3.i + 1`, // left join
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM tabletest order by i",
		ExpectedSelect: []sql.Row{
			sql.NewRow(1, "first row"),
			sql.NewRow(2, "second row"),
			sql.NewRow(4, "fourth row"),
		},
	},
	{
		WriteQuery:          `UPDATE othertable LEFT JOIN tabletest on othertable.i2=3 and tabletest.i=3 LEFT JOIN one_pk on othertable.i2 = one_pk.pk SET one_pk.c1 = one_pk.c1 + 1`, // left join
		ExpectedWriteResult: []sql.Row{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM one_pk order by pk",
		ExpectedSelect: []sql.Row{
			sql.NewRow(0, 0, 1, 2, 3, 4),
			sql.NewRow(1, 11, 11, 12, 13, 14),
			sql.NewRow(2, 21, 21, 22, 23, 24),
			sql.NewRow(3, 31, 31, 32, 33, 34),
		},
	},
	{
		WriteQuery:          `UPDATE othertable LEFT JOIN tabletest on othertable.i2=3 and tabletest.i=3 LEFT JOIN one_pk on othertable.i2 = one_pk.pk SET one_pk.c1 = one_pk.c1 + 1 where one_pk.pk > 4`, // left join
		ExpectedWriteResult: []sql.Row{{newUpdateResult(0, 0)}},
		SelectQuery:         "SELECT * FROM one_pk order by pk",
		ExpectedSelect: []sql.Row{
			sql.NewRow(0, 0, 1, 2, 3, 4),
			sql.NewRow(1, 10, 11, 12, 13, 14),
			sql.NewRow(2, 20, 21, 22, 23, 24),
			sql.NewRow(3, 30, 31, 32, 33, 34),
		},
	},
	{
		WriteQuery:          `UPDATE othertable LEFT JOIN tabletest on othertable.i2=3 and tabletest.i=3 LEFT JOIN one_pk on othertable.i2 = 1 and one_pk.pk = 1 SET one_pk.c1 = one_pk.c1 + 1`, // left join
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM one_pk order by pk",
		ExpectedSelect: []sql.Row{
			sql.NewRow(0, 0, 1, 2, 3, 4),
			sql.NewRow(1, 11, 11, 12, 13, 14),
			sql.NewRow(2, 20, 21, 22, 23, 24),
			sql.NewRow(3, 30, 31, 32, 33, 34),
		},
	},
	{
		WriteQuery:          `UPDATE othertable RIGHT JOIN tabletest on othertable.i2=3 and tabletest.i=3 SET othertable.s2 = 'fourth'`, // right join
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM othertable order by i2",
		ExpectedSelect: []sql.Row{
			sql.NewRow("third", 1),
			sql.NewRow("second", 2),
			sql.NewRow("fourth", 3),
		},
	},
	{
		WriteQuery:          `UPDATE othertable RIGHT JOIN tabletest on othertable.i2=3 and tabletest.i=3 SET othertable.i2 = othertable.i2 + 1`, // right join
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM othertable order by i2",
		ExpectedSelect: []sql.Row{
			sql.NewRow("third", 1),
			sql.NewRow("second", 2),
			sql.NewRow("first", 4),
		},
	},
	{
		WriteQuery:          `UPDATE othertable LEFT JOIN tabletest on othertable.i2=tabletest.i RIGHT JOIN one_pk on othertable.i2 = 1 and one_pk.pk = 1 SET tabletest.s = 'updated';`, // right join
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM tabletest order by i",
		ExpectedSelect: []sql.Row{
			sql.NewRow(1, "updated"),
			sql.NewRow(2, "second row"),
			sql.NewRow(3, "third row"),
		},
	},
	{
		WriteQuery:          "with t (n) as (select (1) from dual) UPDATE mytable set s = concat('updated ', i) where i in (select n from t)",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "select * from mytable order by i",
		ExpectedSelect: []sql.Row{
			sql.NewRow(1, "updated 1"),
			sql.NewRow(2, "second row"),
			sql.NewRow(3, "third row"),
		},
	},
	{
		WriteQuery:          "with recursive t (n) as (select (1) from dual union all select n + 1 from t where n < 2) UPDATE mytable set s = concat('updated ', i) where i in (select n from t)",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(2, 2)}},
		SelectQuery:         "select * from mytable order by i",
		ExpectedSelect: []sql.Row{
			sql.NewRow(1, "updated 1"),
			sql.NewRow(2, "updated 2"),
			sql.NewRow(3, "third row"),
		},
	},
}

var SpatialUpdateTests = []WriteQueryTest{
	{
		WriteQuery:          "UPDATE point_table SET p = point(123.456,789);",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM point_table;",
		ExpectedSelect:      []sql.Row{{int64(5), types.Point{X: 123.456, Y: 789}}},
	},
	{
		WriteQuery:          "UPDATE line_table SET l = linestring(point(1.2,3.4),point(5.6,7.8));",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(2, 2)}},
		SelectQuery:         "SELECT * FROM line_table;",
		ExpectedSelect:      []sql.Row{{int64(0), types.LineString{Points: []types.Point{{X: 1.2, Y: 3.4}, {X: 5.6, Y: 7.8}}}}, {int64(1), types.LineString{Points: []types.Point{{X: 1.2, Y: 3.4}, {X: 5.6, Y: 7.8}}}}},
	},
	{
		WriteQuery:          "UPDATE polygon_table SET p = polygon(linestring(point(1,1),point(1,-1),point(-1,-1),point(-1,1),point(1,1)));",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(2, 2)}},
		SelectQuery:         "SELECT * FROM polygon_table;",
		ExpectedSelect: []sql.Row{
			{int64(0), types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 1}, {X: 1, Y: -1}, {X: -1, Y: -1}, {X: -1, Y: 1}, {X: 1, Y: 1}}}}}},
			{int64(1), types.Polygon{Lines: []types.LineString{{Points: []types.Point{{X: 1, Y: 1}, {X: 1, Y: -1}, {X: -1, Y: -1}, {X: -1, Y: 1}, {X: 1, Y: 1}}}}}},
		},
	},
}

// These tests return the correct select query answer but the wrong write result.
var SkippedUpdateTests = []WriteQueryTest{
	{
		WriteQuery:          `UPDATE one_pk INNER JOIN two_pk on one_pk.pk = two_pk.pk1 SET one_pk.c1 = one_pk.c1 + 1, two_pk.c1 = two_pk.c2 + 1`,
		ExpectedWriteResult: []sql.Row{{newUpdateResult(8, 6)}}, // TODO: Should be matched = 6
		SelectQuery:         "SELECT * FROM two_pk;",
		ExpectedSelect: []sql.Row{
			sql.NewRow(0, 0, 2, 1, 2, 3, 4),
			sql.NewRow(0, 1, 12, 11, 12, 13, 14),
			sql.NewRow(1, 0, 22, 21, 22, 23, 24),
			sql.NewRow(1, 1, 32, 31, 32, 33, 34),
		},
	},
	{
		WriteQuery:          `UPDATE othertable INNER JOIN tabletest on othertable.i2=3 and tabletest.i=3 SET othertable.s2 = 'fourth'`,
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 1)}},
		SelectQuery:         "SELECT * FROM othertable;",
		ExpectedSelect: []sql.Row{
			sql.NewRow("third", 1),
			sql.NewRow("second", 2),
			sql.NewRow("fourth", 3),
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
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 0)}},
		SelectQuery:         "SELECT * FROM mytable order by i",
		ExpectedSelect: []sql.Row{
			sql.NewRow(1, "first row"),
			sql.NewRow(2, "second row"),
			sql.NewRow(3, "third row"),
		},
	},
	{
		WriteQuery:          "UPDATE IGNORE mytable SET i = i+1 where i = 1",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(1, 0)}},
		SelectQuery:         "SELECT * FROM mytable order by i",
		ExpectedSelect: []sql.Row{
			sql.NewRow(1, "first row"),
			sql.NewRow(2, "second row"),
			sql.NewRow(3, "third row"),
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
				Expected:        []sql.Row{{newUpdateResult(3, 1)}},
				ExpectedWarning: mysql.ERDupEntry,
			},
			{
				Query:    "SELECT * FROM pkTable order by pk",
				Expected: []sql.Row{{1, 1}, {2, 2}, {4, 4}},
			},
			{
				Query:           "UPDATE IGNORE idxTable set val = val + 1",
				Expected:        []sql.Row{{newUpdateResult(3, 1)}},
				ExpectedWarning: mysql.ERDupEntry,
			},
			{
				Query:    "SELECT * FROM idxTable order by pk",
				Expected: []sql.Row{{1, 1}, {2, 2}, {3, 4}},
			},
			{
				Query:    "UPDATE IGNORE pkTable set val = val + 1 where pk = 2",
				Expected: []sql.Row{{newUpdateResult(1, 1)}},
			},
			{
				Query:    "SELECT * FROM pkTable order by pk",
				Expected: []sql.Row{{1, 1}, {2, 3}, {4, 4}},
			},
			{
				Query:           "UPDATE IGNORE pkTable SET pk = NULL",
				Expected:        []sql.Row{{newUpdateResult(3, 3)}},
				ExpectedWarning: mysql.ERBadNullError,
			},
			{
				Query:    "SELECT * FROM pkTable order by pk",
				Expected: []sql.Row{{0, 1}, {0, 3}, {0, 4}},
			},
			{
				Query:    "UPDATE IGNORE pkTable SET val = NULL",
				Expected: []sql.Row{{newUpdateResult(3, 1)}},
			},
			{
				Query:    "SELECT * FROM pkTable order by pk",
				Expected: []sql.Row{{0, 0}, {0, 3}, {0, 4}},
			},
			{
				Query:           "UPDATE IGNORE idxTable set pk = pk + 1, val = val + 1", // two bad updates
				Expected:        []sql.Row{{newUpdateResult(3, 1)}},
				ExpectedWarning: mysql.ERDupEntry,
			},
			{
				Query:    "SELECT * FROM idxTable order by pk",
				Expected: []sql.Row{{1, 1}, {2, 2}, {4, 5}},
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
				Expected:        []sql.Row{{newUpdateResult(1, 1)}},
				ExpectedWarning: mysql.ERTruncatedWrongValueForField,
			},
			{
				Query:    "SELECT * FROM t1",
				Expected: []sql.Row{{1, 0, 1}},
			},
			{
				Query:           "UPDATE IGNORE t1 SET pk = 'dasda', v2 = 'dsddads'",
				Expected:        []sql.Row{{newUpdateResult(1, 1)}},
				ExpectedWarning: mysql.ERTruncatedWrongValueForField,
			},
			{
				Query:    "SELECT * FROM t1",
				Expected: []sql.Row{{0, 0, 0}},
			},
		},
	},
	{
		Name: "UPDATE IGNORE with foreign keys",
		SetUpScript: []string{
			"CREATE TABLE colors ( id INT NOT NULL, color VARCHAR(32) NOT NULL, PRIMARY KEY (id), INDEX color_index(color));",
			"CREATE TABLE objects (id INT NOT NULL, name VARCHAR(64) NOT NULL,color VARCHAR(32), PRIMARY KEY(id),FOREIGN KEY (color) REFERENCES colors(color))",
			"INSERT INTO colors (id,color) VALUES (1,'red'),(2,'green'),(3,'blue'),(4,'purple')",
			"INSERT INTO objects (id,name,color) VALUES (1,'truck','red'),(2,'ball','green'),(3,'shoe','blue')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "UPDATE IGNORE objects SET color = 'orange' where id = 2",
				Expected:        []sql.Row{{newUpdateResult(1, 0)}},
				ExpectedWarning: mysql.ErNoReferencedRow2,
			},
			{
				Query:    "SELECT * FROM objects ORDER BY id",
				Expected: []sql.Row{{1, "truck", "red"}, {2, "ball", "green"}, {3, "shoe", "blue"}},
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
				Expected:        []sql.Row{{newUpdateResult(1, 0)}},
				ExpectedWarning: mysql.ERUnknownError,
			},
			{
				Query:    "SELECT * from checksTable ORDER BY pk",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
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
		ExpectedErr: analyzererrors.ErrAggregationUnsupported,
	},
	{
		Query:       `UPDATE people SET height_inches = IF(SUM(*) % 2 = 0, 42, height_inches)`,
		ExpectedErr: sql.ErrStarUnsupported,
	},
	{
		Query:       `UPDATE people SET height_inches = IF(ROW_NUMBER() OVER() % 2 = 0, 42, height_inches)`,
		ExpectedErr: analyzererrors.ErrWindowUnsupported,
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
