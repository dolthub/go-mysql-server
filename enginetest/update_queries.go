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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
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
		WriteQuery:          "UPDATE mytable SET s = ?;",
		ExpectedWriteResult: []sql.Row{{newUpdateResult(3, 3)}},
		SelectQuery:         "SELECT * FROM mytable;",
		ExpectedSelect:      []sql.Row{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "updated"}},
		Bindings: map[string]sql.Expression{
			"v1": expression.NewLiteral("updated", sql.Text),
		},
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
			sql.MustConvert(sql.Timestamp.Convert("2020-03-06 00:00:00")),
			sql.MustConvert(sql.Date.Convert("2019-12-31")),
			"fourteen",
			0,
			nil,
			nil}},
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
			sql.MustConvert(sql.Timestamp.Convert("2020-03-06 00:00:00")),
			sql.MustConvert(sql.Date.Convert("2020-03-06")),
			"fourteen",
			0,
			nil,
			nil}},
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
			sql.Timestamp.Zero(),
			sql.Date.Zero(),
			"fourteen",
			0,
			nil,
			nil}},
	},
}

func newUpdateResult(matched, updated int) sql.OkResult {
	return sql.OkResult{
		RowsAffected: uint64(updated),
		Info:         plan.UpdateInfo{matched, updated, 0},
	}
}

var UpdateErrorTests = []GenericErrorQueryTest{
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
