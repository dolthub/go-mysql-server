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
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

var UpdateTests = []WriteQueryTest{
	{
		"UPDATE mytable SET s = 'updated';",
		[]sql.Row{{newUpdateResult(3, 3)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "updated"}},
	},
	{
		"UPDATE mytable SET s = 'updated' WHERE i > 9999;",
		[]sql.Row{{newUpdateResult(0, 0)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		"UPDATE mytable SET s = 'updated' WHERE i = 1;",
		[]sql.Row{{newUpdateResult(1, 1)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "updated"}, {int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		"UPDATE mytable SET s = 'updated' WHERE i <> 9999;",
		[]sql.Row{{newUpdateResult(3, 3)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "updated"}},
	},
	{
		"UPDATE floattable SET f32 = f32 + f32, f64 = f32 * f64 WHERE i = 2;",
		[]sql.Row{{newUpdateResult(1, 1)}},
		"SELECT * FROM floattable WHERE i = 2;",
		[]sql.Row{{int64(2), float32(3.0), float64(4.5)}},
	},
	{
		"UPDATE floattable SET f32 = 5, f32 = 4 WHERE i = 1;",
		[]sql.Row{{newUpdateResult(1, 1)}},
		"SELECT f32 FROM floattable WHERE i = 1;",
		[]sql.Row{{float32(4.0)}},
	},
	{
		"UPDATE mytable SET s = 'first row' WHERE i = 1;",
		[]sql.Row{{newUpdateResult(1, 0)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
	},
	{
		"UPDATE niltable SET b = NULL WHERE f IS NULL;",
		[]sql.Row{{newUpdateResult(3, 2)}},
		"SELECT i,b FROM niltable WHERE f IS NULL;",
		[]sql.Row{{int64(1), nil}, {int64(2), nil}, {int64(3), nil}},
	},
	{
		"UPDATE mytable SET s = 'updated' ORDER BY i ASC LIMIT 2;",
		[]sql.Row{{newUpdateResult(2, 2)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "third row"}},
	},
	{
		"UPDATE mytable SET s = 'updated' ORDER BY i DESC LIMIT 2;",
		[]sql.Row{{newUpdateResult(2, 2)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}, {int64(2), "updated"}, {int64(3), "updated"}},
	},
	{
		"UPDATE mytable SET s = 'updated' ORDER BY i LIMIT 1 OFFSET 1;",
		[]sql.Row{{newUpdateResult(1, 1)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "first row"}, {int64(2), "updated"}, {int64(3), "third row"}},
	},
	{
		"UPDATE mytable SET s = 'updated';",
		[]sql.Row{{newUpdateResult(3, 3)}},
		"SELECT * FROM mytable;",
		[]sql.Row{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "updated"}},
	},
	{
		"UPDATE typestable SET ti = '2020-03-06 00:00:00';",
		[]sql.Row{{newUpdateResult(1, 1)}},
		"SELECT * FROM typestable;",
		[]sql.Row{{
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
			sql.Timestamp.MustConvert("2020-03-06 00:00:00"),
			sql.Date.MustConvert("2019-12-31"),
			"fourteen",
			false,
			nil,
			nil}},
	},
	{
		"UPDATE typestable SET ti = '2020-03-06 00:00:00', da = '2020-03-06';",
		[]sql.Row{{newUpdateResult(1, 1)}},
		"SELECT * FROM typestable;",
		[]sql.Row{{
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
			sql.Timestamp.MustConvert("2020-03-06 00:00:00"),
			sql.Date.MustConvert("2020-03-06"),
			"fourteen",
			false,
			nil,
			nil}},
	},
	{
		"UPDATE typestable SET da = '0000-00-00', ti = '0000-00-00 00:00:00';",
		[]sql.Row{{newUpdateResult(1, 1)}},
		"SELECT * FROM typestable;",
		[]sql.Row{{
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
			false,
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
		"invalid table",
		"UPDATE doesnotexist SET i = 0;",
	},
	{
		"invalid column set",
		"UPDATE mytable SET z = 0;",
	},
	{
		"invalid column set value",
		"UPDATE mytable SET i = z;",
	},
	{
		"invalid column where",
		"UPDATE mytable SET s = 'hi' WHERE z = 1;",
	},
	{
		"invalid column order by",
		"UPDATE mytable SET s = 'hi' ORDER BY z;",
	},
	{
		"negative limit",
		"UPDATE mytable SET s = 'hi' LIMIT -1;",
	},
	{
		"negative offset",
		"UPDATE mytable SET s = 'hi' LIMIT 1 OFFSET -1;",
	},
	{
		"set null on non-nullable",
		"UPDATE mytable SET s = NULL;",
	},
}

