// Copyright 2022 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql"
)

var NullRangeTests = []QueryTest{
	{
		Query: "select * from null_ranges where y IS NULL or y < 1",
		Expected: []sql.UntypedSqlRow{
			{0, 0},
			{3, nil},
			{4, nil},
		},
	},
	{
		Query:    "select * from null_ranges where y IS NULL and y < 1",
		Expected: []sql.UntypedSqlRow{},
	},
	{
		Query: "select * from null_ranges where y IS NULL or y IS NOT NULL",
		Expected: []sql.UntypedSqlRow{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, nil},
			{4, nil},
		},
	},
	{
		Query: "select * from null_ranges where y IS NOT NULL",
		Expected: []sql.UntypedSqlRow{
			{0, 0},
			{1, 1},
			{2, 2},
		},
	},
	{
		Query: "select * from null_ranges where y IS NULL or y = 0 or y = 1",
		Expected: []sql.UntypedSqlRow{
			{0, 0},
			{1, 1},
			{3, nil},
			{4, nil},
		},
	},
	{
		Query: "select * from null_ranges where y IS NULL or y < 1 or y > 1",
		Expected: []sql.UntypedSqlRow{
			{0, 0},
			{2, 2},
			{3, nil},
			{4, nil},
		},
	},
	{
		Query: "select * from null_ranges where y IS NOT NULL and x > 1",
		Expected: []sql.UntypedSqlRow{
			{2, 2},
		},
	}, {
		Query: "select * from null_ranges where y IS NULL and x = 4",
		Expected: []sql.UntypedSqlRow{
			{4, nil},
		},
	}, {
		Query: "select * from null_ranges where y IS NULL and x > 1",
		Expected: []sql.UntypedSqlRow{
			{3, nil},
			{4, nil},
		},
	},
	{
		Query:    "select * from null_ranges where y IS NULL and y IS NOT NULL",
		Expected: []sql.UntypedSqlRow{},
	},
	{
		Query:    "select * from null_ranges where y is NULL and y > -1 and y > -2",
		Expected: []sql.UntypedSqlRow{},
	},
	{
		Query:    "select * from null_ranges where y > -1 and y < 7 and y IS NULL",
		Expected: []sql.UntypedSqlRow{},
	},
	{
		Query: "select * from null_ranges where y > -1 and y > -2 and y IS NOT NULL",
		Expected: []sql.UntypedSqlRow{
			{0, 0},
			{1, 1},
			{2, 2},
		},
	},
	{
		Query: "select * from null_ranges where y > -1 and y > 1 and y IS NOT NULL",
		Expected: []sql.UntypedSqlRow{
			{2, 2},
		},
	},
	{
		Query: "select * from null_ranges where y < 6 and y > -1 and y IS NOT NULL",
		Expected: []sql.UntypedSqlRow{
			{0, 0},
			{1, 1},
			{2, 2},
		},
	},
}
