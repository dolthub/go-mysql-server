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

var JSONTableQueryTests = []QueryTest{
	{
		Query: "SELECT * FROM JSON_TABLE('[{\"a\":1},{\"a\":2}]',\"$[*]\" COLUMNS(x varchar(100) path \"$.a\")) as tt;",
		Expected: []sql.Row{
			{"1"},
			{"2"},
		},
	},
	{
		Query: "SELECT * FROM JSON_TABLE('[{\"a\":1, \"b\":2},{\"a\":3, \"b\":4}]',\"$[*]\" COLUMNS(x int path \"$.a\", y int path \"$.b\")) as tt;",
		Expected: []sql.Row{
			{1, 2},
			{3, 4},
		},
	},
	{
		Query: "SELECT * FROM JSON_TABLE('[{\"a\":1.123, \"b\":2.234},{\"a\":3.345, \"b\":4.456}]',\"$[*]\" COLUMNS(x float path \"$.a\", y float path \"$.b\")) as tt;",
		Expected: []sql.Row{
			{1.123, 2.234},
			{3.345, 4.456},
		},
	},
	{
		Query: "SELECT * FROM JSON_TABLE(concat('[{},','{}]'),\"$[*]\" COLUMNS(x varchar(100) path \"$.a\",y varchar(100) path \"$.b\")) as t;",
		Expected: []sql.Row{
			{nil, nil},
			{nil, nil},
		},
	},
	{
		Query: "select * from JSON_TABLE('[{\"a\":1},{\"a\":2}]', \"$[*]\" COLUMNS(x int path \"$.a\")) as t1 join JSON_TABLE('[{\"a\":1},{\"a\":2}]', \"$[*]\" COLUMNS(x int path \"$.a\")) as t2;",
		Expected: []sql.Row{
			{1, 1},
			{1, 2},
			{2, 1},
			{2, 2},
		},
	},
	{
		Query: "select * from JSON_TABLE('[{\"a\":1},{\"a\":2}]', \"$[*]\" COLUMNS(x int path \"$.a\")) as t1 join one_pk order by x;",
		Expected: []sql.Row{
			{1, 0, 0, 1, 2, 3, 4},
			{1, 1, 10, 11, 12, 13, 14},
			{1, 2, 20, 21, 22, 23, 24},
			{1, 3, 30, 31, 32, 33, 34},
			{2, 0, 0, 1, 2, 3, 4},
			{2, 1, 10, 11, 12, 13, 14},
			{2, 2, 20, 21, 22, 23, 24},
			{2, 3, 30, 31, 32, 33, 34},
		},
	},
	{
		Query: "select * from one_pk join JSON_TABLE('[{\"a\":1},{\"a\":2}]', \"$[*]\" COLUMNS(x int path \"$.a\")) as t1 order by x;",
		Expected: []sql.Row{
			{0, 0, 1, 2, 3, 4, 1},
			{1, 10, 11, 12, 13, 14, 1},
			{2, 20, 21, 22, 23, 24, 1},
			{3, 30, 31, 32, 33, 34, 1},
			{0, 0, 1, 2, 3, 4, 2},
			{1, 10, 11, 12, 13, 14, 2},
			{2, 20, 21, 22, 23, 24, 2},
			{3, 30, 31, 32, 33, 34, 2},
		},
	},
	{
		Query: "select * from JSON_TABLE('[{\"a\":1},{\"a\":2}]', \"$[*]\" COLUMNS(x int path \"$.a\")) as t1 union select * from JSON_TABLE('[{\"b\":3},{\"b\":4}]', \"$[*]\" COLUMNS(y int path \"$.b\")) as t2",
		Expected: []sql.Row{
			{1},
			{2},
			{3},
			{4},
		},
	},
	{
		Query: "select * from one_pk where pk in (select x from JSON_TABLE('[{\"a\":1},{\"a\":2}]', \"$[*]\" COLUMNS(x int path \"$.a\")) as t)",
		Expected: []sql.Row{
			{1, 10, 11, 12, 13, 14},
			{2, 20, 21, 22, 23, 24},
		},
	},
	{
		Query: "select * from JSON_TABLE('[{\"a\":1},{\"a\":2}]', \"$[*]\" COLUMNS(x int path \"$.a\")) t1 where x in (select y from JSON_TABLE('[{\"b\":1},{\"b\":100}]', \"$[*]\" COLUMNS(y int path \"$.b\")) as t2)",
		Expected: []sql.Row{
			{1},
		},
	},
}
