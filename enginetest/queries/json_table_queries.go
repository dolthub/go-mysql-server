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
	//{
	//	Query: "SELECT * FROM JSON_TABLE('[{\"a\":1},{\"a\":2}]',\"$[*]\" COLUMNS(x varchar(100) path \"$.a\")) as tt;",
	//	Expected: []sql.Row{
	//		{"1"},
	//		{"2"},
	//	},
	//},
	//{
	//	Query: "SELECT * FROM JSON_TABLE('[{\"a\":1, \"b\":2},{\"a\":3, \"b\":4}]',\"$[*]\" COLUMNS(x int path \"$.a\", y int path \"$.b\")) as tt;",
	//	Expected: []sql.Row{
	//		{1, 2},
	//		{3, 4},
	//	},
	//},
	//{
	//	Query: "SELECT * FROM JSON_TABLE('[{\"a\":1.123, \"b\":2.234},{\"a\":3.345, \"b\":4.456}]',\"$[*]\" COLUMNS(x float path \"$.a\", y float path \"$.b\")) as tt;",
	//	Expected: []sql.Row{
	//		{1.123, 2.234},
	//		{3.345, 4.456},
	//	},
	//},
	{
		Query: "SELECT * FROM JSON_TABLE(concat('[{},','{}]'),\"$[*]\" COLUMNS(x varchar(100) path \"$.a\",y varchar(100) path \"$.b\")) as t;",
		Expected: []sql.Row{
			{nil, nil},
			{nil, nil},
		},
	},
}
