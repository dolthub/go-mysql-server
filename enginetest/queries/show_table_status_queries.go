// Copyright 2025 Dolthub, Inc.
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

import "github.com/dolthub/go-mysql-server/sql"

var ShowTableStatusQueries = []QueryTest{
	{
		Query: `SHOW TABLE STATUS FROM mydb`,
		Expected: []sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, ""},
			{"othertable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, ""},
		},
	},
	{
		Query: `SHOW TABLE STATUS LIKE '%table'`,
		Expected: []sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, ""},
			{"othertable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, ""},
		},
	},
	{
		Query: `SHOW TABLE STATUS FROM mydb LIKE 'othertable'`,
		Expected: []sql.Row{
			{"othertable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, ""},
		},
	},
	{
		Query: `SHOW TABLE STATUS WHERE Name = 'mytable'`,
		Expected: []sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, ""},
		},
	},
	{
		Query: `SHOW TABLE STATUS`,
		Expected: []sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, ""},
			{"othertable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, ""},
		},
	},
	{
		Query: `SHOW TABLE STATUS FROM mydb LIKE 'othertable'`,
		Expected: []sql.Row{
			{"othertable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, ""},
		},
	},
}

var ShowTableStatusScripts = []ScriptTest{
	{
		Name: "filter table status by comment",
		SetUpScript: []string{
			"create table table_with_comment(pk int, c0 int) comment = 'table comment'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `show table status where comment = 'table comment'`,
				Expected: []sql.Row{
					{"table_with_comment", "InnoDB", "10", "Fixed", uint64(0), uint64(0), uint64(0), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_bin", nil, nil, "table comment"},
				},
			},
		},
	},
}
