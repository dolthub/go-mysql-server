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

var OrdinalDDLQueries = []QueryTest{
	{
		Query: "show keys from short_ord_pk",
		Expected: []sql.Row{
			{"short_ord_pk", 0, "PRIMARY", 1, "y", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"short_ord_pk", 0, "PRIMARY", 2, "x", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
		},
	},
	{
		Query: "SELECT column_name, ordinal_position FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'short_ord_pk'",
		Expected: []sql.Row{
			{"x", uint(1)},
			{"y", uint(2)},
		},
	},
	{
		Query: "show keys from long_ord_pk1",
		Expected: []sql.Row{
			{"long_ord_pk1", 0, "PRIMARY", 1, "y", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk1", 0, "PRIMARY", 2, "v", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
		},
	},
	{
		Query: "SELECT column_name, ordinal_position FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'long_ord_pk1' and column_key = 'PRI'",
		Expected: []sql.Row{
			{"v", uint(2)},
			{"y", uint(5)},
		},
	},
	{
		Query: "show keys from long_ord_pk2",
		Expected: []sql.Row{
			{"long_ord_pk2", 0, "PRIMARY", 1, "y", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk2", 0, "PRIMARY", 2, "v", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk2", 0, "PRIMARY", 3, "x", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk2", 0, "PRIMARY", 4, "z", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk2", 0, "PRIMARY", 5, "u", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
		},
	},
	{
		Query: "SELECT column_name, ordinal_position FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'long_ord_pk2' and column_key = 'PRI'",
		Expected: []sql.Row{
			{"u", uint(1)},
			{"v", uint(2)},
			{"x", uint(4)},
			{"y", uint(5)},
			{"z", uint(6)},
		},
	},
	{
		Query: "show keys from long_ord_pk3",
		Expected: []sql.Row{
			{"long_ord_pk3", 0, "PRIMARY", 1, "y", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk3", 0, "PRIMARY", 2, "v", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk3", 0, "PRIMARY", 3, "x", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk3", 0, "PRIMARY", 4, "z", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk3", 0, "PRIMARY", 5, "u", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
		},
	},
	{
		Query: "SELECT column_name, ordinal_position FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'long_ord_pk3' and column_key = 'PRI'",
		Expected: []sql.Row{
			{"u", uint(1)},
			{"v", uint(2)},
			{"x", uint(5)},
			{"y", uint(6)},
			{"z", uint(7)},
		},
	},
	{
		Query:    "show keys from ord_kl",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT column_name, ordinal_position FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'ord_kl' and column_key = 'PRI'",
		Expected: []sql.Row{},
	},
}

var OrdinalDDLWriteQueries = []WriteQueryTest{
	{
		WriteQuery: "ALTER TABLE long_ord_pk1 ADD COLUMN ww int AFTER v",
		ExpectedWriteResult: []sql.Row{
			{sql.OkResult{RowsAffected: 0}},
		},
		SelectQuery: "SELECT column_name, ordinal_position FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'long_ord_pk1' and column_key = 'PRI'",
		ExpectedSelect: []sql.Row{
			{"v", uint(2)},
			{"y", uint(6)},
		},
	},
	{
		WriteQuery: "ALTER TABLE long_ord_pk1 MODIFY COLUMN w int AFTER y",
		ExpectedWriteResult: []sql.Row{
			{sql.OkResult{RowsAffected: 0}},
		},
		SelectQuery: "SELECT column_name, ordinal_position FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'long_ord_pk1' and column_key = 'PRI'",
		ExpectedSelect: []sql.Row{
			{"v", uint(2)},
			{"y", uint(4)},
		},
	},
	{
		WriteQuery: "ALTER TABLE long_ord_pk1 DROP PRIMARY KEY",
		ExpectedWriteResult: []sql.Row{
			{sql.OkResult{RowsAffected: 0}},
		},
		SelectQuery:    "show keys from ord_kl",
		ExpectedSelect: []sql.Row{},
	},
	{
		WriteQuery: "ALTER TABLE ord_kl ADD PRIMARY KEY (y,v)",
		ExpectedWriteResult: []sql.Row{
			{sql.OkResult{RowsAffected: 0}},
		},
		SelectQuery: "show keys from ord_kl",
		ExpectedSelect: []sql.Row{
			{"ord_kl", 0, "PRIMARY", 1, "y", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"ord_kl", 0, "PRIMARY", 2, "v", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
		},
	},
	{
		WriteQuery: "ALTER TABLE ord_kl ADD PRIMARY KEY (y,v)",
		ExpectedWriteResult: []sql.Row{
			{sql.OkResult{RowsAffected: 0}},
		},
		SelectQuery: "SELECT column_name, ordinal_position FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'ord_kl' and column_key = 'PRI'",
		ExpectedSelect: []sql.Row{
			{"v", uint(2)},
			{"y", uint(5)},
		},
	},
	{
		WriteQuery: "ALTER TABLE long_ord_pk1 MODIFY COLUMN y int AFTER u",
		ExpectedWriteResult: []sql.Row{
			{sql.OkResult{RowsAffected: 0}},
		},
		SelectQuery: `SELECT column_name, ordinal_position FROM INFORMATION_SCHEMA.COLUMNS 
				WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'long_ord_pk1' and column_key = 'PRI' order by 2`,
		ExpectedSelect: []sql.Row{
			{"y", uint(2)},
			{"v", uint(3)},
		},
	},
	{
		WriteQuery: "ALTER TABLE long_ord_pk1 MODIFY COLUMN y int AFTER u",
		ExpectedWriteResult: []sql.Row{
			{sql.OkResult{RowsAffected: 0}},
		},
		SelectQuery: "show keys from long_ord_pk1",
		ExpectedSelect: []sql.Row{
			{"long_ord_pk1", 0, "PRIMARY", 1, "y", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk1", 0, "PRIMARY", 2, "v", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
		},
	},
	{
		WriteQuery: "ALTER TABLE long_ord_pk1 RENAME COLUMN y to yy",
		ExpectedWriteResult: []sql.Row{
			{sql.OkResult{RowsAffected: 0}},
		},
		SelectQuery: "SELECT column_name, ordinal_position FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'long_ord_pk1' and column_key = 'PRI'",
		ExpectedSelect: []sql.Row{
			{"v", uint(2)},
			{"yy", uint(5)},
		},
	},
	{
		WriteQuery: "ALTER TABLE long_ord_pk1 RENAME COLUMN y to yy",
		ExpectedWriteResult: []sql.Row{
			{sql.OkResult{RowsAffected: 0}},
		},
		SelectQuery: "show keys from long_ord_pk1",
		ExpectedSelect: []sql.Row{
			{"long_ord_pk1", 0, "PRIMARY", 1, "yy", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk1", 0, "PRIMARY", 2, "v", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
		},
	},
	{
		WriteQuery: "ALTER TABLE long_ord_pk2 ADD COLUMN ww int AFTER w",
		ExpectedWriteResult: []sql.Row{
			{sql.OkResult{RowsAffected: 0}},
		},
		SelectQuery: "SELECT column_name, ordinal_position FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'long_ord_pk2' and column_key = 'PRI'",
		ExpectedSelect: []sql.Row{
			{"u", uint(1)},
			{"v", uint(2)},
			{"x", uint(5)},
			{"y", uint(6)},
			{"z", uint(7)},
		},
	},
	{
		WriteQuery: "ALTER TABLE long_ord_pk2 ADD COLUMN ww int AFTER w",
		ExpectedWriteResult: []sql.Row{
			{sql.OkResult{RowsAffected: 0}},
		},
		SelectQuery: "show keys from long_ord_pk2",
		ExpectedSelect: []sql.Row{
			{"long_ord_pk2", 0, "PRIMARY", 1, "y", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk2", 0, "PRIMARY", 2, "v", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk2", 0, "PRIMARY", 3, "x", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk2", 0, "PRIMARY", 4, "z", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk2", 0, "PRIMARY", 5, "u", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
		},
	},
	{
		WriteQuery: "ALTER TABLE long_ord_pk3 DROP COLUMN ww",
		ExpectedWriteResult: []sql.Row{
			{sql.OkResult{RowsAffected: 0}},
		},
		SelectQuery: "SELECT column_name, ordinal_position FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'long_ord_pk3' and column_key = 'PRI'",
		ExpectedSelect: []sql.Row{
			{"u", uint(1)},
			{"v", uint(2)},
			{"x", uint(4)},
			{"y", uint(5)},
			{"z", uint(6)},
		},
	},
	{
		WriteQuery: "ALTER TABLE long_ord_pk3 DROP COLUMN ww",
		ExpectedWriteResult: []sql.Row{
			{sql.OkResult{RowsAffected: 0}},
		},
		SelectQuery: "show keys from long_ord_pk3",
		ExpectedSelect: []sql.Row{
			{"long_ord_pk3", 0, "PRIMARY", 1, "y", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk3", 0, "PRIMARY", 2, "v", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk3", 0, "PRIMARY", 3, "x", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk3", 0, "PRIMARY", 4, "z", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk3", 0, "PRIMARY", 5, "u", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
		},
	},
	{
		WriteQuery: "ALTER TABLE long_ord_pk2 MODIFY COLUMN y int AFTER u",
		ExpectedWriteResult: []sql.Row{
			{sql.OkResult{RowsAffected: 0}},
		},
		SelectQuery: "SELECT column_name, ordinal_position FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'long_ord_pk2' and column_key = 'PRI'",
		ExpectedSelect: []sql.Row{
			{"u", uint(1)},
			{"y", uint(2)},
			{"v", uint(3)},
			{"x", uint(5)},
			{"z", uint(6)},
		},
	},
	{
		WriteQuery: "ALTER TABLE long_ord_pk2 MODIFY COLUMN y int AFTER u",
		ExpectedWriteResult: []sql.Row{
			{sql.OkResult{RowsAffected: 0}},
		},
		SelectQuery: "show keys from long_ord_pk2",
		ExpectedSelect: []sql.Row{
			{"long_ord_pk2", 0, "PRIMARY", 1, "y", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk2", 0, "PRIMARY", 2, "v", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk2", 0, "PRIMARY", 3, "x", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk2", 0, "PRIMARY", 4, "z", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"long_ord_pk2", 0, "PRIMARY", 5, "u", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
		},
	},
}
