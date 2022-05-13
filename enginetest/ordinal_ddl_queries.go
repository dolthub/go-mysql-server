package enginetest

import (
	"github.com/dolthub/go-mysql-server/sql"
)

var OrdinalDDLQueries = []QueryTest{
	{
		Query: "show keys from short_ord_pk",
		Expected: []sql.Row{
			{"short_ord_pk", 0, "PRIMARY", 1, "y", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"short_ord_pk", 0, "PRIMARY", 2, "x", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
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
			{"long_ord_pk1", 0, "PRIMARY", 1, "y", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk1", 0, "PRIMARY", 2, "v", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
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
			{"long_ord_pk2", 0, "PRIMARY", 1, "y", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk2", 0, "PRIMARY", 2, "v", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk2", 0, "PRIMARY", 3, "x", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk2", 0, "PRIMARY", 4, "z", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk2", 0, "PRIMARY", 5, "u", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
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
			{"long_ord_pk3", 0, "PRIMARY", 1, "y", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk3", 0, "PRIMARY", 2, "v", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk3", 0, "PRIMARY", 3, "x", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk3", 0, "PRIMARY", 4, "z", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk3", 0, "PRIMARY", 5, "u", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
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
			{"ord_kl", 0, "PRIMARY", 1, "y", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"ord_kl", 0, "PRIMARY", 2, "v", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
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
		SelectQuery: "SELECT column_name, ordinal_position FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'long_ord_pk1' and column_key = 'PRI'",
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
			{"long_ord_pk1", 0, "PRIMARY", 1, "y", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk1", 0, "PRIMARY", 2, "v", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
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
			{"long_ord_pk1", 0, "PRIMARY", 1, "yy", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk1", 0, "PRIMARY", 2, "v", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
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
			{"long_ord_pk2", 0, "PRIMARY", 1, "y", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk2", 0, "PRIMARY", 2, "v", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk2", 0, "PRIMARY", 3, "x", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk2", 0, "PRIMARY", 4, "z", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk2", 0, "PRIMARY", 5, "u", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
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
			{"long_ord_pk3", 0, "PRIMARY", 1, "y", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk3", 0, "PRIMARY", 2, "v", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk3", 0, "PRIMARY", 3, "x", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk3", 0, "PRIMARY", 4, "z", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk3", 0, "PRIMARY", 5, "u", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
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
			{"long_ord_pk2", 0, "PRIMARY", 1, "y", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk2", 0, "PRIMARY", 2, "v", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk2", 0, "PRIMARY", 3, "x", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk2", 0, "PRIMARY", 4, "z", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
			{"long_ord_pk2", 0, "PRIMARY", 5, "u", "NULL", 0, "NULL", "NULL", "", "BTREE", "", "", "YES", "NULL"},
		},
	},
}
