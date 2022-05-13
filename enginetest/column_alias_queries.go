package enginetest

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"
)

var ColumnAliasQueries = []QueryTest{
	{
		Query: `SELECT i AS cOl FROM mytable`,
		ExpectedColumns: sql.Schema{
			{
				Name: "cOl",
				Type: sql.Int64,
			},
		},
		Expected: []sql.Row{
			{int64(1)},
			{int64(2)},
			{int64(3)},
		},
	},
	{
		Query: `SELECT i AS cOl, s as COL FROM mytable`,
		ExpectedColumns: sql.Schema{
			{
				Name: "cOl",
				Type: sql.Int64,
			},
			{
				Name: "COL",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
	},
	{
		// TODO: this is actually inconsistent with MySQL, which doesn't allow column aliases in the where clause
		Query: `SELECT i AS cOl, s as COL FROM mytable where cOl = 1`,
		ExpectedColumns: sql.Schema{
			{
				Name: "cOl",
				Type: sql.Int64,
			},
			{
				Name: "COL",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
		Expected: []sql.Row{
			{int64(1), "first row"},
		},
	},
	{
		Query: `SELECT s as COL1, SUM(i) COL2 FROM mytable group by s order by cOL2`,
		ExpectedColumns: sql.Schema{
			{
				Name: "COL1",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
			{
				Name: "COL2",
				Type: sql.Float64,
			},
		},
		// TODO: SUM should be integer typed for integers
		Expected: []sql.Row{
			{"first row", float64(1)},
			{"second row", float64(2)},
			{"third row", float64(3)},
		},
	},
	{
		Query: `SELECT s as COL1, SUM(i) COL2 FROM mytable group by col1 order by col2`,
		ExpectedColumns: sql.Schema{
			{
				Name: "COL1",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
			{
				Name: "COL2",
				Type: sql.Float64,
			},
		},
		Expected: []sql.Row{
			{"first row", float64(1)},
			{"second row", float64(2)},
			{"third row", float64(3)},
		},
	},
	{
		Query: `SELECT s as coL1, SUM(i) coL2 FROM mytable group by 1 order by 2`,
		ExpectedColumns: sql.Schema{
			{
				Name: "coL1",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
			{
				Name: "coL2",
				Type: sql.Float64,
			},
		},
		Expected: []sql.Row{
			{"first row", float64(1)},
			{"second row", float64(2)},
			{"third row", float64(3)},
		},
	},
	{
		Query: `SELECT s as Date, SUM(i) TimeStamp FROM mytable group by 1 order by 2`,
		ExpectedColumns: sql.Schema{
			{
				Name: "Date",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
			{
				Name: "TimeStamp",
				Type: sql.Float64,
			},
		},
		Expected: []sql.Row{
			{"first row", float64(1)},
			{"second row", float64(2)},
			{"third row", float64(3)},
		},
	},
}
