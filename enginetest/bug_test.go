package enginetest

import (
	"context"
	"testing"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Bugs encountered while working with go-mysql-server

// Join push filter looks at the table name and not the alias to discover if column exist in table or not.
// This obviously does not work with alias
// Only happens when filteredTable is activated
func TestJoinPushFilterAlias(t *testing.T) {
	const (
		tableName1 = "customers"
		tableName2 = "orders"
	)
	db := memory.NewDatabase("memory")
	// activate Filteredtable
	table1 := memory.NewFilteredTable(tableName1, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: sql.Text, Nullable: false, PrimaryKey: true, Source: tableName1},
		{Name: "name", Type: sql.Text, Nullable: true, Source: tableName1},
	}))
	db.AddTable(tableName1, table1)
	ctx := sql.NewEmptyContext()
	table1.Insert(ctx, sql.NewRow("1", "1"))

	// activate Filteredtable
	table2 := memory.NewFilteredTable(tableName2, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: sql.Text, Nullable: false, PrimaryKey: true, Source: tableName2},
		{Name: "customerId", Type: sql.Text, Nullable: true, Source: tableName2},
	}))
	db.AddTable(tableName2, table2)
	table2.Insert(ctx, sql.NewRow("1", "1"))

	e := sqle.NewDefault(sql.NewDatabaseProvider(db))
	ctx = sql.NewContext(context.Background()).WithCurrentDB("memory")
	text := "select c.`id`, o.`id` from `customers` as c inner join `orders` as o " +
		"on c.`id` = o.`customerId` " +
		"where c.`id` = '1'"

	schema, rows, err := e.Query(ctx, text)
	require.NoError(t, err)

	if err == nil {
		data, err := sql.RowIterToRows(ctx, schema, rows)
		require.NoError(t, err)
		if err == nil {
			assert.Equal(t, 1, len(data), "should have 1 row")
			assert.Equal(t, []sql.Row{{"1", "1"}}, data, "should have customer id and order id as data")
		}
	}
}

/* // When joining two tables and the fist table has empty end columns. Note this only happend when columns are truly empty.
// The row builder joins from data without taking the table headers into account. Given that the select columns are chosen by position,
// this leads to the wrong column in table 2 being selected
// Note that nil values work fine, so maybe this is not a problem.
func TestEmptyColumnLastJoin(t *testing.T) {
	const (
		tableName1 = "customers"
		tableName2 = "orders"
	)
	db := memory.NewDatabase("memory")
	table1 := memory.NewTable(tableName1, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: sql.Text, Nullable: false, PrimaryKey: true, Source: tableName1},
		{Name: "name", Type: sql.Text, Nullable: true, Source: tableName1},
	}))
	db.AddTable(tableName1, table1)
	ctx := sql.NewEmptyContext()
	table1.Insert(ctx, sql.NewRow("1"))
	// If we set nil, it works well
	// table1.Insert(ctx, sql.NewRow("1",nil))

	table2 := memory.NewTable(tableName2, sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: sql.Text, Nullable: false, PrimaryKey: true, Source: tableName2},
		{Name: "customerId", Type: sql.Text, Nullable: true, Source: tableName2},
	}))
	db.AddTable(tableName2, table2)
	table2.Insert(ctx, sql.NewRow("1", "1"))

	e := sqle.NewDefault(sql.NewDatabaseProvider(db))
	ctx = sql.NewContext(context.Background()).WithCurrentDB("memory")
	text := "select c.`id`, o.`id` from `customers` as c inner join `orders` as o " +
		"on c.`id` = o.`customerId` "

	schema, rows, err := e.Query(ctx, text)
	if err != nil {
		t.Error(err)
	}
	data, err := sql.RowIterToRows(ctx, schema, rows)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 1, len(data), "should have 1 row")
	assert.Equal(t, []sql.Row{{"1", "1"}}, data, "should have customer id and order id as data")
} */
