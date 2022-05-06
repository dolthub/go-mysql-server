package mysql_db

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/in_mem_table"
)

// How do I do polymorphism??

// MySQLTable represents a table in the MySQL db.
// TODO: interface or struct?
type MySQLTable interface {
	sql.Table
	sql.InsertableTable
	sql.UpdatableTable
	sql.DeletableTable
	sql.ReplaceableTable
	sql.TruncateableTable
}

type mysqlTableImpl struct {
	name string
	sch  sql.Schema
	data *in_mem_table.Data
}
