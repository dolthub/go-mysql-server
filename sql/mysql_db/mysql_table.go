package mysql_db

import (
	"github.com/dolthub/go-mysql-server/sql"
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
