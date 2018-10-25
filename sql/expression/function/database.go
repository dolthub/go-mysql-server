package function

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Database stands for DATABASE() function
type Database struct {
	catalog *sql.Catalog
}

// NewDatabase returns a new Database function
func NewDatabase(c *sql.Catalog) func() sql.Expression {
	return func() sql.Expression {
		return &Database{c}
	}
}

// Type implements the sql.Expression (sql.Text)
func (db *Database) Type() sql.Type { return sql.Text }

// IsNullable implements the sql.Expression interface.
// The function returns always true
func (db *Database) IsNullable() bool {
	return true
}

func (*Database) String() string {
	return "DATABASE()"
}

// TransformUp implements the sql.Expression interface.
func (db *Database) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	return fn(db)
}

// Resolved implements the sql.Expression interface.
func (db *Database) Resolved() bool {
	return true
}

// Children implements the sql.Expression interface.
func (db *Database) Children() []sql.Expression { return nil }

// Eval implements the sql.Expression interface.
func (db *Database) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return db.catalog.CurrentDatabase(), nil
}
