package function

import (
	"github.com/src-d/go-mysql-server/sql"
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

// WithChildren implements the Expression interface.
func (d *Database) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 0)
	}
	return NewDatabase(d.catalog)(), nil
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
