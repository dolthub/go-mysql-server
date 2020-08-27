package function

import (
	"github.com/liquidata-inc/go-mysql-server/sql"
)

// Database stands for DATABASE() function
type Database struct {
	catalog *sql.Catalog
}

var _ sql.FunctionExpression = (*Database)(nil)

// NewDatabase returns a new Database function
func NewDatabase(c *sql.Catalog) func() sql.Expression {
	return func() sql.Expression {
		return &Database{c}
	}
}

// FunctionName implements sql.FunctionExpression
func (db *Database) FunctionName() string {
	return "database"
}

// Type implements the sql.Expression (sql.LongText)
func (db *Database) Type() sql.Type { return sql.LongText }

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
	return ctx.GetCurrentDatabase(), nil
}
