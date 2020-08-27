package function

import (
	"fmt"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

const mysqlVersion = "8.0.11"

// Version is a function that returns server version.
type Version string

var _ sql.FunctionExpression = (Version)("")

// NewVersion creates a new Version UDF.
func NewVersion(versionPostfix string) func(...sql.Expression) (sql.Expression, error) {
	return func(...sql.Expression) (sql.Expression, error) {
		return Version(versionPostfix), nil
	}
}

// FunctionName implements sql.FunctionExpression
func (f Version) FunctionName() string {
	return "version"
}

// Type implements the Expression interface.
func (f Version) Type() sql.Type { return sql.LongText }

// IsNullable implements the Expression interface.
func (f Version) IsNullable() bool {
	return false
}

func (f Version) String() string {
	return "VERSION()"
}

// WithChildren implements the Expression interface.
func (f Version) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 0)
	}
	return f, nil
}

// Resolved implements the Expression interface.
func (f Version) Resolved() bool {
	return true
}

// Children implements the Expression interface.
func (f Version) Children() []sql.Expression { return nil }

// Eval implements the Expression interface.
func (f Version) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if f == "" {
		return mysqlVersion, nil
	}

	return fmt.Sprintf("%s-%s", mysqlVersion, string(f)), nil
}
