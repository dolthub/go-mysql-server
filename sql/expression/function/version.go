package function

import (
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

const mysqlVersion = "8.0.11"

// Version is a function that returns server version.
type Version string

// NewVersion creates a new Version UDF.
func NewVersion(versionPostfix string) func(...sql.Expression) (sql.Expression, error) {
	return func(...sql.Expression) (sql.Expression, error) {
		return Version(versionPostfix), nil
	}
}

// Type implements the Expression interface.
func (f Version) Type() sql.Type { return sql.Text }

// IsNullable implements the Expression interface.
func (f Version) IsNullable() bool {
	return false
}

func (f Version) String() string {
	return "VERSION()"
}

// TransformUp implements the Expression interface.
func (f Version) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	return fn(f)
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
