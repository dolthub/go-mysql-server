package analyzer

import (
	"reflect"
	"strings"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

// EnsureReadOnlyRule is a rule ensuring only read queries can be performed.
const EnsureReadOnlyRule = "ensure_read_only"

// ErrQueryNotAllowed is returned when the engine is in read-only mode and
// there is a query with that performs any write/update operation.
var ErrQueryNotAllowed = errors.NewKind("query of type %q not allowed in read-only mode")

// EnsureReadOnly ensures only read queries can be performed, and returns an
// error if it's not the case.
func EnsureReadOnly(ctx *sql.Context, a *Analyzer, node sql.Node) (sql.Node, error) {
	switch node.(type) {
	case *plan.InsertInto, *plan.DropIndex, *plan.CreateIndex:
		typ := strings.Split(reflect.TypeOf(node).String(), ".")[1]
		return nil, ErrQueryNotAllowed.New(typ)
	default:
		return node, nil
	}
}
