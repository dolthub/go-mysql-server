package plan

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// AliasSubqueryString returns a string with subquery expressions simplified into
// static query strings, rather than the plan string (which is mutable through
// analysis).
func AliasSubqueryString(ctx *sql.Context, e sql.Expression) string {
	e, _, err := transform.Expr(ctx, e, func(ctx *sql.Context, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		case *Subquery:
			return NewSubquery(NewStrExpr(e.QueryString, e), e.QueryString), transform.NewTree, nil
		default:
			return e, transform.SameTree, nil
		}
	})
	if err != nil {
		panic(err)
	}

	// String literal values are quoted when their String() method is called, so to avoid that, we
	// check if we're dealing with a string literal and use it's raw value if so.
	if literal, ok := e.(*expression.Literal); ok {
		if s, ok := literal.Value().(string); ok {
			return s
		}
	}
	return e.String(ctx)
}

// StrExpr is used exclusively for overriding the .String()
// method of a subquery expression for efficiency and display purposes.
type StrExpr struct {
	original *Subquery
	s        string
}

var _ sql.Node = (*StrExpr)(nil)

func NewStrExpr(s string, orig *Subquery) *StrExpr {
	return &StrExpr{
		s:        s,
		original: orig,
	}
}

func (s *StrExpr) Schema(ctx *sql.Context) sql.Schema {
	return s.original.Query.Schema(ctx)
}

func (s *StrExpr) Children() []sql.Node {
	panic("StrExpr.Children should never be called")
}

func (s *StrExpr) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	panic("StrExpr.WithChildren should never be called")
}

func (s *StrExpr) IsReadOnly() bool {
	panic("StrExpr.IsReadOnly should never be called")
}

func (s *StrExpr) Resolved() bool {
	return s.original.Resolved()
}

func (s *StrExpr) String(ctx *sql.Context) string {
	return s.s
}

func (s *StrExpr) IsNullable(ctx *sql.Context) bool {
	panic("StrExpr.IsNullable should never be called")
}

func (s *StrExpr) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	panic("StrExpr.Eval should never be called")
}
