package plan

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// AliasSubqueryString returns a string with subquery expressions simplified into
// static query strings, rather than the plan string (which is mutable through
// analysis).
func AliasSubqueryString(e sql.Expression) string {
	e, _, err := transform.Expr(e, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		case *Subquery:
			return NewSubquery(NewStrExpr(e.QueryString), e.QueryString), transform.NewTree, nil
		default:
			return e, transform.SameTree, nil
		}
	})
	if err != nil {
		panic(err)
	}
	return e.String()
}

// StrExpr is used exclusively for overriding the .String()
// method of a node.
type StrExpr struct {
	s string
}

var _ sql.Node = (*StrExpr)(nil)

func (s *StrExpr) Schema() sql.Schema {
	//TODO implement me
	panic("implement me")
}

func (s *StrExpr) Children() []sql.Node {
	//TODO implement me
	panic("implement me")
}

func (s *StrExpr) WithChildren(children ...sql.Node) (sql.Node, error) {
	//TODO implement me
	panic("implement me")
}

func (s *StrExpr) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	//TODO implement me
	panic("implement me")
}

func (s *StrExpr) IsReadOnly() bool {
	//TODO implement me
	panic("implement me")
}

func NewStrExpr(s string) *StrExpr {
	return &StrExpr{s: s}
}

func (s *StrExpr) Resolved() bool {
	//TODO implement me
	panic("implement me")
}

func (s *StrExpr) String() string {
	return s.s
}

func (s *StrExpr) Type() sql.Type {
	//TODO implement me
	panic("implement me")
}

func (s *StrExpr) IsNullable() bool {
	//TODO implement me
	panic("implement me")
}

func (s *StrExpr) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	//TODO implement me
	panic("implement me")
}
