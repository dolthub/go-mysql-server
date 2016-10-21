package plan

import (
	"fmt"

	"github.com/mvader/gitql/sql"
)

type UnresolvedRelation struct {
	Name string
}

func NewUnresolvedRelation(name string) *UnresolvedRelation {
	return &UnresolvedRelation{name}
}

func (*UnresolvedRelation) Resolved() bool {
	return false
}

func (*UnresolvedRelation) Children() []sql.Node {
	return []sql.Node{}
}

func (*UnresolvedRelation) Schema() sql.Schema {
	return sql.Schema{}
}

func (*UnresolvedRelation) RowIter() (sql.RowIter, error) {
	return nil, fmt.Errorf("unresolved relation")
}

func (p *UnresolvedRelation) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(&UnresolvedRelation{p.Name})
}

func (p *UnresolvedRelation) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return p
}
