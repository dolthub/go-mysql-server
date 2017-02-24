package plan

import (
	"fmt"

	"gopkg.in/sqle/sqle.v0/sql"
)

type UnresolvedTable struct {
	Name string
}

func NewUnresolvedTable(name string) *UnresolvedTable {
	return &UnresolvedTable{name}
}

func (*UnresolvedTable) Resolved() bool {
	return false
}

func (*UnresolvedTable) Children() []sql.Node {
	return []sql.Node{}
}

func (*UnresolvedTable) Schema() sql.Schema {
	return sql.Schema{}
}

func (*UnresolvedTable) RowIter() (sql.RowIter, error) {
	return nil, fmt.Errorf("unresolved table")
}

func (p *UnresolvedTable) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(NewUnresolvedTable(p.Name))
}

func (p *UnresolvedTable) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return p
}
