package plan

import "github.com/mvader/gitql/sql"

type Filter struct {
	expression sql.Expression
	child      sql.Node
}

func NewFilter(expression sql.Expression, child sql.Node) *Filter {
	return &Filter{
		expression: expression,
		child:      child,
	}
}

func (p *Filter) Children() []sql.Node {
	return []sql.Node{p.child}
}

func (p *Filter) RowIter() (sql.RowIter, error) {
	i, err := p.child.RowIter()
	if err != nil {
		return nil, err
	}
	return &filterIter{p, i}, nil
}

type filterIter struct {
	f         *Filter
	childIter sql.RowIter
}

func (i *filterIter) Next() (sql.Row, error) {
	for {
		row, err := i.childIter.Next()

		if err != nil {
			return nil, err
		} else if i.f.expression.Eval(row) != false {
			return row, nil
		}
	}
}
