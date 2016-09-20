package plan

import "github.com/mvader/gitql/sql"

type Filter struct {
	UnaryNode
	expression sql.Expression
}

func NewFilter(expression sql.Expression, child sql.Node) *Filter {
	return &Filter{
		UnaryNode:  UnaryNode{Child: child},
		expression: expression,
	}
}

func (p *Filter) RowIter() (sql.RowIter, error) {
	i, err := p.Child.RowIter()
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
