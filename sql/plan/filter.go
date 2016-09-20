package plan

import "github.com/mvader/gitql/sql"

type Filter struct {
	fieldIndex int
	value      interface{}
	child      sql.Node
}

func NewFilter(fieldName string, child sql.Node, value interface{}) *Filter {
	childSchema := child.Schema()
	var i int
	for index, field := range childSchema {
		if fieldName == field.Name {
			i = index
			break
		}
	}

	return &Filter{
		fieldIndex: i,
		value:      value,
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
	index := i.f.fieldIndex
	for {
		row, err := i.childIter.Next()

		if err != nil {
			return nil, err
		} else if row.Fields()[index] == i.f.value {
			return row, nil
		}
	}
}
