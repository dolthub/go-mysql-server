package plan

import (
	"github.com/mvader/gitql/sql"
)

type Project struct {
	UnaryNode
	expressions []sql.Expression
	schema      sql.Schema
}

func NewProject(expressions []sql.Expression, child sql.Node) *Project {
	schema := sql.Schema{}
	childSchema := child.Schema()
	for _, expr := range expressions {
		for _, field := range childSchema {
			if expr.Name() == field.Name {
				schema = append(schema, field)
				break
			}
		}
	}
	return &Project{
		UnaryNode:   UnaryNode{child},
		expressions: expressions,
		schema:      schema,
	}
}

func (p *Project) Schema() sql.Schema {
	return p.schema
}

func (p *Project) RowIter() (sql.RowIter, error) {
	i, err := p.Child.RowIter()
	if err != nil {
		return nil, err
	}
	return &iter{p, i}, nil
}

type iter struct {
	p         *Project
	childIter sql.RowIter
}

func (i *iter) Next() (sql.Row, error) {
	childRow, err := i.childIter.Next()
	if err != nil {
		return nil, err
	}
	return filterRow(i.p.expressions, childRow), nil
}

func filterRow(expressions []sql.Expression, row sql.Row) sql.Row {
	fields := []interface{}{}
	for _, expr := range expressions {
		fields = append(fields, expr.Eval(row))
	}
	return sql.NewMemoryRow(fields...)
}
