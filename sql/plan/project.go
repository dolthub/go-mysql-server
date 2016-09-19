package sql

import "github.com/mvader/gitql/sql"

type Project struct {
	fieldIndexes []int
	schema sql.Schema
	child sql.Node
}

func NewProject(fieldNames []string, child sql.Node) *Project {
	indexes := []int{}
	childSchema := child.Schema()
	schema := sql.Schema{}
	for _, name := range fieldNames {
		for idx, field := range childSchema {
			if name == field.Name {
				indexes = append(indexes, idx)
				schema = append(schema, field)
				break
			}
		}
	}
	return &Project{
		fieldIndexes: indexes,
		schema: schema,
		child: child,
	}
}

func (p *Project) Children() []sql.Node {
	return []sql.Node{p.child}
}


func (p *Project) Schema() sql.Schema {
	return p.schema
}

func (p *Project) RowIter() (sql.RowIter, error) {
	i, err := p.child.RowIter()
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
	return filterRow(i.p.fieldIndexes, childRow), nil
}

func filterRow(indexes []int, row sql.Row) sql.Row {
	childFields := row.Fields()
	fields := []interface{}{}
	for _, idx := range indexes {
		fields = append(fields, childFields[idx])
	}
	return sql.NewMemoryRow(fields...)
}
