package expression

import "github.com/mvader/gitql/sql"

type GetField struct {
	fieldIndex int
	fieldName  string
	fieldType  sql.Type
}

func NewGetField(index int, fieldType sql.Type, fieldName string) *GetField {
	return &GetField{
		fieldIndex: index,
		fieldType:  fieldType,
		fieldName:  fieldName,
	}
}

func (p GetField) Resolved() bool {
	return true
}

func (p GetField) Type() sql.Type {
	return p.fieldType
}

func (p GetField) Eval(row sql.Row) interface{} {
	return row.Fields()[p.fieldIndex]
}

func (p GetField) Name() string {
	return p.fieldName
}

func (p *GetField) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	n := *p
	return f(&n)
}
