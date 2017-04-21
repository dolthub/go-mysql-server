package expression

import "gopkg.in/sqle/sqle.v0/sql"

type GetField struct {
	fieldIndex int
	fieldName  string
	fieldType  sql.Type
	nullable   bool
}

func NewGetField(index int, fieldType sql.Type, fieldName string, nullable bool) *GetField {
	return &GetField{
		fieldIndex: index,
		fieldType:  fieldType,
		fieldName:  fieldName,
		nullable:   nullable,
	}
}

func (p GetField) Resolved() bool {
	return true
}

func (p GetField) IsNullable() bool {
	return p.nullable
}

func (p GetField) Type() sql.Type {
	return p.fieldType
}

func (p GetField) Eval(row sql.Row) interface{} {
	return row[p.fieldIndex]
}

func (p GetField) Name() string {
	return p.fieldName
}

func (p *GetField) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	n := *p
	return f(&n)
}
