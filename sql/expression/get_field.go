package expression

import "github.com/mvader/gitql/sql"

type GetField struct {
	fieldIndex int
	fieldType  sql.Type
}

func NewGetField(index int, fieldType sql.Type) *GetField {
	return &GetField{
		fieldIndex: index,
		fieldType:  fieldType,
	}
}

func (p GetField) Type() sql.Type {
	return p.fieldType
}

func (p GetField) Eval(row sql.Row) interface{} {
	return row.Fields()[p.fieldIndex]
}
