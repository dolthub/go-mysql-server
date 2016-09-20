package expression

import "github.com/mvader/gitql/sql"

type Literal struct {
	value     interface{}
	fieldType sql.Type
}

func NewLiteral(value interface{}, fieldType sql.Type) *Literal {
	return &Literal{
		value:     value,
		fieldType: fieldType,
	}
}

func (p Literal) Type() sql.Type {
	return p.fieldType
}

func (p Literal) Eval(row sql.Row) interface{} {
	return p.value
}
