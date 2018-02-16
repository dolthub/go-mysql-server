package expression

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// GetField is an expression to get the field of a table.
type GetField struct {
	fieldIndex int
	fieldName  string
	fieldType  sql.Type
	nullable   bool
}

// NewGetField creates a GetField expression.
func NewGetField(index int, fieldType sql.Type, fieldName string, nullable bool) *GetField {
	return &GetField{
		fieldIndex: index,
		fieldType:  fieldType,
		fieldName:  fieldName,
		nullable:   nullable,
	}
}

// Resolved implements the Expression interface.
func (p GetField) Resolved() bool {
	return true
}

// IsNullable returns whether the field is nullable or not.
func (p GetField) IsNullable() bool {
	return p.nullable
}

// Type returns the type of the field.
func (p GetField) Type() sql.Type {
	return p.fieldType
}

// Eval implements the Expression interface.
func (p GetField) Eval(row sql.Row) interface{} {
	return row[p.fieldIndex]
}

// Name returns the name of the field.
func (p GetField) Name() string {
	return p.fieldName
}

// TransformUp implements the Expression interface.
func (p *GetField) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	n := *p
	return f(&n)
}
