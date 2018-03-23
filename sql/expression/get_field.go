package expression

import (
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// GetField is an expression to get the field of a table.
type GetField struct {
	table      string
	fieldIndex int
	name       string
	fieldType  sql.Type
	nullable   bool
}

// NewGetField creates a GetField expression.
func NewGetField(index int, fieldType sql.Type, fieldName string, nullable bool) *GetField {
	return NewGetFieldWithTable(index, fieldType, "", fieldName, nullable)
}

// NewGetFieldWithTable creates a GetField expression with table name.
func NewGetFieldWithTable(index int, fieldType sql.Type, table, fieldName string, nullable bool) *GetField {
	return &GetField{
		table:      table,
		fieldIndex: index,
		fieldType:  fieldType,
		name:       fieldName,
		nullable:   nullable,
	}
}

// Children implements the Expression interface.
func (GetField) Children() []sql.Expression {
	return nil
}

// Table returns the name of the field table.
func (p GetField) Table() string { return p.table }

// Resolved implements the Expression interface.
func (p GetField) Resolved() bool {
	return true
}

// Name implements the Nameable interface.
func (p GetField) Name() string { return p.name }

// IsNullable returns whether the field is nullable or not.
func (p GetField) IsNullable() bool {
	return p.nullable
}

// Type returns the type of the field.
func (p GetField) Type() sql.Type {
	return p.fieldType
}

// Eval implements the Expression interface.
func (p GetField) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return row[p.fieldIndex], nil
}

// TransformUp implements the Expression interface.
func (p *GetField) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	n := *p
	return f(&n)
}

func (p GetField) String() string {
	if p.table == "" {
		return p.name
	}
	return fmt.Sprintf("%s.%s", p.table, p.name)
}
