package expression

import (
	"fmt"

	errors "gopkg.in/src-d/go-errors.v1"
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

// Index returns the index where the GetField will look for the value from a sql.Row.
func (p *GetField) Index() int { return p.fieldIndex }

// Children implements the Expression interface.
func (*GetField) Children() []sql.Expression {
	return nil
}

// Table returns the name of the field table.
func (p *GetField) Table() string { return p.table }

// Resolved implements the Expression interface.
func (p *GetField) Resolved() bool {
	return true
}

// Name implements the Nameable interface.
func (p *GetField) Name() string { return p.name }

// IsNullable returns whether the field is nullable or not.
func (p *GetField) IsNullable() bool {
	return p.nullable
}

// Type returns the type of the field.
func (p *GetField) Type() sql.Type {
	return p.fieldType
}

// ErrIndexOutOfBounds is returned when the field index is out of the bounds.
var ErrIndexOutOfBounds = errors.NewKind("unable to find field with index %d in row of %d columns")

// Eval implements the Expression interface.
func (p *GetField) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if p.fieldIndex < 0 || p.fieldIndex >= len(row) {
		return nil, ErrIndexOutOfBounds.New(p.fieldIndex, len(row))
	}
	return row[p.fieldIndex], nil
}

// TransformUp implements the Expression interface.
func (p *GetField) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	n := *p
	return f(&n)
}

func (p *GetField) String() string {
	if p.table == "" {
		return p.name
	}
	return fmt.Sprintf("%s.%s", p.table, p.name)
}

// WithIndex returns this same GetField with a new index.
func (p *GetField) WithIndex(n int) sql.Expression {
	p2 := *p
	p2.fieldIndex = n
	return &p2
}

// GetSessionField is an expression that returns the value of a session configuration.
type GetSessionField struct {
	name  string
	typ   sql.Type
	value interface{}
}

// NewGetSessionField creates a new GetSessionField expression.
func NewGetSessionField(name string, typ sql.Type, value interface{}) *GetSessionField {
	return &GetSessionField{name, typ, value}
}

// Children implements the sql.Expression interface.
func (f *GetSessionField) Children() []sql.Expression { return nil }

// Eval implements the sql.Expression interface.
func (f *GetSessionField) Eval(*sql.Context, sql.Row) (interface{}, error) {
	return f.value, nil
}

// Type implements the sql.Expression interface.
func (f *GetSessionField) Type() sql.Type { return f.typ }

// IsNullable implements the sql.Expression interface.
func (f *GetSessionField) IsNullable() bool { return f.value == nil }

// Resolved implements the sql.Expression interface.
func (f *GetSessionField) Resolved() bool { return true }

// String implements the sql.Expression interface.
func (f *GetSessionField) String() string { return "@@" + f.name }

// TransformUp implements the sql.Expression interface.
func (f *GetSessionField) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	return fn(f)
}
