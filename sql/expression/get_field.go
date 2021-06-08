// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"

	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
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

// NewGetFieldWithTable creates a GetField expression with table name. The table name may be an alias.
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

// WithTable returns a copy of this expression with the table given
func (p *GetField) WithTable(table string) *GetField {
	p2 := *p
	p2.table = table
	return &p2
}

// WithName returns a copy of this expression with the field name given.
func (p *GetField) WithName(name string) *GetField {
	p2 := *p
	p2.name = name
	return &p2
}

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

// WithChildren implements the Expression interface.
func (p *GetField) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}
	return p, nil
}

func (p *GetField) String() string {
	if p.table == "" {
		return p.name
	}
	return fmt.Sprintf("%s.%s", p.table, p.name)
}

func (p *GetField) DebugString() string {
	return fmt.Sprintf("[%s.%s, idx=%d, type=%s, nullable=%t]", p.table, p.name, p.fieldIndex, p.fieldType, p.nullable)
}

// WithIndex returns this same GetField with a new index.
func (p *GetField) WithIndex(n int) sql.Expression {
	p2 := *p
	p2.fieldIndex = n
	return &p2
}
