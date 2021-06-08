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

	"github.com/dolthub/go-mysql-server/sql"
)

// Literal represents a literal expression (string, number, bool, ...).
type Literal struct {
	value     interface{}
	fieldType sql.Type
}

var _ sql.Expression = &Literal{}

// NewLiteral creates a new Literal expression.
func NewLiteral(value interface{}, fieldType sql.Type) *Literal {
	// TODO(juanjux): we should probably check here if the type is sql.VarChar and the
	// Capacity of the Type and the length of the value, but this can't return an error
	return &Literal{
		value:     value,
		fieldType: fieldType,
	}
}

// Resolved implements the Expression interface.
func (p *Literal) Resolved() bool {
	return true
}

// IsNullable implements the Expression interface.
func (p *Literal) IsNullable() bool {
	return p.value == nil
}

// Type implements the Expression interface.
func (p *Literal) Type() sql.Type {
	return p.fieldType
}

// Eval implements the Expression interface.
func (p *Literal) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return p.value, nil
}

func (p *Literal) String() string {
	switch v := p.value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case string:
		return fmt.Sprintf("%q", v)
	case []byte:
		return "BLOB"
	case nil:
		return "NULL"
	default:
		return fmt.Sprint(v)
	}
}

func (p *Literal) DebugString() string {
	typeStr := p.fieldType.String()
	switch v := p.value.(type) {
	case string:
		return fmt.Sprintf("%s (%s)", v, typeStr)
	case []byte:
		return fmt.Sprintf("BLOB(%s)", string(v))
	case nil:
		return fmt.Sprintf("NULL (%s)", typeStr)
	case int, uint, int8, uint8, int16, uint16, int32, uint32, int64, uint64:
		return fmt.Sprintf("%d (%s)", v, typeStr)
	case float32, float64:
		return fmt.Sprintf("%f (%s)", v, typeStr)
	default:
		return fmt.Sprintf("%s (%s)", v, typeStr)
	}
}

// WithChildren implements the Expression interface.
func (p *Literal) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}
	return p, nil
}

// Children implements the Expression interface.
func (*Literal) Children() []sql.Expression {
	return nil
}

// Value returns the literal value.
func (p *Literal) Value() interface{} {
	return p.value
}
