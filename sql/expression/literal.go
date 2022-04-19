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
	val2      sql.Value
	fieldType sql.Type
}

var _ sql.Expression = &Literal{}
var _ sql.Expression2 = &Literal{}

// NewLiteral creates a new Literal expression.
func NewLiteral(value interface{}, fieldType sql.Type) *Literal {
	val2, _ := sql.ConvertToValue(value)
	return &Literal{
		value:     value,
		val2:      val2,
		fieldType: fieldType,
	}
}

// Resolved implements the Expression interface.
func (lit *Literal) Resolved() bool {
	return true
}

// IsNullable implements the Expression interface.
func (lit *Literal) IsNullable() bool {
	return lit.value == nil
}

// Type implements the Expression interface.
func (lit *Literal) Type() sql.Type {
	return lit.fieldType
}

// Eval implements the Expression interface.
func (lit *Literal) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return lit.value, nil
}

func (lit *Literal) String() string {
	switch v := lit.value.(type) {
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

func (lit *Literal) DebugString() string {
	typeStr := lit.fieldType.String()
	switch v := lit.value.(type) {
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
func (lit *Literal) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(lit, len(children), 0)
	}
	return lit, nil
}

// Children implements the Expression interface.
func (*Literal) Children() []sql.Expression {
	return nil
}

func (lit *Literal) Eval2(ctx *sql.Context, row sql.Row2) (sql.Value, error) {
	return lit.val2, nil
}

func (lit *Literal) Type2() sql.Type2 {
	t2, ok := lit.fieldType.(sql.Type2)
	if !ok {
		panic(fmt.Errorf("expected Type2, but was %T", lit.fieldType))
	}
	return t2
}

// Value returns the literal value.
func (p *Literal) Value() interface{} {
	return p.value
}
