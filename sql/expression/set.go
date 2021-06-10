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

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

var errCannotSetField = errors.NewKind("Expected GetField expression on left but got %T")

// SetField updates the value of a field or a system variable
type SetField struct {
	BinaryExpression
}

// NewSetField creates a new SetField expression.
func NewSetField(left, expr sql.Expression) sql.Expression {
	return &SetField{BinaryExpression{Left: left, Right: expr}}
}

func (s *SetField) String() string {
	return fmt.Sprintf("SET %s = %s", s.Left, s.Right)
}

func (s *SetField) DebugString() string {
	return fmt.Sprintf("SET %s = %s", sql.DebugString(s.Left), sql.DebugString(s.Right))
}

// Type implements the Expression interface.
func (s *SetField) Type() sql.Type {
	return s.Left.Type()
}

// Eval implements the Expression interface.
// Returns a copy of the given row with an updated value.
func (s *SetField) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	getField, ok := s.Left.(*GetField)
	if !ok {
		return nil, errCannotSetField.New(s.Left)
	}

	if getField.fieldIndex < 0 || getField.fieldIndex >= len(row) {
		return nil, ErrIndexOutOfBounds.New(getField.fieldIndex, len(row))
	}
	val, err := s.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if val != nil {
		val, err = getField.fieldType.Convert(val)
		if err != nil {
			return nil, err
		}
	}
	updatedRow := row.Copy()
	updatedRow[getField.fieldIndex] = val
	return updatedRow, nil
}

// WithChildren implements the Expression interface.
func (s *SetField) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 2)
	}
	return NewSetField(children[0], children[1]), nil
}
