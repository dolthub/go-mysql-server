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

import "github.com/dolthub/go-mysql-server/sql"

// IsNull is an expression that checks if an expression is null.
type IsNull struct {
	UnaryExpression
}

// NewIsNull creates a new IsNull expression.
func NewIsNull(child sql.Expression) *IsNull {
	return &IsNull{UnaryExpression{child}}
}

// Type implements the Expression interface.
func (e *IsNull) Type() sql.Type {
	return sql.Boolean
}

// IsNullable implements the Expression interface.
func (e *IsNull) IsNullable() bool {
	return false
}

// Eval implements the Expression interface.
func (e *IsNull) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	v, err := e.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	return v == nil, nil
}

func (e IsNull) String() string {
	return e.Child.String() + " IS NULL"
}

func (e IsNull) DebugString() string {
	return sql.DebugString(e.Child) + " IS NULL"
}

// WithChildren implements the Expression interface.
func (e *IsNull) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	return NewIsNull(children[0]), nil
}
