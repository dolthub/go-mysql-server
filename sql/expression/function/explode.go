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

package function

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// Explode is a function that generates a row for each value of its child.
// It is a placeholder expression node.
type Explode struct {
	Child sql.Expression
}

var _ sql.FunctionExpression = (*Explode)(nil)

// NewExplode creates a new Explode function.
func NewExplode(ctx *sql.Context, child sql.Expression) sql.Expression {
	return &Explode{child}
}

// FunctionName implements sql.FunctionExpression
func (e *Explode) FunctionName() string {
	return "explode"
}

// Resolved implements the sql.Expression interface.
func (e *Explode) Resolved() bool { return e.Child.Resolved() }

// Children implements the sql.Expression interface.
func (e *Explode) Children() []sql.Expression { return []sql.Expression{e.Child} }

// IsNullable implements the sql.Expression interface.
func (e *Explode) IsNullable() bool { return e.Child.IsNullable() }

// Type implements the sql.Expression interface.
func (e *Explode) Type() sql.Type {
	return sql.UnderlyingType(e.Child.Type())
}

// Eval implements the sql.Expression interface.
func (e *Explode) Eval(*sql.Context, sql.Row) (interface{}, error) {
	panic("eval method of Explode is only a placeholder")
}

func (e *Explode) String() string {
	return fmt.Sprintf("EXPLODE(%s)", e.Child)
}

// WithChildren implements the Expression interface.
func (e *Explode) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	return NewExplode(ctx, children[0]), nil
}

// Generate is a function that generates a row for each value of its child.
// This is the non-placeholder counterpart of Explode.
type Generate struct {
	Child sql.Expression
}

// NewGenerate creates a new Generate function.
func NewGenerate(ctx *sql.Context, child sql.Expression) sql.Expression {
	return &Generate{child}
}

// Resolved implements the sql.Expression interface.
func (e *Generate) Resolved() bool { return e.Child.Resolved() }

// Children implements the sql.Expression interface.
func (e *Generate) Children() []sql.Expression { return []sql.Expression{e.Child} }

// IsNullable implements the sql.Expression interface.
func (e *Generate) IsNullable() bool { return e.Child.IsNullable() }

// Type implements the sql.Expression interface.
func (e *Generate) Type() sql.Type {
	return e.Child.Type()
}

// Eval implements the sql.Expression interface.
func (e *Generate) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return e.Child.Eval(ctx, row)
}

func (e *Generate) String() string {
	return fmt.Sprintf("EXPLODE(%s)", e.Child)
}

// WithChildren implements the Expression interface.
func (e *Generate) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	return NewGenerate(ctx, children[0]), nil
}
