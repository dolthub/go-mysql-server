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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Lower is a function that returns the lowercase of the text provided.
type Lower struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Lower)(nil)

// NewLower creates a new Lower expression.
func NewLower(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &Lower{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (l *Lower) FunctionName() string {
	return "lower"
}

// Eval implements the Expression interface.
func (l *Lower) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	v, err := l.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, nil
	}

	v, err = sql.LongText.Convert(v)
	if err != nil {
		return nil, err
	}

	return strings.ToLower(v.(string)), nil
}

func (l *Lower) String() string {
	return fmt.Sprintf("LOWER(%s)", l.Child)
}

// WithChildren implements the Expression interface.
func (l *Lower) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 1)
	}
	return NewLower(ctx, children[0]), nil
}

// Type implements the Expression interface.
func (l *Lower) Type() sql.Type {
	return l.Child.Type()
}

// Upper is a function that returns the UPPERCASE of the text provided.
type Upper struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Upper)(nil)

// FunctionName implements sql.FunctionExpression
func (u *Upper) FunctionName() string {
	return "upper"
}

// NewUpper creates a new Lower expression.
func NewUpper(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &Upper{expression.UnaryExpression{Child: e}}
}

// Eval implements the Expression interface.
func (u *Upper) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	v, err := u.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, nil
	}

	v, err = sql.LongText.Convert(v)
	if err != nil {
		return nil, err
	}

	return strings.ToUpper(v.(string)), nil
}

func (u *Upper) String() string {
	return fmt.Sprintf("UPPER(%s)", u.Child)
}

// WithChildren implements the Expression interface.
func (u *Upper) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(children), 1)
	}
	return NewUpper(ctx, children[0]), nil
}

// Type implements the Expression interface.
func (u *Upper) Type() sql.Type {
	return u.Child.Type()
}
