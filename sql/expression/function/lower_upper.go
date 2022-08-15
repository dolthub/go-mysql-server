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
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Lower is a function that returns the lowercase of the text provided.
type Lower struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Lower)(nil)

// NewLower creates a new Lower expression.
func NewLower(e sql.Expression) sql.Expression {
	return &Lower{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (l *Lower) FunctionName() string {
	return "lower"
}

// Description implements sql.FunctionExpression
func (l *Lower) Description() string {
	return "returns the string str with all characters in lower case."
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

	vStr, collation, err := sql.ConvertToCollatedString(v, l.Child.Type())
	if err != nil {
		return nil, err
	}
	return collation.CharacterSet().Encoder().Lowercase(vStr), nil
}

func (l *Lower) String() string {
	return fmt.Sprintf("LOWER(%s)", l.Child)
}

// WithChildren implements the Expression interface.
func (l *Lower) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 1)
	}
	return NewLower(children[0]), nil
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

// NewUpper creates a new Lower expression.
func NewUpper(e sql.Expression) sql.Expression {
	return &Upper{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (u *Upper) FunctionName() string {
	return "upper"
}

// Description implements sql.FunctionExpression
func (u *Upper) Description() string {
	return "converts string to uppercase."
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

	vStr, collation, err := sql.ConvertToCollatedString(v, u.Child.Type())
	if err != nil {
		return nil, err
	}
	return collation.CharacterSet().Encoder().Uppercase(vStr), nil
}

func (u *Upper) String() string {
	return fmt.Sprintf("UPPER(%s)", u.Child)
}

// WithChildren implements the Expression interface.
func (u *Upper) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(children), 1)
	}
	return NewUpper(children[0]), nil
}

// Type implements the Expression interface.
func (u *Upper) Type() sql.Type {
	return u.Child.Type()
}
