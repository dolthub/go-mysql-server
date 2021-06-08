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

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Reverse is a function that returns the reverse of the text provided.
type Reverse struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Reverse)(nil)

// NewReverse creates a new Reverse expression.
func NewReverse(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &Reverse{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (r *Reverse) FunctionName() string {
	return "reverse"
}

// Eval implements the Expression interface.
func (r *Reverse) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	v, err := r.Child.Eval(ctx, row)
	if v == nil || err != nil {
		return nil, err
	}

	v, err = sql.LongText.Convert(v)
	if err != nil {
		return nil, err
	}

	return reverseString(v.(string)), nil
}

func reverseString(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}

func (r *Reverse) String() string {
	return fmt.Sprintf("reverse(%s)", r.Child)
}

// WithChildren implements the Expression interface.
func (r *Reverse) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 1)
	}
	return NewReverse(ctx, children[0]), nil
}

// Type implements the Expression interface.
func (r *Reverse) Type() sql.Type {
	return r.Child.Type()
}

var ErrNegativeRepeatCount = errors.NewKind("negative Repeat count: %v")

// Repeat is a function that returns the string repeated n times.
type Repeat struct {
	expression.BinaryExpression
}

var _ sql.FunctionExpression = (*Repeat)(nil)

// NewRepeat creates a new Repeat expression.
func NewRepeat(ctx *sql.Context, str sql.Expression, count sql.Expression) sql.Expression {
	return &Repeat{expression.BinaryExpression{Left: str, Right: count}}
}

// FunctionName implements sql.FunctionExpression
func (r *Repeat) FunctionName() string {
	return "repeat"
}

func (r *Repeat) String() string {
	return fmt.Sprintf("repeat(%s, %s)", r.Left, r.Right)
}

// Type implements the Expression interface.
func (r *Repeat) Type() sql.Type {
	return sql.LongText
}

// WithChildren implements the Expression interface.
func (r *Repeat) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 2)
	}
	return NewRepeat(ctx, children[0], children[1]), nil
}

// Eval implements the Expression interface.
func (r *Repeat) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	str, err := r.Left.Eval(ctx, row)
	if str == nil || err != nil {
		return nil, err
	}

	str, err = sql.LongText.Convert(str)
	if err != nil {
		return nil, err
	}

	count, err := r.Right.Eval(ctx, row)
	if count == nil || err != nil {
		return nil, err
	}

	count, err = sql.Int32.Convert(count)
	if err != nil {
		return nil, err
	}
	if count.(int32) < 0 {
		return nil, ErrNegativeRepeatCount.New(count)
	}
	return strings.Repeat(str.(string), int(count.(int32))), nil
}

// Replace is a function that returns a string with all occurrences of fromStr replaced by the
// string toStr
type Replace struct {
	str     sql.Expression
	fromStr sql.Expression
	toStr   sql.Expression
}

var _ sql.FunctionExpression = (*Replace)(nil)

// NewReplace creates a new Replace expression.
func NewReplace(ctx *sql.Context, str sql.Expression, fromStr sql.Expression, toStr sql.Expression) sql.Expression {
	return &Replace{str, fromStr, toStr}
}

// FunctionName implements sql.FunctionExpression
func (r *Replace) FunctionName() string {
	return "replace"
}

// Children implements the Expression interface.
func (r *Replace) Children() []sql.Expression {
	return []sql.Expression{r.str, r.fromStr, r.toStr}
}

// Resolved implements the Expression interface.
func (r *Replace) Resolved() bool {
	return r.str.Resolved() && r.fromStr.Resolved() && r.toStr.Resolved()
}

// IsNullable implements the Expression interface.
func (r *Replace) IsNullable() bool {
	return r.str.IsNullable() || r.fromStr.IsNullable() || r.toStr.IsNullable()
}

func (r *Replace) String() string {
	return fmt.Sprintf("replace(%s, %s, %s)", r.str, r.fromStr, r.toStr)
}

// Type implements the Expression interface.
func (r *Replace) Type() sql.Type {
	return sql.LongText
}

// WithChildren implements the Expression interface.
func (r *Replace) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 3)
	}
	return NewReplace(ctx, children[0], children[1], children[2]), nil
}

// Eval implements the Expression interface.
func (r *Replace) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	str, err := r.str.Eval(ctx, row)
	if str == nil || err != nil {
		return nil, err
	}

	str, err = sql.LongText.Convert(str)
	if err != nil {
		return nil, err
	}

	fromStr, err := r.fromStr.Eval(ctx, row)
	if fromStr == nil || err != nil {
		return nil, err
	}

	fromStr, err = sql.LongText.Convert(fromStr)
	if err != nil {
		return nil, err
	}

	toStr, err := r.toStr.Eval(ctx, row)
	if toStr == nil || err != nil {
		return nil, err
	}

	toStr, err = sql.LongText.Convert(toStr)
	if err != nil {
		return nil, err
	}

	if fromStr.(string) == "" {
		return str, nil
	}

	return strings.Replace(str.(string), fromStr.(string), toStr.(string), -1), nil
}
