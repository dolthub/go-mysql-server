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
	"reflect"
	"strings"
	"unicode"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type trimType rune

const (
	lTrimType trimType = 'l'
	rTrimType trimType = 'r'
	bTrimType trimType = 'b'
)

// NewTrimFunc returns a Trim creator function with a specific trimType.
func NewTrimFunc(tType trimType) func(ctx *sql.Context, e sql.Expression) sql.Expression {
	return func(ctx *sql.Context, e sql.Expression) sql.Expression {
		return NewTrim(ctx, tType, e)
	}
}

// NewTrim creates a new Trim expression.
func NewTrim(ctx *sql.Context, tType trimType, str sql.Expression) sql.Expression {
	return &Trim{expression.UnaryExpression{Child: str}, tType}
}

// Trim is a function that returns the string with prefix or suffix spaces removed based on the trimType
type Trim struct {
	expression.UnaryExpression
	trimType
}

var _ sql.FunctionExpression = (*Trim)(nil)

// FunctionName implements sql.FunctionExpression
func (t *Trim) FunctionName() string {
	switch t.trimType {
	case lTrimType:
		return "ltrim"
	case rTrimType:
		return "rtrim"
	case bTrimType:
		return "trim"
	default:
		panic("unknown name for trim type")
	}
}

// Type implements the Expression interface.
func (t *Trim) Type() sql.Type { return sql.LongText }

func (t *Trim) String() string {
	switch t.trimType {
	case lTrimType:
		return fmt.Sprintf("ltrim(%s)", t.Child)
	case rTrimType:
		return fmt.Sprintf("rtrim(%s)", t.Child)
	default:
		return fmt.Sprintf("trim(%s)", t.Child)
	}
}

// IsNullable implements the Expression interface.
func (t *Trim) IsNullable() bool {
	return t.Child.IsNullable()
}

// WithChildren implements the Expression interface.
func (t *Trim) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 1)
	}
	return NewTrim(ctx, t.trimType, children[0]), nil
}

// Eval implements the Expression interface.
func (t *Trim) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	str, err := t.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if str == nil {
		return nil, nil
	}

	str, err = sql.LongText.Convert(str)
	if err != nil {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(str))
	}

	switch t.trimType {
	case lTrimType:
		return strings.TrimLeftFunc(str.(string), unicode.IsSpace), nil
	case rTrimType:
		return strings.TrimRightFunc(str.(string), unicode.IsSpace), nil
	default:
		return strings.TrimFunc(str.(string), unicode.IsSpace), nil
	}
}
