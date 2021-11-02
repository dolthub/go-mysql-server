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

type Trim struct {
	str sql.Expression
	pat sql.Expression
	dir sql.Expression
}

var _ sql.FunctionExpression = (*Trim)(nil)

func NewTrim(str, pat, dir sql.Expression) sql.Expression {
	return &Trim{str, pat, dir}
}

// FunctionName implements sql.FunctionExpression
func (t *Trim) FunctionName() string {
	return "trim"
}

// Children implements the Expression interface.
func (t *Trim) Children() []sql.Expression {
	return []sql.Expression{t.str, t.pat, t.dir}
}

// Eval implements the Expression interface.
func (t *Trim) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate pattern
	pat, err := t.pat.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Cast to string
	var pat_text string
	switch pat := pat.(type) {
	case string:
		pat_text = pat
	case []byte:
		pat_text = string(pat)
	default:
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(pat).String())
	}

	// Evaluate string value
	str, err := t.str.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Cast to string type
	var str_text string
	switch str := str.(type) {
	case string:
		str_text = str
	case []byte:
		str_text = string(str)
	case nil:
		return nil, nil
	default:
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(str).String())
	}

	// Evaluate direction
	dir, err := t.dir.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Cast to string type
	var dir_text string
	switch dir := dir.(type) {
	case string:
		dir_text = dir
	case []byte:
		dir_text = string(dir)
	default:
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(str).String())
	}

	start := 0
	end := len(str_text)
	n := len(pat_text)

	if n > end {
		return str_text, nil
	}

	// remove from left
	if dir_text == "l" || dir_text == "b" {
		for start < end && str_text[start:start+n] == pat_text {
			start += n
		}
	}

	// remove from right
	if dir_text == "r" || dir_text == "b" {
		for start < end && str_text[end-n:end] == pat_text {
			end -= n
		}
	}

	return str_text[start:end], nil
}

// IsNullable implements the Expression interface.
func (t Trim) IsNullable() bool {
	return t.str.IsNullable() || t.pat.IsNullable()
}

func (t Trim) String() string {
	return fmt.Sprintf("TRIM(%s, %s, %s)", t.str, t.pat, t.pat)
}

func (t Trim) Resolved() bool {
	return t.str.Resolved() && t.pat.Resolved() && t.pat.Resolved()
}

func (Trim) Type() sql.Type { return sql.LongText }

func (t Trim) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 3)
	}
	return NewTrim(children[0], children[1], children[2]), nil
}

type LeftTrim struct {
	expression.UnaryExpression
}

func NewLeftTrim(str sql.Expression) sql.Expression {
	return &LeftTrim{expression.UnaryExpression{Child: str}}
}

var _ sql.FunctionExpression = (*LeftTrim)(nil)

func (t *LeftTrim) FunctionName() string {
	return "ltrim"
}

func (t *LeftTrim) Type() sql.Type { return sql.LongText }

func (t *LeftTrim) String() string {
	return fmt.Sprintf("ltrim(%s)", t.Child)
}

func (t *LeftTrim) IsNullable() bool {
	return t.Child.IsNullable()
}

func (t *LeftTrim) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 1)
	}
	return NewLeftTrim(children[0]), nil
}

func (t *LeftTrim) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
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

	return strings.TrimLeftFunc(str.(string), unicode.IsSpace), nil
}

type RightTrim struct {
	expression.UnaryExpression
}

func NewRightTrim(str sql.Expression) sql.Expression {
	return &RightTrim{expression.UnaryExpression{Child: str}}
}

var _ sql.FunctionExpression = (*RightTrim)(nil)

func (t *RightTrim) FunctionName() string {
	return "rtrim"
}

func (t *RightTrim) Type() sql.Type { return sql.LongText }

func (t *RightTrim) String() string {
	return fmt.Sprintf("rtrim(%s)", t.Child)
}

func (t *RightTrim) IsNullable() bool {
	return t.Child.IsNullable()
}

func (t *RightTrim) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 1)
	}
	return NewRightTrim(children[0]), nil
}

func (t *RightTrim) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
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

	return strings.TrimRightFunc(str.(string), unicode.IsSpace), nil
}

/*
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

func NewLeftTrim(e sql.Expression) sql.Expression {
	return newTrim(lTrimType, e)
}

func NewRightTrim(e sql.Expression) sql.Expression {
	return newTrim(rTrimType, e)
}

func NewTrim(e sql.Expression) sql.Expression {
	return newTrim(bTrimType, e)
}

// newTrim creates a new Trim expression.
func newTrim(tType trimType, str sql.Expression) sql.Expression {
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
func (t *Trim) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 1)
	}
	return newTrim(t.trimType, children[0]), nil
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
*/
