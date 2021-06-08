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

	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

// Concat joins several strings together.
type Concat struct {
	args []sql.Expression
}

var _ sql.FunctionExpression = (*Concat)(nil)

// ErrConcatArrayWithOthers is returned when there are more than 1 argument in
// concat and any of them is an array.
var ErrConcatArrayWithOthers = errors.NewKind("can't concat a string array with any other elements")

// NewConcat creates a new Concat UDF.
func NewConcat(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	if len(args) == 0 {
		return nil, sql.ErrInvalidArgumentNumber.New("CONCAT", "1 or more", 0)
	}

	for _, arg := range args {
		// Don't perform this check until it's resolved. Otherwise we
		// can't get the type for sure.
		if !arg.Resolved() {
			continue
		}

		if len(args) > 1 && sql.IsArray(arg.Type()) {
			return nil, ErrConcatArrayWithOthers.New()
		}

		if sql.IsTuple(arg.Type()) {
			return nil, sql.ErrInvalidType.New("tuple")
		}
	}

	return &Concat{args}, nil
}

// FunctionName implements sql.FunctionExpression
func (c *Concat) FunctionName() string {
	return "concat"
}

// Type implements the Expression interface.
func (f *Concat) Type() sql.Type { return sql.LongText }

// IsNullable implements the Expression interface.
func (f *Concat) IsNullable() bool {
	for _, arg := range f.args {
		if arg.IsNullable() {
			return true
		}
	}
	return false
}

func (f *Concat) String() string {
	var args = make([]string, len(f.args))
	for i, arg := range f.args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("concat(%s)", strings.Join(args, ", "))
}

// WithChildren implements the Expression interface.
func (*Concat) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewConcat(ctx, children...)
}

// Resolved implements the Expression interface.
func (f *Concat) Resolved() bool {
	for _, arg := range f.args {
		if !arg.Resolved() {
			return false
		}
	}
	return true
}

// Children implements the Expression interface.
func (f *Concat) Children() []sql.Expression { return f.args }

// Eval implements the Expression interface.
func (f *Concat) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	var parts []string

	for _, arg := range f.args {
		val, err := arg.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		if val == nil {
			return nil, nil
		}

		if sql.IsArray(arg.Type()) {
			val, err = sql.CreateArray(sql.LongText).Convert(val)
			if err != nil {
				return nil, err
			}

			for _, v := range val.([]interface{}) {
				parts = append(parts, v.(string))
			}
		} else {
			val, err = sql.LongText.Convert(val)
			if err != nil {
				return nil, err
			}

			parts = append(parts, val.(string))
		}
	}

	return strings.Join(parts, ""), nil
}
