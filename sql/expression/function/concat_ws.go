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
)

// ConcatWithSeparator joins several strings together. The first argument is
// the separator for the rest of the arguments. The separator is added between
// the strings to be concatenated. The separator can be a string, as can the
// rest of the arguments. If the separator is NULL, the result is NULL.
type ConcatWithSeparator struct {
	args []sql.Expression
}

var _ sql.FunctionExpression = (*ConcatWithSeparator)(nil)

// NewConcatWithSeparator creates a new NewConcatWithSeparator UDF.
func NewConcatWithSeparator(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	if len(args) == 0 {
		return nil, sql.ErrInvalidArgumentNumber.New("CONCAT_WS", "1 or more", 0)
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

	return &ConcatWithSeparator{args}, nil
}

// FunctionName implements sql.FunctionExpression
func (f *ConcatWithSeparator) FunctionName() string {
	return "concat_ws"
}

// Type implements the Expression interface.
func (f *ConcatWithSeparator) Type() sql.Type { return sql.LongText }

// IsNullable implements the Expression interface.
func (f *ConcatWithSeparator) IsNullable() bool {
	for _, arg := range f.args {
		if arg.IsNullable() {
			return true
		}
	}
	return false
}

func (f *ConcatWithSeparator) String() string {
	var args = make([]string, len(f.args))
	for i, arg := range f.args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("concat_ws(%s)", strings.Join(args, ", "))
}

// WithChildren implements the Expression interface.
func (*ConcatWithSeparator) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewConcatWithSeparator(ctx, children...)
}

// Resolved implements the Expression interface.
func (f *ConcatWithSeparator) Resolved() bool {
	for _, arg := range f.args {
		if !arg.Resolved() {
			return false
		}
	}
	return true
}

// Children implements the Expression interface.
func (f *ConcatWithSeparator) Children() []sql.Expression { return f.args }

// Eval implements the Expression interface.
func (f *ConcatWithSeparator) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	var parts []string

	for i, arg := range f.args {
		val, err := arg.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		if val == nil && i == 0 {
			return nil, nil
		}

		if val == nil {
			continue
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

	return strings.Join(parts[1:], parts[0]), nil
}
