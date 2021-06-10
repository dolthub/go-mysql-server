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
	"regexp"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Split receives a string and returns the parts of it splitted by a
// delimiter.
type Split struct {
	expression.BinaryExpression
}

var _ sql.FunctionExpression = (*Split)(nil)

// NewSplit creates a new Split UDF.
func NewSplit(ctx *sql.Context, str, delimiter sql.Expression) sql.Expression {
	return &Split{expression.BinaryExpression{
		Left:  str,
		Right: delimiter,
	}}
}

// FunctionName implements sql.FunctionExpression
func (f *Split) FunctionName() string {
	return "split"
}

// Eval implements the Expression interface.
func (f *Split) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	left, err := f.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if left == nil {
		return nil, nil
	}

	left, err = sql.LongText.Convert(left)
	if err != nil {
		return nil, err
	}

	right, err := f.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if right == nil {
		return nil, nil
	}

	right, err = sql.LongText.Convert(right)
	if err != nil {
		return nil, err
	}

	re, err := regexp.Compile(right.(string))
	if err != nil {
		return nil, err
	}

	parts := re.Split(left.(string), -1)
	var result = make([]interface{}, len(parts))
	for i, part := range parts {
		result[i] = part
	}

	return result, nil
}

// Type implements the Expression interface.
func (*Split) Type() sql.Type { return sql.CreateArray(sql.LongText) }

// IsNullable implements the Expression interface.
func (f *Split) IsNullable() bool { return f.Left.IsNullable() || f.Right.IsNullable() }

func (f *Split) String() string {
	return fmt.Sprintf("split(%s, %s)", f.Left, f.Right)
}

// WithChildren implements the Expression interface.
func (f *Split) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 2)
	}
	return NewSplit(ctx, children[0], children[1]), nil
}
