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

package aggregation

import (
	"fmt"

	"github.com/mitchellh/hashstructure"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Count node to count how many rows are in the result set.
type Count struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Count)(nil)
var _ sql.Aggregation = (*Count)(nil)

var _ sql.FunctionExpression = (*CountDistinct)(nil)
var _ sql.Aggregation = (*CountDistinct)(nil)

// NewCount creates a new Count node.
func NewCount(e sql.Expression) *Count {
	return &Count{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (c *Count) FunctionName() string {
	return "count"
}

// NewBuffer creates a new buffer for the aggregation.
func (c *Count) NewBuffer() (sql.AggregationBuffer, error) {
	bufferChild, err := expression.Clone(c.UnaryExpression.Child)
	if err != nil {
		return nil, err
	}
	return &countBuffer{0, bufferChild}, nil
}

// Type returns the type of the result.
func (c *Count) Type() sql.Type {
	return sql.Int64
}

// IsNullable returns whether the return value can be null.
func (c *Count) IsNullable() bool {
	return false
}

// Resolved implements the Expression interface.
func (c *Count) Resolved() bool {
	if _, ok := c.Child.(*expression.Star); ok {
		return true
	}

	return c.Child.Resolved()
}

func (c *Count) String() string {
	return fmt.Sprintf("COUNT(%s)", c.Child)
}

// WithChildren implements the Expression interface.
func (c *Count) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return NewCount(children[0]), nil
}

// Eval implements the Expression interface.
func (c *Count) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return nil, ErrEvalUnsupportedOnAggregation.New("Count")
}

// CountDistinct node to count how many rows are in the result set.
type CountDistinct struct {
	expression.UnaryExpression
}

// NewCountDistinct creates a new CountDistinct node.
func NewCountDistinct(e sql.Expression) *CountDistinct {
	return &CountDistinct{expression.UnaryExpression{Child: e}}
}

// NewBuffer creates a new buffer for the aggregation.
func (c *CountDistinct) NewBuffer() (sql.AggregationBuffer, error) {
	return &countDistinctBuffer{make(map[uint64]struct{}), c.Child}, nil
}

// Type returns the type of the result.
func (c *CountDistinct) Type() sql.Type {
	return sql.Int64
}

// IsNullable returns whether the return value can be null.
func (c *CountDistinct) IsNullable() bool {
	return false
}

// Resolved implements the Expression interface.
func (c *CountDistinct) Resolved() bool {
	if _, ok := c.Child.(*expression.Star); ok {
		return true
	}

	return c.Child.Resolved()
}

func (c *CountDistinct) String() string {
	return fmt.Sprintf("COUNT(DISTINCT %s)", c.Child)
}

// WithChildren implements the Expression interface.
func (c *CountDistinct) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return NewCountDistinct(children[0]), nil
}

// Eval implements the Expression interface.
func (c *CountDistinct) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return nil, ErrEvalUnsupportedOnAggregation.New("CountDistinct")
}

// FunctionName implements sql.FunctionExpression
func (c *CountDistinct) FunctionName() string {
	return "count distinct"
}

type countDistinctBuffer struct {
	seen map[uint64]struct{}
	expr sql.Expression
}

// Update implements the AggregationBuffer interface.
func (c *countDistinctBuffer) Update(ctx *sql.Context, row sql.Row) error {
	var value interface{}
	if _, ok := c.expr.(*expression.Star); ok {
		value = row
	} else {
		v, err := c.expr.Eval(ctx, row)
		if v == nil {
			return nil
		}

		if err != nil {
			return err
		}

		value = v
	}

	hash, err := hashstructure.Hash(value, nil)
	if err != nil {
		return fmt.Errorf("count distinct unable to hash value: %s", err)
	}

	c.seen[hash] = struct{}{}

	return nil
}

// Eval implements the AggregationBuffer interface.
func (c *countDistinctBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	return int64(len(c.seen)), nil
}

func (c *countDistinctBuffer) Dispose() {
	expression.Dispose(c.expr)
}

type countBuffer struct {
	cnt  int64
	expr sql.Expression
}

// Update implements the AggregationBuffer interface.
func (c *countBuffer) Update(ctx *sql.Context, row sql.Row) error {
	var inc bool
	if _, ok := c.expr.(*expression.Star); ok {
		inc = true
	} else {
		v, err := c.expr.Eval(ctx, row)
		if v != nil {
			inc = true
		}

		if err != nil {
			return err
		}
	}

	if inc {
		c.cnt += 1
	}

	return nil
}

// Eval implements the AggregationBuffer interface.
func (c *countBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	return c.cnt, nil
}

// Dispose implements the Disposable interface.
func (c *countBuffer) Dispose() {
	expression.Dispose(c.expr)
}
