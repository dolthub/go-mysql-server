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

// NewCount creates a new Count node.
func NewCount(ctx *sql.Context, e sql.Expression) *Count {
	return &Count{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (c *Count) FunctionName() string {
	return "count"
}

// NewBuffer creates a new buffer for the aggregation.
func (c *Count) NewBuffer() sql.Row {
	return sql.NewRow(int64(0))
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
func (c *Count) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return NewCount(ctx, children[0]), nil
}

// Update implements the Aggregation interface.
func (c *Count) Update(ctx *sql.Context, buffer, row sql.Row) error {
	var inc bool
	if _, ok := c.Child.(*expression.Star); ok {
		inc = true
	} else {
		v, err := c.Child.Eval(ctx, row)
		if v != nil {
			inc = true
		}

		if err != nil {
			return err
		}
	}

	if inc {
		buffer[0] = buffer[0].(int64) + int64(1)
	}

	return nil
}

// Merge implements the Aggregation interface.
func (c *Count) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	buffer[0] = buffer[0].(int64) + partial[0].(int64)
	return nil
}

// Eval implements the Aggregation interface.
func (c *Count) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	count := buffer[0]
	return count, nil
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
func (c *CountDistinct) NewBuffer() sql.Row {
	return sql.NewRow(make(map[uint64]struct{}))
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
func (c *CountDistinct) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return NewCountDistinct(children[0]), nil
}

// Update implements the Aggregation interface.
func (c *CountDistinct) Update(ctx *sql.Context, buffer, row sql.Row) error {
	seen := buffer[0].(map[uint64]struct{})
	var value interface{}
	if _, ok := c.Child.(*expression.Star); ok {
		value = row
	} else {
		v, err := c.Child.Eval(ctx, row)
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

	seen[hash] = struct{}{}

	return nil
}

// Merge implements the Aggregation interface.
func (c *CountDistinct) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	seen := buffer[0].(map[uint64]struct{})
	for k := range partial[0].(map[uint64]struct{}) {
		seen[k] = struct{}{}
	}
	return nil
}

// Eval implements the Aggregation interface.
func (c *CountDistinct) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	seen := buffer[0].(map[uint64]struct{})
	return int64(len(seen)), nil
}
