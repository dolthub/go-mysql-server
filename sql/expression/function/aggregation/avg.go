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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Avg node to calculate the average from numeric column
type Avg struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Avg)(nil)

// NewAvg creates a new Avg node.
func NewAvg(ctx *sql.Context, e sql.Expression) *Avg {
	return &Avg{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (a *Avg) FunctionName() string {
	return "avg"
}

func (a *Avg) String() string {
	return fmt.Sprintf("AVG(%s)", a.Child)
}

// Type implements AggregationExpression interface. (AggregationExpression[Expression]])
func (a *Avg) Type() sql.Type {
	return sql.Float64
}

// IsNullable implements AggregationExpression interface. (AggregationExpression[Expression]])
func (a *Avg) IsNullable() bool {
	return true
}

// Eval implements AggregationExpression interface. (AggregationExpression[Expression]])
func (a *Avg) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	// This case is triggered when no rows exist.
	if buffer[0] == float64(0) && buffer[1] == int64(0) {
		return nil, nil
	}

	sum := buffer[0].(float64)
	rows := buffer[1].(int64)

	if rows == 0 {
		return float64(0), nil
	}

	return sum / float64(rows), nil
}

// WithChildren implements the Expression interface.
func (a *Avg) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 1)
	}
	return NewAvg(ctx, children[0]), nil
}

// NewBuffer implements AggregationExpression interface. (AggregationExpression)
func (a *Avg) NewBuffer() sql.Row {
	const (
		sum  = float64(0)
		rows = int64(0)
	)

	return sql.NewRow(sum, rows)
}

// Update implements AggregationExpression interface. (AggregationExpression)
func (a *Avg) Update(ctx *sql.Context, buffer, row sql.Row) error {
	v, err := a.Child.Eval(ctx, row)
	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}

	v, err = sql.Float64.Convert(v)
	if err != nil {
		v = float64(0)
	}

	buffer[0] = buffer[0].(float64) + v.(float64)
	buffer[1] = buffer[1].(int64) + 1

	return nil
}

// Merge implements AggregationExpression interface. (AggregationExpression)
func (a *Avg) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	bsum := buffer[0].(float64)
	brows := buffer[1].(int64)

	psum := partial[0].(float64)
	prows := partial[1].(int64)

	buffer[0] = bsum + psum
	buffer[1] = brows + prows

	return nil
}
