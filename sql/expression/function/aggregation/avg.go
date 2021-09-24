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
var _ sql.Aggregation = (*Avg)(nil)

// NewAvg creates a new Avg node.
func NewAvg(e sql.Expression) *Avg {
	return &Avg{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (a *Avg) FunctionName() string {
	return "avg"
}

func (a *Avg) String() string {
	return fmt.Sprintf("AVG(%s)", a.Child)
}

// Type implements Expression interface.
func (a *Avg) Type() sql.Type {
	return sql.Float64
}

// IsNullable implements Expression interface.
func (a *Avg) IsNullable() bool {
	return true
}

// Eval implements Expression interface.
func (a *Avg) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return nil, ErrEvalUnsupportedOnAggregation.New("Avg")
}

// WithChildren implements the Expression interface.
func (a *Avg) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 1)
	}
	return NewAvg(children[0]), nil
}

// NewBuffer implements Aggregation interface.
func (a *Avg) NewBuffer() (sql.AggregationBuffer, error) {
	const (
		sum  = float64(0)
		rows = int64(0)
	)

	bufferChild, err := expression.Clone(a.UnaryExpression.Child)
	if err != nil {
		return nil, err
	}

	return &avgBuffer{sum, rows, bufferChild}, nil
}

type avgBuffer struct {
	sum  float64
	rows int64
	expr sql.Expression
}

// Update implements the AggregationBuffer interface.
func (a *avgBuffer) Update(ctx *sql.Context, row sql.Row) error {
	v, err := a.expr.Eval(ctx, row)
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

	a.sum += v.(float64)
	a.rows += 1

	return nil
}

// Eval implements the AggregationBuffer interface.
func (a *avgBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	// This case is triggered when no rows exist.
	if a.sum == 0 && a.rows == 0 {
		return nil, nil
	}

	if a.rows == 0 {
		return float64(0), nil
	}

	return a.sum / float64(a.rows), nil
}

// Dispose implements the Disposable interface.
func (a *avgBuffer) Dispose() {
	expression.Dispose(a.expr)
}
