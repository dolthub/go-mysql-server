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
	"github.com/mitchellh/hashstructure"
)

// Sum agregation returns the sum of all values in the selected column.
// It implements the Aggregation interface.
type Sum struct {
	expression.UnaryExpression
	distinct *AggregateDistinctOperator
}

var _ sql.FunctionExpression = (*Sum)(nil)

// NewSum returns a new Sum node.
func NewSum(e sql.Expression) *Sum {
	return &Sum{expression.UnaryExpression{Child: e}, nil}
}

// FunctionName implements sql.FunctionExpression
func (m *Sum) FunctionName() string {
	return "sum"
}

// Type returns the resultant type of the aggregation.
func (m *Sum) Type() sql.Type {
	return sql.Float64
}

func (m *Sum) String() string {
	return fmt.Sprintf("SUM(%s)", m.Child)
}

func (m *Sum) Children() []sql.Expression {
	if m.distinct != nil {
		return []sql.Expression{m.Child, expression.NewLiteral(true, sql.Boolean)}
	}

	return []sql.Expression{m.Child, expression.NewLiteral(false, sql.Boolean)}
}

// WithChildren implements the Expression interface.
func (m *Sum) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) == 0 || len(children) > 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}

	if len(children) == 1 {
		return NewSum(children[0]), nil
	}

	hasDistict := children[1].String() == "true"

	if hasDistict {
		expr := NewSum(children[0])
		return expr.WithDistinctIterator(&AggregateDistinctOperator{seen: nil, dispose: nil})
	}

	return NewSum(children[0]), nil
}

// NewBuffer creates a new buffer to compute the result.
func (m *Sum) NewBuffer() sql.Row {
	return sql.NewRow(nil)
}

// Update implements the Aggregation interface.
func (m *Sum) Update(ctx *sql.Context, buffer, row sql.Row) error {
	v, err := m.Child.Eval(ctx, row)
	if err != nil {
		return err
	}

	if m.distinct != nil {
		shouldUseValue, err := m.distinct.ShouldProcess(ctx, v)
		if err != nil {
			return err
		}

		if !shouldUseValue {
			return nil
		}
	}

	if v == nil {
		return nil
	}

	val, err := sql.Float64.Convert(v)
	if err != nil {
		val = float64(0)
	}

	if buffer[0] == nil {
		buffer[0] = float64(0)
	}

	buffer[0] = buffer[0].(float64) + val.(float64)

	return nil
}

// Merge implements the Aggregation interface.
func (m *Sum) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	return m.Update(ctx, buffer, partial)
}

// Eval implements the Aggregation interface.
func (m *Sum) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	sum := buffer[0]

	if m.distinct != nil {
		m.distinct.dispose()
	}

	return sum, nil
}

func (m *Sum) WithDistinctIterator(distinct *AggregateDistinctOperator) (*Sum, error) {
	nr := *m
	nr.distinct = distinct
	return &nr, nil
}

type AggregateDistinctOperator struct {
	seen sql.KeyValueCache
	dispose sql.DisposeFunc
}

func NewAggregateDistinctOperator(ctx *sql.Context) *AggregateDistinctOperator{
	cache, dispose := ctx.Memory.NewHistoryCache()
	return &AggregateDistinctOperator{
		seen:      cache,
		dispose:   dispose,
	}
}

func (ad *AggregateDistinctOperator) ShouldProcess(ctx *sql.Context, value interface{}) (bool, error) {
	if ad.seen == nil {
		cache, dispose := ctx.Memory.NewHistoryCache()
		ad.seen = cache
		ad.dispose = dispose
	}

	hash, err := hashstructure.Hash(value, nil)
	if err != nil {
		return false, err
	}

	if _, err := ad.seen.Get(hash); err == nil {
		return false, nil
	}

	if err := ad.seen.Put(hash, struct{}{}); err != nil {
		return false, err
	}

	return true, nil
}

func (ad *AggregateDistinctOperator) Dispose() {
	if ad.dispose != nil {
		ad.dispose()
	}
}