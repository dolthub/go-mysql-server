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

// Last aggregation returns the last of all values in the selected column.
// It implements the Aggregation interface.
type Last struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Last)(nil)
var _ sql.Aggregation = (*Last)(nil)

// NewLast returns a new Last node.
func NewLast(e sql.Expression) *Last {
	return &Last{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (l *Last) FunctionName() string {
	return "last"
}

// Type returns the resultant type of the aggregation.
func (l *Last) Type() sql.Type {
	return l.Child.Type()
}

func (l *Last) String() string {
	return fmt.Sprintf("LAST(%s)", l.Child)
}

// WithChildren implements the sql.Expression interface.
func (l *Last) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 1)
	}
	return NewLast(children[0]), nil
}

// NewBuffer creates a new buffer to compute the result.
func (l *Last) NewBuffer() (sql.AggregationBuffer, error) {
	bufferChild, err := expression.Clone(l.UnaryExpression.Child)
	if err != nil {
		return nil, err
	}
	return &lastBuffer{nil, bufferChild}, nil
}

// Eval implements the sql.Expression interface.
func (l *Last) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return nil, ErrEvalUnsupportedOnAggregation.New("Last")
}

type lastBuffer struct {
	val  interface{}
	expr sql.Expression
}

// Update implements the AggregationBuffer interface.
func (l *lastBuffer) Update(ctx *sql.Context, row sql.Row) error {
	v, err := l.expr.Eval(ctx, row)
	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}

	l.val = v

	return nil
}

// Eval implements the AggregationBuffer interface.
func (l *lastBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	return l.val, nil
}

// Dispose implements the Disposable interface.
func (l *lastBuffer) Dispose() {
	expression.Dispose(l.expr)
}
