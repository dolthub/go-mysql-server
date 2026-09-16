// Copyright 2026 Dolthub, Inc.
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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Quarter is a function that returns the quarter of the year of a date, from 1 to 4.
type Quarter struct {
	*UnaryDatetimeFunc
}

var _ sql.FunctionExpression = (*Quarter)(nil)
var _ sql.CollationCoercible = (*Quarter)(nil)

// NewQuarter creates a new Month UDF.
func NewQuarter(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &Quarter{
		UnaryDatetimeFunc: NewUnaryDatetimeFunc(date, "QUARTER", types.Int32),
	}
}

// Description implements sql.FunctionExpression
func (q *Quarter) Description() string {
	return "returns the quarter of the given date."
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (q *Quarter) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements the Expression interface.
func (q *Quarter) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	val, err := q.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}
	dt, ok := val.(time.Time)
	if !ok {
		return nil, nil
	}
	if types.ZeroTime.Equal(dt) {
		return 0, nil
	}
	return (int(dt.Month())-1)/3 + 1, nil
}

// WithChildren implements the Expression interface.
func (q *Quarter) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(q, len(children), 1)
	}
	return NewQuarter(ctx, children[0]), nil
}
