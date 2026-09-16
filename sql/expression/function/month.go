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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Month is a function that returns the month of a date.
type Month struct {
	*UnaryDatetimeFunc
}

var _ sql.FunctionExpression = (*Month)(nil)
var _ sql.CollationCoercible = (*Month)(nil)

// NewMonth creates a new Month UDF.
func NewMonth(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &Month{
		UnaryDatetimeFunc: NewUnaryDatetimeFunc(date, "MONTH", types.Int32),
	}
}

// Description implements sql.FunctionExpression
func (m *Month) Description() string {
	return "returns the month of the given date."
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (m *Month) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements the Expression interface.
func (m *Month) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	val, err := m.EvalChild(ctx, row)
	if err != nil {
		return 0, nil
	}
	dt, ok := val.(time.Time)
	if !ok {
		return nil, nil
	}
	if types.ZeroTime.Equal(dt) {
		return 0, nil
	}
	return int(dt.Month()), nil
}

// WithChildren implements the Expression interface.
func (m *Month) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewMonth(ctx, children[0]), nil
}
