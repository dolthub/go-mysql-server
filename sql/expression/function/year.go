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

// Year is a function that returns the year of a date.
type Year struct {
	*UnaryDatetimeFunc
}

var _ sql.FunctionExpression = (*Year)(nil)
var _ sql.CollationCoercible = (*Year)(nil)

// NewYear creates a new Year UDF.
func NewYear(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &Year{
		UnaryDatetimeFunc: NewUnaryDatetimeFunc(date, "YEAR", types.Int32),
	}
}

// Description implements sql.FunctionExpression
func (y *Year) Description() string {
	return "returns the year of the given date."
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Year) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements the Expression interface.
func (y *Year) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := y.EvalChild(ctx, row)
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
	return dt.Year(), nil
}

// WithChildren implements the Expression interface.
func (y *Year) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(y, len(children), 1)
	}
	return NewYear(ctx, children[0]), nil
}
