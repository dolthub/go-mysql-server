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

	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TimeToSec implements the time_to_sec function
type TimeToSec struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*TimeToSec)(nil)
var _ sql.CollationCoercible = (*TimeToSec)(nil)

// NewTimeToSec creates a new TimeToSec UDF.
func NewTimeToSec(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &TimeToSec{
		UnaryFunc: NewUnaryFunc(arg, "TIME_TO_SEC", types.Uint64),
	}
}

// Description implements sql.FunctionExpression
func (m *TimeToSec) Description() string {
	return "returns the argument converted to seconds."
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*TimeToSec) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements the sql.Expression interface.
func (m *TimeToSec) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	val, err := m.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	timespan, err := types.Time.ConvertToTimespan(val)
	if err != nil {
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", sql.ErrIncorrectValue.New(val).Error())
		return nil, nil
	}
	return uint64(timespan.AsMicroseconds() / int64(time.Second/time.Microsecond)), nil
}

// WithChildren implements the sql.Expression interface.
func (m *TimeToSec) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewTimeToSec(ctx, children[0]), nil
}
