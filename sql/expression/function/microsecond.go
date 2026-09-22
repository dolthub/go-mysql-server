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

// Microsecond implements the MICROSECOND function
type Microsecond struct {
	*UnaryDatetimeFunc
}

var _ sql.FunctionExpression = (*Microsecond)(nil)
var _ sql.CollationCoercible = (*Microsecond)(nil)

// Description implements sql.FunctionExpression
func (m *Microsecond) Description() string {
	return "returns the microseconds from argument."
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Microsecond) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// NewMicrosecond creates a new Microsecond UDF.
func NewMicrosecond(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &Microsecond{NewUnaryDatetimeFunc(arg, "MICROSECOND", types.Uint64)}
}

// Eval implements the sql.Expression interface.
func (m *Microsecond) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	val, err := m.EvalChild(ctx, row)
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
	return uint64(dt.Nanosecond()) / 1000, nil
}

// WithChildren implements the sql.Expression interface.
func (m *Microsecond) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewMicrosecond(ctx, children[0]), nil
}
