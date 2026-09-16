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
	"fmt"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// UTCTimestamp is a function that returns the current time.
type UTCTimestamp struct {
	precision *int
}

var _ sql.FunctionExpression = (*UTCTimestamp)(nil)
var _ sql.CollationCoercible = (*UTCTimestamp)(nil)

// NewUTCTimestamp returns a new UTCTimestamp node.
func NewUTCTimestamp(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	var precision *int
	if len(args) > 1 {
		return nil, sql.ErrInvalidArgumentNumber.New("UTC_TIMESTAMP", 1, len(args))
	} else if len(args) == 1 {
		argType := args[0].Type(ctx).Promote()
		if argType != types.Int64 && argType != types.Uint64 {
			return nil, sql.ErrInvalidType.New(args[0].Type(ctx).String())
		}
		val, err := args[0].Eval(ctx, nil)
		if err != nil {
			return nil, err
		}
		precisionArg, _, err := types.Int32.Convert(ctx, val)

		if err != nil {
			return nil, err
		}

		n := int(precisionArg.(int32))
		if n < 0 || n > 6 {
			return nil, sql.ErrValueOutOfRange.New("precision", "utc_timestamp")
		}
		precision = &n
	}

	return &UTCTimestamp{precision}, nil
}

// FunctionName implements sql.FunctionExpression
func (ut *UTCTimestamp) FunctionName() string {
	return "utc_timestamp"
}

// Description implements sql.FunctionExpression
func (ut *UTCTimestamp) Description() string {
	return "returns the current UTC timestamp."
}

// Type implements the sql.Expression interface.
func (ut *UTCTimestamp) Type(ctx *sql.Context) sql.Type {
	return types.DatetimeMaxPrecision
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*UTCTimestamp) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// String implements the sql.Expression interface.
func (ut *UTCTimestamp) String() string {
	if ut.precision == nil {
		return "UTC_TIMESTAMP()"
	}

	return fmt.Sprintf("UTC_TIMESTAMP(%d)", *ut.precision)
}

// IsNullable implements the sql.Expression interface.
func (ut *UTCTimestamp) IsNullable(ctx *sql.Context) bool { return false }

// Resolved implements the sql.Expression interface.
func (ut *UTCTimestamp) Resolved() bool { return true }

// Children implements the sql.Expression interface.
func (ut *UTCTimestamp) Children() []sql.Expression { return nil }

// Eval implements the sql.Expression interface.
func (ut *UTCTimestamp) Eval(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	t := ctx.QueryTime()
	// TODO: UTC Timestamp needs to also handle precision arguments
	nano := 1000 * (t.Nanosecond() / 1000)
	tt := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), nano, t.Location())
	return tt.UTC(), nil
}

// WithChildren implements the Expression interface.
func (ut *UTCTimestamp) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewUTCTimestamp(ctx, children...)
}
