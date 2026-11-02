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

package expression

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// BitNot complements each bit of a number or binary string.
type BitNot struct {
	UnaryExpressionStub
	numericLiteral bool
}

var _ sql.Expression = (*BitNot)(nil)
var _ sql.CollationCoercible = (*BitNot)(nil)

// NewBitNot creates a unary bitwise complement expression.
func NewBitNot(child sql.Expression) *BitNot {
	return &BitNot{UnaryExpressionStub: UnaryExpressionStub{Child: child}}
}

// NewBitNotNumeric creates a complement that evaluates a hexadecimal literal as a number.
func NewBitNotNumeric(child sql.Expression) *BitNot {
	return &BitNot{UnaryExpressionStub: UnaryExpressionStub{Child: child}, numericLiteral: true}
}

// Type implements sql.Expression.
func (b *BitNot) Type(ctx *sql.Context) sql.Type {
	typ := b.Child.Type(ctx)
	if types.IsDeferredType(typ) {
		return typ
	}
	if str, ok := typ.(sql.StringType); ok && !b.numericLiteral && str.CharacterSet() == sql.CharacterSet_binary {
		return types.LongBlob
	}
	return types.Uint64
}

// CollationCoercibility implements sql.CollationCoercible.
func (*BitNot) CollationCoercibility(ctx *sql.Context) (sql.CollationID, byte) {
	return sql.Collation_binary, 5
}

// String implements sql.Expression.
func (b *BitNot) String() string {
	return fmt.Sprintf("~%s", b.Child)
}

// DebugString implements sql.DebugStringer.
func (b *BitNot) DebugString(ctx *sql.Context) string {
	return fmt.Sprintf("~%s", sql.DebugString(ctx, b.Child))
}

// WithChildren implements sql.Expression.
func (b *BitNot) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(b, len(children), 1)
	}
	return &BitNot{UnaryExpressionStub: UnaryExpressionStub{Child: children[0]}, numericLiteral: b.numericLiteral}, nil
}

// Eval implements sql.Expression.
func (b *BitNot) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := b.Child.Eval(ctx, row)
	if err != nil || val == nil {
		return nil, err
	}
	if b.Type(ctx) == types.LongBlob {
		binary, _, err := types.LongBlob.Convert(ctx, val)
		if err != nil {
			return nil, err
		}
		bytes := binary.([]byte)
		result := make([]byte, len(bytes))
		for i, value := range bytes {
			result[i] = ^value
		}
		return result, nil
	}

	// MySQL rounds DECIMAL operands as signed integers, but truncates DOUBLE.
	typ := b.Child.Type(ctx)
	if types.IsDecimal(typ) {
		converted, inRange, err := types.Int64.Convert(ctx, val)
		if err != nil || inRange != sql.InRange {
			ctx.Warn(mysql.ERTruncatedWrongValue, "Truncated incorrect DECIMAL value: '%v'", val)
		}
		if err != nil {
			return nil, err
		}
		return ^uint64(converted.(int64)), nil
	}
	if timespan, ok := val.(types.Timespan); ok {
		// Round to whole seconds before forming HHMMSS so minute carries are retained.
		seconds := int64(math.Round(float64(timespan.AsMicroseconds()) / 1e6))
		numeric := seconds/3600*10000 + seconds/60%60*100 + seconds%60
		return ^uint64(numeric), nil
	}
	if datetime, ok := val.(time.Time); ok {
		// Round the temporal value before packing its digits to preserve calendar carries.
		val = datetime.Round(time.Second)
	}
	if types.IsFloat(typ) {
		converted := convertValueToType(ctx, val, typ, types.Float64).(float64)
		if math.IsNaN(converted) || converted >= float64(math.MaxInt64) || converted <= float64(math.MinInt64) {
			return nil, sql.ErrIntegerOutOfRange.New("BIGINT", b.String())
		}
		return ^uint64(int64(converted)), nil
	}

	// Negative strings use signed saturation, while positive strings allow all 64 bits.
	if text, ok := val.(string); ok && strings.HasPrefix(strings.TrimSpace(text), "-") {
		prefix, truncated := types.TruncateStringToInt(text)
		converted, err := strconv.ParseInt(prefix, 10, 64)
		if err != nil || truncated {
			ctx.Warn(mysql.ERTruncatedWrongValue, "Truncated incorrect INTEGER value: '%s'", text)
		}
		return ^uint64(converted), nil
	}
	if binary, ok := val.([]byte); ok && len(binary) == 0 {
		return uint64(math.MaxUint64), nil
	}
	converted, inRange, err := types.TypeAwareConversion(ctx, val, typ, types.Uint64)
	if err != nil || inRange == sql.Overflow {
		kind := "INTEGER"
		if b.numericLiteral {
			kind = "BINARY"
		}
		ctx.Warn(mysql.ERTruncatedWrongValue, "Truncated incorrect %s value: '%v'", kind, val)
	}
	if converted == nil {
		return uint64(math.MaxUint64), nil
	}
	return ^converted.(uint64), nil
}
