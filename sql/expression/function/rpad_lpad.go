// Copyright 2020-2026 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// LeftPad represents the LPAD function, which returns a string
// left-padded with a pad string to a specified character length.
type LeftPad struct {
	pad
}

var _ sql.FunctionExpression = (*LeftPad)(nil)
var _ sql.CollationCoercible = (*LeftPad)(nil)

// NewLeftPad creates a new LeftPad expression with the
// given arguments.
func NewLeftPad(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	if len(args) != 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("lpad", "3", len(args))
	}
	return &LeftPad{pad: pad{str: args[0], length: args[1], padStr: args[2]}}, nil
}

// FunctionName implements [sql.FunctionExpression].
func (l *LeftPad) FunctionName() string {
	return "lpad"
}

// Description implements [sql.FunctionExpression].
func (l *LeftPad) Description() string {
	return "returns the string str, left-padded with the string padstr to a length of len characters."
}

func (l *LeftPad) String() string {
	return fmt.Sprintf("lpad(%s, %s, %s)", l.str, l.length, l.padStr)
}

// WithChildren implements [sql.Expression].
func (l *LeftPad) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewLeftPad(ctx, children...)
}

// Eval implements [sql.Expression].
func (l *LeftPad) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return l.eval(ctx, row, true)
}

// RightPad represents the RPAD function, which returns a string
// right-padded with a pad string to a specified character length.
type RightPad struct {
	pad
}

var _ sql.FunctionExpression = (*RightPad)(nil)
var _ sql.CollationCoercible = (*RightPad)(nil)

// NewRightPad creates a new RightPad expression with the
// given arguments.
func NewRightPad(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	if len(args) != 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("rpad", "3", len(args))
	}
	return &RightPad{pad: pad{str: args[0], length: args[1], padStr: args[2]}}, nil
}

// FunctionName implements [sql.FunctionExpression].
func (r *RightPad) FunctionName() string {
	return "rpad"
}

// Description implements [sql.FunctionExpression].
func (r *RightPad) Description() string {
	return "returns the string str, right-padded with the string padstr to a length of len characters."
}

func (r *RightPad) String() string {
	return fmt.Sprintf("rpad(%s, %s, %s)", r.str, r.length, r.padStr)
}

// WithChildren implements [sql.Expression].
func (r *RightPad) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewRightPad(ctx, children...)
}

// Eval implements [sql.Expression].
func (r *RightPad) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return r.eval(ctx, row, false)
}

// pad is the base expression for LPAD and RPAD string padding
// functions.
type pad struct {
	str    sql.Expression
	length sql.Expression
	padStr sql.Expression
}

// Children implements [sql.Expression].
func (p *pad) Children() []sql.Expression {
	return []sql.Expression{p.str, p.length, p.padStr}
}

// Resolved implements [sql.Expression].
func (p *pad) Resolved() bool {
	return p.str.Resolved() && p.length.Resolved() && p.padStr.Resolved()
}

// IsNullable implements [sql.Expression].
func (p *pad) IsNullable(ctx *sql.Context) bool {
	return p.str.IsNullable(ctx) || p.length.IsNullable(ctx) || p.padStr.IsNullable(ctx)
}

// Type implements [sql.Expression].
func (p *pad) Type(ctx *sql.Context) sql.Type {
	return types.LongTextForType(p.str.Type(ctx))
}

// CollationCoercibility implements [sql.CollationCoercible]. It
// returns the collation and coercibility of the string expression,
// deriving them solely from the first argument.
func (p *pad) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.ResolveCoercibilityExpressions(ctx, p.str)
}

// eval pads |p.str| on the left when |isLeft| is true, or on the right
// otherwise, to |count| characters using |p.padStr|. It returns NULL and
// records warning 1301 when the result would exceed @@max_allowed_packet.
func (p *pad) eval(ctx *sql.Context, row sql.Row, isLeft bool) (interface{}, error) {
	collation, _ := p.CollationCoercibility(ctx)

	s, ok, err := evalString(ctx, p.str, row, collation)
	if err != nil || !ok {
		return nil, err
	}

	count, ok, err := evalInt64(ctx, p.length, row)
	if err != nil || !ok || count < 0 {
		return nil, err
	}
	if count == 0 {
		return "", nil
	}

	ps, ok, err := evalString(ctx, p.padStr, row, collation)
	if err != nil || !ok {
		return nil, err
	}

	maxPacket, err := maxAllowedPacket(ctx)
	if err != nil {
		return nil, err
	}
	result, fits, err := padString(s, count, ps, isLeft, collation, maxPacket)
	if err != nil {
		return nil, err
	}
	if !fits {
		funcName := "rpad"
		if isLeft {
			funcName = "lpad"
		}
		warnMaxAllowedPacket(ctx, funcName, maxPacket)
		return nil, nil
	}
	return result, nil
}

// padString pads |str| on the left when |isLeft| is true, or on the right
// otherwise, with |padStr| to |targetLen| characters. It reports whether the
// result fits within |maxPacket| bytes.
func padString(str string, targetLen int64, padStr string, isLeft bool, collation sql.CollationID, maxPacket int64) (string, bool, error) {
	if targetLen <= 0 {
		return "", true, nil
	}
	handler := sql.NewCharSetHandler(collation)
	resCharLen, err := handler.NumChars(str)
	if err != nil {
		return "", false, err
	}
	if int64(resCharLen) >= targetLen {
		pos, err := handler.CharPos(str, int(targetLen))
		if err != nil {
			return "", false, err
		}
		return str[:pos], true, nil
	}

	padCharLen, err := handler.NumChars(padStr)
	if err != nil {
		return "", false, err
	}

	// The result's worst-case byte length assumes every padding character
	// occupies the character set's maximum byte length. The result is compared
	// against max_allowed_packet before it is built.
	remainderCharLen := targetLen - int64(resCharLen)
	if resultExceedsMaxPacket(int64(len(str)), remainderCharLen, collation.CharacterSet().MaxLength(), maxPacket) {
		return "", false, nil
	}

	if padCharLen == 0 {
		return "", true, nil
	}

	quo := remainderCharLen / int64(padCharLen)
	rem := remainderCharLen % int64(padCharLen)

	var b strings.Builder
	if !isLeft {
		b.WriteString(str)
	}
	for i := int64(0); i < quo; i++ {
		b.WriteString(padStr)
	}
	if rem > 0 {
		pos, err := handler.CharPos(padStr, int(rem))
		if err != nil {
			return "", false, err
		}
		b.WriteString(padStr[:pos])
	}
	if isLeft {
		b.WriteString(str)
	}
	return b.String(), true, nil
}

// resultExceedsMaxPacket reports whether a result of |baseBytes| bytes plus
// |extraChars| characters of at most |maxCharBytes| bytes each is larger than
// |maxPacket|. It uses division so that |extraChars| cannot overflow.
func resultExceedsMaxPacket(baseBytes, extraChars, maxCharBytes, maxPacket int64) bool {
	if baseBytes > maxPacket {
		return true
	}
	if maxCharBytes <= 0 {
		maxCharBytes = 1
	}
	return extraChars > (maxPacket-baseBytes)/maxCharBytes
}
