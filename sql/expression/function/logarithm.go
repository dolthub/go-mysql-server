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
	"math"
	"reflect"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ErrInvalidArgumentForLogarithm is returned when an invalid argument value is passed to a
// logarithm function
var ErrInvalidArgumentForLogarithm = errors.NewKind("invalid argument value for logarithm: %v")

// NewLogBaseFunc returns LogBase creator function with a specific base.
func NewLogBaseFunc(base float64) func(e sql.Expression) sql.Expression {
	return func(e sql.Expression) sql.Expression {
		return NewLogBase(base, e)
	}
}

// LogBase is a function that returns the logarithm of a value with a specific base.
type LogBase struct {
	expression.UnaryExpression
	base float64
}

var _ sql.FunctionExpression = (*LogBase)(nil)
var _ sql.CollationCoercible = (*LogBase)(nil)

// NewLogBase creates a new LogBase expression.
func NewLogBase(base float64, e sql.Expression) sql.Expression {
	return &LogBase{UnaryExpression: expression.UnaryExpression{Child: e}, base: base}
}

// FunctionName implements sql.FunctionExpression
func (l *LogBase) FunctionName() string {
	switch l.base {
	case float64(math.E):
		return "ln"
	case float64(10):
		return "log10"
	case float64(2):
		return "log2"
	default:
		return "log"
	}
}

// Description implements sql.FunctionExpression
func (l *LogBase) Description() string {
	switch l.base {
	case float64(math.E):
		return "returns the natural logarithm of X."
	case float64(10):
		return "returns the base-10 logarithm of X."
	case float64(2):
		return "returns the base-2 logarithm of X."
	default:
		return "if called with one parameter, this function returns the natural logarithm of X. If called with two parameters, this function returns the logarithm of X to the base B. If X is less than or equal to 0, or if B is less than or equal to 1, then NULL is returned."
	}
}

func (l *LogBase) String() string {
	switch l.base {
	case float64(math.E):
		return fmt.Sprintf("ln(%s)", l.Child)
	case float64(10):
		return fmt.Sprintf("log10(%s)", l.Child)
	case float64(2):
		return fmt.Sprintf("log2(%s)", l.Child)
	default:
		return fmt.Sprintf("log(%v, %s)", l.base, l.Child)
	}
}

// WithChildren implements the Expression interface.
func (l *LogBase) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 1)
	}
	return NewLogBase(l.base, children[0]), nil
}

// Type returns the resultant type of the function.
func (l *LogBase) Type() sql.Type {
	return types.Float64
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*LogBase) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// IsNullable implements the sql.Expression interface.
func (l *LogBase) IsNullable() bool {
	return l.base == float64(1) || l.base <= float64(0) || l.Child.IsNullable()
}

// Eval implements the Expression interface.
func (l *LogBase) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	v, err := l.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, nil
	}

	val, err := types.Float64.Convert(v)
	if err != nil {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(v))
	}
	return computeLog(val.(float64), l.base)
}

// Log is a function that returns the natural logarithm of a value.
type Log struct {
	expression.BinaryExpression
}

var _ sql.FunctionExpression = (*Log)(nil)
var _ sql.CollationCoercible = (*Log)(nil)

// NewLog creates a new Log expression.
func NewLog(args ...sql.Expression) (sql.Expression, error) {
	argLen := len(args)
	if argLen == 0 || argLen > 2 {
		return nil, sql.ErrInvalidArgumentNumber.New("LOG", "1 or 2", argLen)
	}

	if argLen == 1 {
		return &Log{expression.BinaryExpression{Left: expression.NewLiteral(math.E, types.Float64), Right: args[0]}}, nil
	} else {
		return &Log{expression.BinaryExpression{Left: args[0], Right: args[1]}}, nil
	}
}

// FunctionName implements sql.FunctionExpression
func (l *Log) FunctionName() string {
	return "log"
}

// Description implements sql.FunctionExpression
func (l *Log) Description() string {
	return "if called with one parameter, this function returns the natural logarithm of X. If called with two parameters, this function returns the logarithm of X to the base B. If X is less than or equal to 0, or if B is less than or equal to 1, then NULL is returned."
}

func (l *Log) String() string {
	return fmt.Sprintf("%s(%s,%s)", l.FunctionName(), l.Left, l.Right)
}

// WithChildren implements the Expression interface.
func (l *Log) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewLog(children...)
}

// Children implements the Expression interface.
func (l *Log) Children() []sql.Expression {
	return []sql.Expression{l.Left, l.Right}
}

// Type returns the resultant type of the function.
func (l *Log) Type() sql.Type {
	return types.Float64
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Log) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// IsNullable implements the Expression interface.
func (l *Log) IsNullable() bool {
	return l.Left.IsNullable() || l.Right.IsNullable()
}

// Eval implements the Expression interface.
func (l *Log) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	left, err := l.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if left == nil {
		return nil, nil
	}

	lhs, err := types.Float64.Convert(left)
	if err != nil {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(left))
	}

	right, err := l.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if right == nil {
		return nil, nil
	}

	rhs, err := types.Float64.Convert(right)
	if err != nil {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(right))
	}

	// rhs becomes value, lhs becomes base
	return computeLog(rhs.(float64), lhs.(float64))
}

func computeLog(v float64, base float64) (float64, error) {
	if v <= 0 {
		return float64(0), ErrInvalidArgumentForLogarithm.New(v)
	}
	if base == float64(1) || base <= float64(0) {
		return float64(0), ErrInvalidArgumentForLogarithm.New(base)
	}
	switch base {
	case float64(2):
		return math.Log2(v), nil
	case float64(10):
		return math.Log10(v), nil
	case math.E:
		return math.Log(v), nil
	default:
		// LOG(BASE,V) is equivalent to LOG(V) / LOG(BASE).
		return float64(math.Log(v) / math.Log(base)), nil
	}
}
