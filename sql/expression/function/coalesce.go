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
	"github.com/dolthub/go-mysql-server/sql/expression"
"strings"

	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/go-mysql-server/sql"
)

// Coalesce returns the first non-NULL value in the list, or NULL if there are no non-NULL values.
type Coalesce struct {
	args []sql.Expression
}

var _ sql.FunctionExpression = (*Coalesce)(nil)
var _ sql.CollationCoercible = (*Coalesce)(nil)

// NewCoalesce creates a new Coalesce sql.Expression.
func NewCoalesce(args ...sql.Expression) (sql.Expression, error) {
	if len(args) == 0 {
		return nil, sql.ErrInvalidArgumentNumber.New("COALESCE", "1 or more", 0)
	}

	return &Coalesce{args}, nil
}

// FunctionName implements sql.FunctionExpression
func (c *Coalesce) FunctionName() string {
	return "coalesce"
}

// Description implements sql.FunctionExpression
func (c *Coalesce) Description() string {
	return "returns the first non-null value in a list."
}

// Type implements the sql.Expression interface.
// The return type of Type() is the aggregated type of the argument types.
func (c *Coalesce) Type() sql.Type {
	typ := types.Null
	for _, arg := range c.args {
		if arg == nil {
			continue
		}
		t := arg.Type()
		// special case for signed and unsigned integers
		if (types.IsSigned(typ) && types.IsUnsigned(t)) || (types.IsUnsigned(typ) && types.IsSigned(t)) {
			typ = types.MustCreateDecimalType(20, 0)
			continue
		}

		if t != nil && t != types.Null {
			convType := expression.GetConvertToType(typ, t)
			switch convType {
			case expression.ConvertToChar:
				// Can't get any larger than this
				return types.LongText
			case expression.ConvertToDecimal:
				if typ == types.Float64 || t == types.Float64 {
					typ = types.Float64
				} else if types.IsDecimal(t) {
					typ = t
				} else if !types.IsDecimal(typ) {
					typ = types.MustCreateDecimalType(10, 0)
				}
			case expression.ConvertToUnsigned:
				if typ == types.Uint64 || t == types.Uint64 {
					typ = types.Uint64
				} else {
					typ = types.Uint32
				}
			case expression.ConvertToSigned:
				if typ == types.Int64 || t == types.Int64 {
					typ = types.Int64
				} else {
					typ = types.Int32
				}
			case expression.ConvertToFloat:
				if typ == types.Float64 || t == types.Float64 {
					typ = types.Float64
				} else {
					typ = types.Float32
				}
			default:
			}
		}
	}

	return typ
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (c *Coalesce) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	// Preferably, this would be done during evaluation, but that's not possible with the current abstraction
	if typ := c.Type(); typ != nil {
		return typ.CollationCoercibility(ctx)
	}
	return sql.Collation_binary, 6
}

// IsNullable implements the sql.Expression interface.
// Returns true if all arguments are nil
// or of the first non-nil argument is nullable, otherwise false.
func (c *Coalesce) IsNullable() bool {
	for _, arg := range c.args {
		if arg == nil {
			continue
		}
		return arg.IsNullable()
	}
	return true
}

func (c *Coalesce) String() string {
	var args = make([]string, len(c.args))
	for i, arg := range c.args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("%s(%s)", c.FunctionName(), strings.Join(args, ","))
}

func (c *Coalesce) DebugString() string {
	var args = make([]string, len(c.args))
	for i, arg := range c.args {
		args[i] = sql.DebugString(arg)
	}
	return fmt.Sprintf("%s(%s)", c.FunctionName(), strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (*Coalesce) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewCoalesce(children...)
}

// Resolved implements the sql.Expression interface.
// The function checks if first non-nil argument is resolved.
func (c *Coalesce) Resolved() bool {
	for _, arg := range c.args {
		if arg == nil {
			continue
		}
		if !arg.Resolved() {
			return false
		}
	}
	return true
}

// Children implements the sql.Expression interface.
func (c *Coalesce) Children() []sql.Expression { return c.args }

// Eval implements the sql.Expression interface.
// The function evaluates the first non-nil argument. If the value is nil,
// then we keep going, otherwise we return the first non-nil value.
func (c *Coalesce) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	for _, arg := range c.args {
		if arg == nil {
			continue
		}

		val, err := arg.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		if val == nil {
			continue
		}

		return val, nil
	}

	return nil, nil
}
