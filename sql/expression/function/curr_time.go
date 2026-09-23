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
	"fmt"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// CurrTime is a function that returns the current time.
type CurrTime struct {
	prec sql.Expression
}

// IsNonDeterministic implements the sql.NonDeterministicExpression interface.
func (c *CurrTime) IsNonDeterministic() bool {
	return true
}

var _ sql.FunctionExpression = (*CurrTime)(nil)
var _ sql.CollationCoercible = (*CurrTime)(nil)

// NewCurrTime returns a new CurrTime node.
func NewCurrTime(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	c := &CurrTime{}
	// parser should make it impossible to pass in more than one argument
	if len(args) > 0 {
		c.prec = args[0]
	}
	return c, nil
}

// Name implements sql.FunctionExpression
func (c *CurrTime) Name() string {
	return "current_time"
}

// Description implements sql.FunctionExpression
func (c *CurrTime) Description() string {
	return "returns the current time."
}

// Type implements the sql.Expression interface.
func (c *CurrTime) Type(ctx *sql.Context) sql.Type {
	return types.Time
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*CurrTime) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// String implements the sql.Expression interface.
func (c *CurrTime) String() string {
	if c.prec == nil {
		return "CURRENT_TIME()"
	}

	return fmt.Sprintf("CURRENT_TIME(%s)", c.prec.String())
}

// IsNullable implements the sql.Expression interface.
func (c *CurrTime) IsNullable(ctx *sql.Context) bool { return false }

// Resolved implements the sql.Expression interface.
func (c *CurrTime) Resolved() bool {
	if c.prec == nil {
		return true
	}
	return c.prec.Resolved()
}

// Children implements the sql.Expression interface.
func (c *CurrTime) Children() []sql.Expression {
	if c.prec == nil {
		return nil
	}
	return []sql.Expression{c.prec}
}

// Eval implements sql.Expression
func (c *CurrTime) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	newNow, err := NewNow(ctx, c.prec)
	if err != nil {
		return nil, err
	}

	result, err := newNow.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if t, ok := result.(time.Time); ok {
		precision := 0
		if c.prec != nil {
			prec, err := c.prec.Eval(ctx, row)
			if err != nil {
				return nil, err
			}
			fsp, ok := types.CoalesceInt(prec)
			if !ok {
				return nil, sql.ErrInvalidArgumentType.New(c.Name())
			}
			precision = int(fsp)
		}
		return fmt.Sprintf("%02d:%02d:%02d%s", t.Hour(), t.Minute(), t.Second(), subSecondPrecision(t, precision)), nil
	} else {
		return nil, fmt.Errorf("unexpected type %T for NOW() result", result)
	}
}

// WithChildren implements sql.Expression
func (c *CurrTime) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewCurrTime(ctx, children...)
}
