// Copyright 2024 Dolthub, Inc.
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

// Char implements the sql function "char" which returns the character for each integer passed
type Char struct {
	// TODO: support using (charset/collation) clause
	args []sql.Expression
}

var _ sql.FunctionExpression = (*Char)(nil)
var _ sql.CollationCoercible = (*Char)(nil)

func NewChar(args ...sql.Expression) (sql.Expression, error) {
	return &Char{args: args}, nil
}

// FunctionName implements sql.FunctionExpression
func (c *Char) FunctionName() string {
	return "char"
}

// Resolved implements sql.FunctionExpression
func (c *Char) Resolved() bool {
	for _, arg := range c.args {
		if !arg.Resolved() {
			return false
		}
	}
	return true
}

// String implements sql.Expression
func (c *Char) String() string {
	args := make([]string, len(c.args))
	for i, arg := range c.args {
		args[i] = arg.String()
	}
	str := strings.Join(args, ", ")
	return fmt.Sprintf("%s(%s)", c.FunctionName(), str)
}

// Type implements sql.Expression
func (c *Char) Type() sql.Type {
	return types.LongBlob
}

// IsNullable implements sql.Expression
func (c *Char) IsNullable() bool {
	return true
}

// Description implements sql.FunctionExpression
func (c *Char) Description() string {
	return "returns the numeric value of the leftmost character."
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (c *Char) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func char(num uint32) []byte {
	res := []byte{}
	for num > 0 {
		res = append([]byte{byte(num % 256)}, res...)
		num = num / 256
	}
	return res
}

// Eval implements the sql.Expression interface
func (c *Char) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	res := []byte{}
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

		v, _, err := types.Uint32.Convert(val)
		if err != nil {
			ctx.Warn(1292, "Truncated incorrect INTEGER value: '%v'", val)
			res = append(res, 0)
			continue
		}

		res = append(res, char(v.(uint32))...)
	}

	return res, nil
}

// Children implements sql.Expression
func (c *Char) Children() []sql.Expression {
	return c.args
}

// WithChildren implements the sql.Expression interface
func (c *Char) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewChar(children...)
}