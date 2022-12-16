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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// Concat joins several strings together.
type Concat struct {
	args []sql.Expression
}

var _ sql.FunctionExpression = (*Concat)(nil)

// NewConcat creates a new Concat UDF.
func NewConcat(args ...sql.Expression) (sql.Expression, error) {
	if len(args) == 0 {
		return nil, sql.ErrInvalidArgumentNumber.New("CONCAT", "1 or more", 0)
	}

	return &Concat{args}, nil
}

// FunctionName implements sql.FunctionExpression
func (c *Concat) FunctionName() string {
	return "concat"
}

// Description implements sql.FunctionExpression
func (c *Concat) Description() string {
	return "concatenates any group of fields into a single string."
}

// Type implements the Expression interface.
func (c *Concat) Type() sql.Type { return sql.LongText }

// IsNullable implements the Expression interface.
func (c *Concat) IsNullable() bool {
	for _, arg := range c.args {
		if arg.IsNullable() {
			return true
		}
	}
	return false
}

func (c *Concat) String() string {
	var args = make([]string, len(c.args))
	for i, arg := range c.args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("concat(%s)", strings.Join(args, ", "))
}

func (c *Concat) DebugString() string {
	var args = make([]string, len(c.args))
	for i, arg := range c.args {
		args[i] = sql.DebugString(arg)
	}
	return fmt.Sprintf("concat(%s)", strings.Join(args, ", "))
}

// WithChildren implements the Expression interface.
func (*Concat) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewConcat(children...)
}

// Resolved implements the Expression interface.
func (c *Concat) Resolved() bool {
	for _, arg := range c.args {
		if !arg.Resolved() {
			return false
		}
	}
	return true
}

// Children implements the Expression interface.
func (c *Concat) Children() []sql.Expression { return c.args }

// Eval implements the Expression interface.
func (c *Concat) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	var parts []string

	for _, arg := range c.args {
		val, err := arg.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		if val == nil {
			return nil, nil
		}

		val, err = sql.LongText.Convert(val)
		if err != nil {
			return nil, err
		}

		parts = append(parts, val.(string))
	}

	return strings.Join(parts, ""), nil
}
