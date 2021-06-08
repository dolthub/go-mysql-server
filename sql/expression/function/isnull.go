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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// IsNull is a function that returns whether a value is null or not.
type IsNull struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*IsNull)(nil)

// NewIsNull creates a new IsNull expression.
func NewIsNull(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &IsNull{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (ib *IsNull) FunctionName() string {
	return "isnull"
}

// Eval implements the Expression interface.
func (ib *IsNull) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	v, err := ib.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if v != nil {
		return false, nil
	}

	return true, nil
}

func (ib *IsNull) String() string {
	return fmt.Sprintf("ISNULL(%s)", ib.Child)
}

// WithChildren implements the Expression interface.
func (ib *IsNull) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(ib, len(children), 1)
	}
	return NewIsNull(ctx, children[0]), nil
}

// Type implements the Expression interface.
func (ib *IsNull) Type() sql.Type {
	return sql.Boolean
}
