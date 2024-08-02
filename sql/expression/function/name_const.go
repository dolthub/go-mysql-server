// Copyright 2024 Dolthub, Inn.
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
	)

// NameConst implements the sql function "name_const" which returns the value with an alias
type NameConst struct {
	Alias, Value sql.Expression
	AliasStr string
}

var _ sql.FunctionExpression = (*NameConst)(nil)
var _ sql.CollationCoercible = (*NameConst)(nil)

func NewNameConst(alias, value sql.Expression) sql.Expression {
	return &NameConst{Alias: alias, Value: value}
}

// Name implements sql.Nameable
func (n *NameConst) Name() string {
	return n.AliasStr
}

// FunctionName implements sql.FunctionExpression
func (n *NameConst) FunctionName() string {
	return "name_const"
}

// Resolved implements sql.FunctionExpression
func (n *NameConst) Resolved() bool {
	return n.Alias.Resolved() && n.Value.Resolved()
}

// String implements sql.Expression
func (n *NameConst) String() string {
	return fmt.Sprintf("%s(%s, %s)", n.FunctionName(), n.Alias, n.Value)
}

// Type implements sql.Expression
func (n *NameConst) Type() sql.Type {
	return n.Value.Type()
}

// IsNullable implements sql.Expression
func (n *NameConst) IsNullable() bool {
	return true
}

// Description implements sql.FunctionExpression
func (n *NameConst) Description() string {
	return "returns the value with an alias"
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (n *NameConst) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements the sql.Expression interface
func (n *NameConst) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := n.Value.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	return val, nil
}

// Children implements sql.Expression
func (n *NameConst) Children() []sql.Expression {
	return []sql.Expression{n.Alias, n.Value}
}

// WithChildren implements the sql.Expression interface
func (n *NameConst) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 2)
	}
	return NewNameConst(children[0], children[1]), nil
}
