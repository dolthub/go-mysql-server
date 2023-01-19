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

package expression

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// AliasReference is a named reference to an aliased expression.
type AliasReference struct {
	name string
}

// NewAliasReference creates a new AliasReference from the specified alias name.
func NewAliasReference(name string) *AliasReference {
	return &AliasReference{name}
}

func (a AliasReference) Name() string {
	return a.name
}

func (a AliasReference) Table() string {
	return ""
}

func (a AliasReference) String() string {
	return fmt.Sprintf("(alias reference)%s", a.name)
}

func (a AliasReference) Resolved() bool {
	return false
}

func (a AliasReference) IsNullable() bool {
	return true
}

func (a AliasReference) Children() []sql.Expression {
	return []sql.Expression{}
}

func (a AliasReference) Type() sql.Type {
	return types.Null
}

func (a AliasReference) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return nil, fmt.Errorf("tried to call eval on an unresolved AliasReference")
}

func (a AliasReference) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 0)
	}
	return NewAliasReference(a.name), nil
}

var _ sql.Expression = (*AliasReference)(nil)

// Alias is a node that gives a name to an expression.
type Alias struct {
	UnaryExpression
	name string
}

// NewAlias returns a new Alias node.
func NewAlias(name string, expr sql.Expression) *Alias {
	return &Alias{UnaryExpression{expr}, name}
}

// Type returns the type of the expression.
func (e *Alias) Type() sql.Type {
	return e.Child.Type()
}

// Eval implements the Expression interface.
func (e *Alias) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return e.Child.Eval(ctx, row)
}

func (e *Alias) String() string {
	return fmt.Sprintf("%s as %s", e.Child, e.name)
}

func (e *Alias) DebugString() string {
	return fmt.Sprintf("%s as %s", sql.DebugString(e.Child), e.name)
}

// WithChildren implements the Expression interface.
func (e *Alias) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	return NewAlias(e.name, children[0]), nil
}

// Name implements the Nameable interface.
func (e *Alias) Name() string { return e.name }
