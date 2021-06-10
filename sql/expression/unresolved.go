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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// UnresolvedColumn is an expression of a column that is not yet resolved.
// This is a placeholder node, so its methods Type, IsNullable and Eval are not
// supposed to be called.
type UnresolvedColumn struct {
	name  string
	table string
}

// NewUnresolvedColumn creates a new UnresolvedColumn expression.
func NewUnresolvedColumn(name string) *UnresolvedColumn {
	return &UnresolvedColumn{name: name}
}

// NewUnresolvedQualifiedColumn creates a new UnresolvedColumn expression
// with a table qualifier.
func NewUnresolvedQualifiedColumn(table, name string) *UnresolvedColumn {
	return &UnresolvedColumn{name: name, table: table}
}

// Children implements the Expression interface.
func (*UnresolvedColumn) Children() []sql.Expression {
	return nil
}

// Resolved implements the Expression interface.
func (*UnresolvedColumn) Resolved() bool {
	return false
}

// IsNullable implements the Expression interface.
func (*UnresolvedColumn) IsNullable() bool {
	panic("unresolved column is a placeholder node, but IsNullable was called")
}

// Type implements the Expression interface.
func (*UnresolvedColumn) Type() sql.Type {
	panic("unresolved column is a placeholder node, but Type was called")
}

// Name implements the Nameable interface.
func (uc *UnresolvedColumn) Name() string { return uc.name }

// Table returns the table name.
func (uc *UnresolvedColumn) Table() string { return uc.table }

func (uc *UnresolvedColumn) String() string {
	if uc.table == "" {
		return uc.name
	}
	return fmt.Sprintf("%s.%s", uc.table, uc.name)
}

// Eval implements the Expression interface.
func (*UnresolvedColumn) Eval(ctx *sql.Context, r sql.Row) (interface{}, error) {
	panic("unresolved column is a placeholder node, but Eval was called")
}

// WithChildren implements the Expression interface.
func (uc *UnresolvedColumn) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(uc, len(children), 0)
	}
	return uc, nil
}

// UnresolvedFunction represents a function that is not yet resolved.
// This is a placeholder node, so its methods Type, IsNullable and Eval are not
// supposed to be called.
type UnresolvedFunction struct {
	name string
	// IsAggregate or not.
	IsAggregate bool
	// Window is the window for this function, if present
	Window *sql.Window
	// Children of the expression.
	Arguments []sql.Expression
}

// NewUnresolvedFunction creates a new UnresolvedFunction expression.
func NewUnresolvedFunction(
	name string,
	agg bool,
	window *sql.Window,
	arguments ...sql.Expression,
) *UnresolvedFunction {
	return &UnresolvedFunction{
		name:        name,
		IsAggregate: agg,
		Window:      window,
		Arguments:   arguments,
	}
}

// Children implements the Expression interface.
func (uf *UnresolvedFunction) Children() []sql.Expression {
	return append(uf.Arguments, uf.Window.ToExpressions()...)
}

// Resolved implements the Expression interface.
func (*UnresolvedFunction) Resolved() bool {
	return false
}

// IsNullable implements the Expression interface.
func (*UnresolvedFunction) IsNullable() bool {
	panic("unresolved function is a placeholder node, but IsNullable was called")
}

// Type implements the Expression interface.
func (*UnresolvedFunction) Type() sql.Type {
	panic("unresolved function is a placeholder node, but Type was called")
}

// Name implements the Nameable interface.
func (uf *UnresolvedFunction) Name() string { return uf.name }

func (uf *UnresolvedFunction) String() string {
	var exprs = make([]string, len(uf.Arguments))
	for i, e := range uf.Arguments {
		exprs[i] = e.String()
	}

	over := ""
	if uf.Window != nil {
		over = fmt.Sprintf(" %s", uf.Window)
	}

	return fmt.Sprintf("%s(%s)%s", uf.name, strings.Join(exprs, ", "), over)
}

func (uf *UnresolvedFunction) DebugString() string {
	var exprs = make([]string, len(uf.Arguments))
	for i, e := range uf.Arguments {
		exprs[i] = sql.DebugString(e)
	}

	over := ""
	if uf.Window != nil {
		over = fmt.Sprintf(" %s", sql.DebugString(uf.Window))
	}

	return fmt.Sprintf("%s(%s)%s", uf.name, strings.Join(exprs, ", "), over)
}

// Eval implements the Expression interface.
func (*UnresolvedFunction) Eval(ctx *sql.Context, r sql.Row) (interface{}, error) {
	panic("unresolved function is a placeholder node, but Eval was called")
}

// WithChildren implements the Expression interface.
func (uf *UnresolvedFunction) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != len(uf.Arguments)+len(uf.Window.ToExpressions()) {
		return nil, sql.ErrInvalidChildrenNumber.New(uf, len(children), len(uf.Arguments)+len(uf.Window.ToExpressions()))
	}

	window, err := uf.Window.FromExpressions(children[len(uf.Arguments):])
	if err != nil {
		return nil, err
	}

	return NewUnresolvedFunction(uf.name, uf.IsAggregate, window, children[:len(uf.Arguments)]...), nil
}
