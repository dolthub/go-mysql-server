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
	"github.com/dolthub/go-mysql-server/sql"
)

// DefaultColumn is an default expression of a column that is not yet resolved.
type DefaultColumn struct {
	name string
}

// NewDefaultColumn creates a new NewDefaultColumn expression.
func NewDefaultColumn(name string) *DefaultColumn {
	return &DefaultColumn{name: name}
}

// Children implements the sql.Expression interface.
// The function returns always nil
func (*DefaultColumn) Children() []sql.Expression {
	return nil
}

// Resolved implements the sql.Expression interface.
// The function returns always false
func (*DefaultColumn) Resolved() bool {
	return false
}

// IsNullable implements the sql.Expression interface.
// The function always panics!
func (*DefaultColumn) IsNullable() bool {
	panic("default column is a placeholder node, but IsNullable was called")
}

// Type implements the sql.Expression interface.
// The function always panics!
func (*DefaultColumn) Type() sql.Type {
	panic("default column is a placeholder node, but Type was called")
}

// Name implements the sql.Nameable interface.
func (c *DefaultColumn) Name() string { return c.name }

// String implements the Stringer
// The function returns column's name (can be an empty string)
func (c *DefaultColumn) String() string {
	return c.name
}

// Eval implements the sql.Expression interface.
// The function always panics!
func (*DefaultColumn) Eval(ctx *sql.Context, r sql.Row) (interface{}, error) {
	panic("default column is a placeholder node, but Eval was called")
}

// WithChildren implements the Expression interface.
func (c *DefaultColumn) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 0)
	}
	return c, nil
}
