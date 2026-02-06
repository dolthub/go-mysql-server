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

package plan

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// RowsFrom represents a ROWS FROM table function that executes multiple
// set-returning functions in parallel and zips their results together.
// This is the PostgreSQL-compatible syntax: ROWS FROM(func1(...), func2(...), ...)
type RowsFrom struct {
	// Functions contains the set-returning function expressions to execute
	Functions []sql.Expression
	// WithOrdinality when true, adds an ordinality column to the result
	WithOrdinality bool
	// Alias is the table alias for this ROWS FROM expression
	Alias string
	// ColumnAliases are optional column names for the result columns
	ColumnAliases []string
	// colset tracks the column IDs for this node
	colset sql.ColSet
	// id is the table ID for this node
	id sql.TableId
}

var _ sql.Node = (*RowsFrom)(nil)
var _ sql.Expressioner = (*RowsFrom)(nil)
var _ sql.CollationCoercible = (*RowsFrom)(nil)
var _ TableIdNode = (*RowsFrom)(nil)
var _ sql.RenameableNode = (*RowsFrom)(nil)

// NewRowsFrom creates a new RowsFrom node with the given function expressions.
func NewRowsFrom(functions []sql.Expression, alias string) *RowsFrom {
	return &RowsFrom{
		Functions: functions,
		Alias:     alias,
	}
}

// NewRowsFromWithOrdinality creates a new RowsFrom node with ordinality enabled.
func NewRowsFromWithOrdinality(functions []sql.Expression, alias string) *RowsFrom {
	return &RowsFrom{
		Functions:      functions,
		WithOrdinality: true,
		Alias:          alias,
	}
}

// WithId implements sql.TableIdNode
func (r *RowsFrom) WithId(id sql.TableId) TableIdNode {
	ret := *r
	ret.id = id
	return &ret
}

// Id implements sql.TableIdNode
func (r *RowsFrom) Id() sql.TableId {
	return r.id
}

// WithColumns implements sql.TableIdNode
func (r *RowsFrom) WithColumns(set sql.ColSet) TableIdNode {
	ret := *r
	ret.colset = set
	return &ret
}

// Columns implements sql.TableIdNode
func (r *RowsFrom) Columns() sql.ColSet {
	return r.colset
}

// Name returns the alias name for this ROWS FROM expression
func (r *RowsFrom) Name() string {
	if r.Alias != "" {
		return r.Alias
	}
	return "rows_from"
}

// WithName implements sql.RenameableNode
func (r *RowsFrom) WithName(s string) sql.Node {
	ret := *r
	ret.Alias = s
	return &ret
}

// Schema implements the sql.Node interface.
func (r *RowsFrom) Schema() sql.Schema {
	var schema sql.Schema

	for i, f := range r.Functions {
		colName := fmt.Sprintf("col%d", i)
		if i < len(r.ColumnAliases) && r.ColumnAliases[i] != "" {
			colName = r.ColumnAliases[i]
		} else if nameable, ok := f.(sql.Nameable); ok {
			colName = nameable.Name()
		}

		schema = append(schema, &sql.Column{
			Name:     colName,
			Type:     f.Type(),
			Nullable: true, // SRF results can be NULL when zipping unequal-length results
			Source:   r.Name(),
		})
	}

	if r.WithOrdinality {
		schema = append(schema, &sql.Column{
			Name:     "ordinality",
			Type:     types.Int64,
			Nullable: false,
			Source:   r.Name(),
		})
	}

	return schema
}

// Children implements the sql.Node interface.
func (r *RowsFrom) Children() []sql.Node {
	return nil
}

// Resolved implements the sql.Resolvable interface.
func (r *RowsFrom) Resolved() bool {
	for _, f := range r.Functions {
		if !f.Resolved() {
			return false
		}
	}
	return true
}

// IsReadOnly implements the sql.Node interface.
func (r *RowsFrom) IsReadOnly() bool {
	return true
}

// String implements the sql.Node interface.
func (r *RowsFrom) String() string {
	var sb strings.Builder
	sb.WriteString("ROWS FROM(")
	for i, f := range r.Functions {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(f.String())
	}
	sb.WriteString(")")
	if r.WithOrdinality {
		sb.WriteString(" WITH ORDINALITY")
	}
	if r.Alias != "" {
		sb.WriteString(" AS ")
		sb.WriteString(r.Alias)
	}
	return sb.String()
}

// DebugString implements the sql.DebugStringer interface.
func (r *RowsFrom) DebugString() string {
	var sb strings.Builder
	sb.WriteString("RowsFrom(")
	for i, f := range r.Functions {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(sql.DebugString(f))
	}
	sb.WriteString(")")
	if r.WithOrdinality {
		sb.WriteString(" WITH ORDINALITY")
	}
	if r.Alias != "" {
		sb.WriteString(" AS ")
		sb.WriteString(r.Alias)
	}
	return sb.String()
}

// Expressions implements the sql.Expressioner interface.
func (r *RowsFrom) Expressions() []sql.Expression {
	return r.Functions
}

// WithExpressions implements the sql.Expressioner interface.
func (r *RowsFrom) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(r.Functions) {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(exprs), len(r.Functions))
	}
	ret := *r
	ret.Functions = exprs
	return &ret, nil
}

// WithChildren implements the sql.Node interface.
func (r *RowsFrom) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 0)
	}
	return r, nil
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*RowsFrom) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// WithColumnAliases returns a new RowsFrom with the given column aliases.
func (r *RowsFrom) WithColumnAliases(aliases []string) *RowsFrom {
	ret := *r
	ret.ColumnAliases = aliases
	return &ret
}

// WithOrdinality returns a new RowsFrom with ordinality enabled/disabled.
func (r *RowsFrom) SetWithOrdinality(withOrdinality bool) *RowsFrom {
	ret := *r
	ret.WithOrdinality = withOrdinality
	return &ret
}
