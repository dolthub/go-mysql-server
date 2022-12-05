// Copyright 2020-2022 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"
)

// PrepareQuery is a node that prepares the query
type PrepareQuery struct {
	Name  string
	Child sql.Node
}

// NewPrepareQuery creates a new PrepareQuery node.
func NewPrepareQuery(name string, child sql.Node) *PrepareQuery {
	return &PrepareQuery{Name: name, Child: child}
}

// Schema implements the Node interface.
func (p *PrepareQuery) Schema() sql.Schema {
	return sql.OkResultSchema
}

// PrepareInfo is the Info for OKResults returned by Update nodes.
type PrepareInfo struct {
}

// String implements fmt.Stringer
func (pi PrepareInfo) String() string {
	return "Statement prepared"
}

// RowIter implements the Node interface.
func (p *PrepareQuery) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	newRow := sql.NewRow(sql.OkResult{RowsAffected: 0, Info: PrepareInfo{}})
	return sql.RowsToRowIter(newRow), nil
}

func (p *PrepareQuery) Resolved() bool {
	return true
}

// Children implements the Node interface.
func (p *PrepareQuery) Children() []sql.Node {
	return nil // TODO: maybe just make it Opaque instead?
}

// WithChildren implements the Node interface.
func (p *PrepareQuery) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) > 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}
	return p, nil
}

// CheckPrivileges implements the interface sql.Node.
func (p *PrepareQuery) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return p.Child.CheckPrivileges(ctx, opChecker)
}

func (p *PrepareQuery) String() string {
	return fmt.Sprintf("Prepare(%s)", p.Child.String())
}

// ExecuteQuery is a node that prepares the query
type ExecuteQuery struct {
	Name  string
	Child sql.Node
}

// NewExecuteQuery executes a prepared statement
func NewExecuteQuery(name string, child sql.Node) *ExecuteQuery {
	return &ExecuteQuery{Name: name, Child: child}
}

// Schema implements the Node interface.
func (p *ExecuteQuery) Schema() sql.Schema {
	return sql.OkResultSchema
}

// RowIter implements the Node interface.
func (p *ExecuteQuery) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	newRow := sql.NewRow(sql.OkResult{RowsAffected: 0, Info: PrepareInfo{}})
	return sql.RowsToRowIter(newRow), nil
}

func (p *ExecuteQuery) Resolved() bool {
	return true
}

// Children implements the Node interface.
func (p *ExecuteQuery) Children() []sql.Node {
	return nil // TODO: maybe just make it Opaque instead?
}

// WithChildren implements the Node interface.
func (p *ExecuteQuery) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) > 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}
	return p, nil
}

// CheckPrivileges implements the interface sql.Node.
func (p *ExecuteQuery) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return p.Child.CheckPrivileges(ctx, opChecker)
}

func (p *ExecuteQuery) String() string {
	return fmt.Sprintf("Prepare(%s)", p.Child.String())
}
