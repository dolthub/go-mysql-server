// Copyright 2021 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type Call struct {
	db        sql.Database
	Name      string
	Params    []sql.Expression
	asOf      sql.Expression
	Procedure *Procedure
	Pref      *expression.ProcedureReference
	cat       *sql.Catalog
}

var _ sql.Node = (*Call)(nil)
var _ sql.CollationCoercible = (*Call)(nil)
var _ sql.Expressioner = (*Call)(nil)
var _ Versionable = (*Call)(nil)

// NewCall returns a *Call node.
func NewCall(db sql.Database, name string, params []sql.Expression, asOf sql.Expression, catalog *sql.Catalog) *Call {
	return &Call{
		db:     db,
		Name:   name,
		Params: params,
		asOf:   asOf,
		cat:    catalog,
	}
}

// Resolved implements the sql.Node interface.
func (c *Call) Resolved() bool {
	if c.db != nil {
		_, ok := c.db.(sql.UnresolvedDatabase)
		if ok {
			return false
		}
	}
	for _, param := range c.Params {
		if !param.Resolved() {
			return false
		}
	}
	return true
}

func (c *Call) IsReadOnly() bool {
	if c.Procedure == nil {
		return true
	}
	return c.Procedure.IsReadOnly()
}

// Schema implements the sql.Node interface.
func (c *Call) Schema() sql.Schema {
	if c.Procedure != nil {
		return c.Procedure.Schema()
	}
	return types.OkResultSchema
}

// Children implements the sql.Node interface.
func (c *Call) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface.
func (c *Call) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(c, children...)
}

// CheckPrivileges implements the interface sql.Node.
func (c *Call) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	// Procedure permissions checking is performed in the same way MySQL does it, with an exception where
	// procedures which are marked as AdminOnly. These procedures are only accessible to users with explicit Execute
	// permissions on the procedure in question.

	adminOnly := false
	if c.cat != nil {
		paramCount := len(c.Params)
		proc, err := (*c.cat).ExternalStoredProcedure(ctx, c.Name, paramCount)
		// Not finding the procedure isn't great - but that's going to surface with a better error later in the
		// query execution. For the permission check, we'll proceed as though the procedure exists, and is not AdminOnly.
		if proc != nil && err == nil && proc.AdminOnly {
			adminOnly = true
		}
	}

	if !adminOnly {
		subject := sql.PrivilegeCheckSubject{Database: c.Database().Name()}
		if opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Execute)) {
			return true
		}
	}

	subject := sql.PrivilegeCheckSubject{Database: c.Database().Name(), Routine: c.Name, IsProcedure: true}
	return opChecker.RoutineAdminCheck(ctx, sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Execute))
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (c *Call) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return c.Procedure.CollationCoercibility(ctx)
}

// Expressions implements the sql.Expressioner interface.
func (c *Call) Expressions() []sql.Expression {
	return c.Params
}

// AsOf implements the Versionable interface.
func (c *Call) AsOf() sql.Expression {
	return c.asOf
}

// WithExpressions implements the sql.Expressioner interface.
func (c *Call) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(c.Params) {
		return nil, fmt.Errorf("%s: invalid param number, got %d, expected %d", c.Name, len(exprs), len(c.Params))
	}

	nc := *c
	nc.Params = exprs
	return &nc, nil
}

// WithAsOf implements the Versionable interface.
func (c *Call) WithAsOf(asOf sql.Expression) (sql.Node, error) {
	nc := *c
	nc.asOf = asOf
	return &nc, nil
}

// WithProcedure returns a new *Call containing the given *sql.Procedure.
func (c *Call) WithProcedure(proc *Procedure) *Call {
	nc := *c
	nc.Procedure = proc
	return &nc
}

// WithParamReference returns a new *Call containing the given *expression.ProcedureReference.
func (c *Call) WithParamReference(pRef *expression.ProcedureReference) *Call {
	nc := *c
	nc.Pref = pRef
	return &nc
}

// String implements the sql.Node interface.
func (c *Call) String() string {
	paramStr := ""
	for i, param := range c.Params {
		if i > 0 {
			paramStr += ", "
		}
		paramStr += param.String()
	}
	if c.db == nil {
		return fmt.Sprintf("CALL %s(%s)", c.Name, paramStr)
	} else {
		return fmt.Sprintf("CALL %s.%s(%s)", c.db.Name(), c.Name, paramStr)
	}
}

// DebugString implements sql.DebugStringer
func (c *Call) DebugString() string {
	paramStr := ""
	for i, param := range c.Params {
		if i > 0 {
			paramStr += ", "
		}
		paramStr += sql.DebugString(param)
	}
	tp := sql.NewTreePrinter()
	if c.db == nil {
		tp.WriteNode("CALL %s(%s)", c.Name, paramStr)
	} else {
		tp.WriteNode("CALL %s.%s(%s)", c.db.Name(), c.Name, paramStr)
	}
	if c.Procedure != nil {
		tp.WriteChildren(sql.DebugString(c.Procedure.Body))
	}

	return tp.String()
}

// Database implements the sql.Databaser interface.
func (c *Call) Database() sql.Database {
	if c.db == nil {
		return sql.UnresolvedDatabase("")
	}
	return c.db
}

// WithDatabase implements the sql.Databaser interface.
func (c *Call) WithDatabase(db sql.Database) (sql.Node, error) {
	nc := *c
	nc.db = db
	return &nc, nil
}

func (c *Call) Dispose() {
	if c.Procedure != nil {
		disposeNode(c.Procedure)
	}
}
