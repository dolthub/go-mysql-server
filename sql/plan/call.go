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

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
)

type Call struct {
	db        sql.Database
	Name      string
	Params    []sql.Expression
	asOf      sql.Expression
	Procedure *Procedure
	pRef      *expression.ProcedureReference
}

var _ sql.Node = (*Call)(nil)
var _ sql.Expressioner = (*Call)(nil)
var _ Versionable = (*Call)(nil)

// NewCall returns a *Call node.
func NewCall(db sql.Database, name string, params []sql.Expression, asOf sql.Expression) *Call {
	return &Call{
		db:     db,
		Name:   name,
		Params: params,
		asOf:   asOf,
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

// Schema implements the sql.Node interface.
func (c *Call) Schema() sql.Schema {
	if c.Procedure != nil {
		return c.Procedure.Schema()
	}
	return nil
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
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(c.Database().Name(), "", "", sql.PrivilegeType_Execute))
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
	nc.pRef = pRef
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

// RowIter implements the sql.Node interface.
func (c *Call) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	for i, paramExpr := range c.Params {
		val, err := paramExpr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		paramName := c.Procedure.Params[i].Name
		paramType := c.Procedure.Params[i].Type
		err = c.pRef.InitializeVariable(paramName, paramType, val)
		if err != nil {
			return nil, err
		}
	}
	c.pRef.PushScope()
	innerIter, err := c.Procedure.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}
	return &callIter{
		call:      c,
		innerIter: innerIter,
	}, nil
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

// callIter is the row iterator for *Call.
type callIter struct {
	call      *Call
	innerIter sql.RowIter
}

// Next implements the sql.RowIter interface.
func (iter *callIter) Next(ctx *sql.Context) (sql.Row, error) {
	return iter.innerIter.Next(ctx)
}

// Close implements the sql.RowIter interface.
func (iter *callIter) Close(ctx *sql.Context) error {
	err := iter.innerIter.Close(ctx)
	if err != nil {
		return err
	}
	err = iter.call.pRef.CloseAllCursors(ctx)
	if err != nil {
		return err
	}

	// Set all user and system variables from INOUT and OUT params
	for i, param := range iter.call.Procedure.Params {
		if param.Direction == ProcedureParamDirection_Inout ||
			(param.Direction == ProcedureParamDirection_Out && iter.call.pRef.VariableHasBeenSet(param.Name)) {
			val, err := iter.call.pRef.GetVariableValue(param.Name)
			if err != nil {
				return err
			}

			typ := iter.call.pRef.GetVariableType(param.Name)

			switch callParam := iter.call.Params[i].(type) {
			case *expression.UserVar:
				err = ctx.SetUserVariable(ctx, callParam.Name, val, typ)
				if err != nil {
					return err
				}
			case *expression.SystemVar:
				// This should have been caught by the analyzer, so a major bug exists somewhere
				return fmt.Errorf("unable to set `%s` as it is a system variable", callParam.Name)
			case *expression.ProcedureParam:
				err = callParam.Set(val, param.Type)
				if err != nil {
					return err
				}
			}
		} else if param.Direction == ProcedureParamDirection_Out { // VariableHasBeenSet was false
			// For OUT only, if a var was not set within the procedure body, then we set the vars to nil.
			// If the var had a value before the call then it is basically removed.
			switch callParam := iter.call.Params[i].(type) {
			case *expression.UserVar:
				err = ctx.SetUserVariable(ctx, callParam.Name, nil, iter.call.pRef.GetVariableType(param.Name))
				if err != nil {
					return err
				}
			case *expression.SystemVar:
				// This should have been caught by the analyzer, so a major bug exists somewhere
				return fmt.Errorf("unable to set `%s` as it is a system variable", callParam.Name)
			case *expression.ProcedureParam:
				err := callParam.Set(nil, param.Type)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
