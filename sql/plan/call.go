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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type Call struct {
	Name   string
	Params []sql.Expression
	proc   *Procedure
	pRef   *expression.ProcedureParamReference
}

var _ sql.Node = (*Call)(nil)
var _ sql.Expressioner = (*Call)(nil)

// NewCall returns a *Call node.
func NewCall(name string, params []sql.Expression) *Call {
	return &Call{
		Name:   name,
		Params: params,
	}
}

// Resolved implements the sql.Node interface.
func (c *Call) Resolved() bool {
	for _, param := range c.Params {
		if !param.Resolved() {
			return false
		}
	}
	return true
}

// Schema implements the sql.Node interface.
func (c *Call) Schema() sql.Schema {
	if c.proc != nil {
		return c.proc.Schema()
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

// Expressions implements the sql.Expressioner interface.
func (c *Call) Expressions() []sql.Expression {
	return c.Params
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

// WithProcedure returns a new *Call containing the given *sql.Procedure.
func (c *Call) WithProcedure(proc *Procedure) *Call {
	nc := *c
	nc.proc = proc
	return &nc
}

// HasProcedure returns whether a *Call has had its procedure set.
func (c *Call) HasProcedure() bool {
	return c.proc != nil
}

// WithParamReference returns a new *Call containing the given *expression.ProcedureParamReference.
func (c *Call) WithParamReference(pRef *expression.ProcedureParamReference) *Call {
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
	return fmt.Sprintf("CALL %s(%s)", c.Name, paramStr)
}

// RowIter implements the sql.Node interface.
func (c *Call) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	for i, paramExpr := range c.Params {
		val, err := paramExpr.Eval(ctx, nil)
		if err != nil {
			return nil, err
		}
		paramName := c.proc.Params[i].Name
		paramType := c.proc.Params[i].Type
		err = c.pRef.Initialize(paramName, paramType, val)
		if err != nil {
			return nil, err
		}
	}
	innerIter, err := c.proc.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}
	return &callIter{
		call:      c,
		innerIter: innerIter,
	}, nil
}

// callIter is the row iterator for *Call.
type callIter struct {
	call      *Call
	innerIter sql.RowIter
}

// Next implements the sql.RowIter interface.
func (iter *callIter) Next() (sql.Row, error) {
	return iter.innerIter.Next()
}

// Close implements the sql.RowIter interface.
func (iter *callIter) Close(ctx *sql.Context) error {
	err := iter.innerIter.Close(ctx)
	if err != nil {
		return err
	}

	// Set all user and system variables from INOUT and OUT params
	for i, param := range iter.call.proc.Params {
		if param.Direction == ProcedureParamDirection_Inout ||
			(param.Direction == ProcedureParamDirection_Out && iter.call.pRef.HasBeenSet(param.Name)) {
			val, err := iter.call.pRef.Get(param.Name)
			if err != nil {
				return err
			}
			switch callParam := iter.call.Params[i].(type) {
			case *expression.UserVar:
				err = ctx.SetUserVariable(ctx, callParam.Name, val)
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
		} else if param.Direction == ProcedureParamDirection_Out { // HasBeenSet was false
			// For OUT only, if a var was not set within the procedure body, then we set the vars to nil.
			// If the var had a value before the call then it is basically removed.
			switch callParam := iter.call.Params[i].(type) {
			case *expression.UserVar:
				err = ctx.SetUserVariable(ctx, callParam.Name, nil)
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
