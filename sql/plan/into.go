// Copyright 2022 Dolthub, Inc.
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

	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/types"

	"github.com/gabereiser/go-mysql-server/sql"
)

// Into is a node to wrap the top-level node in a query plan so that any result will set user-defined or others
// variables given
type Into struct {
	UnaryNode
	IntoVars []sql.Expression
}

func NewInto(child sql.Node, variables []sql.Expression) *Into {
	return &Into{
		UnaryNode: UnaryNode{child},
		IntoVars:  variables,
	}
}

func (i *Into) String() string {
	p := sql.NewTreePrinter()
	var vars = make([]string, len(i.IntoVars))
	for j, v := range i.IntoVars {
		vars[j] = fmt.Sprintf(v.String())
	}
	_ = p.WriteNode("Into(%s)", strings.Join(vars, ", "))
	_ = p.WriteChildren(i.Child.String())
	return p.String()
}

func (i *Into) DebugString() string {
	p := sql.NewTreePrinter()
	var vars = make([]string, len(i.IntoVars))
	for j, v := range i.IntoVars {
		vars[j] = sql.DebugString(v)
	}
	_ = p.WriteNode("Into(%s)", strings.Join(vars, ", "))
	_ = p.WriteChildren(sql.DebugString(i.Child))
	return p.String()
}

func (i *Into) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Into")
	defer span.End()

	rowIter, err := i.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}
	rows, err := sql.RowIterToRows(ctx, nil, rowIter)
	if err != nil {
		return nil, err
	}

	rowNum := len(rows)
	if rowNum > 1 {
		return nil, sql.ErrMoreThanOneRow.New()
	}
	if rowNum == 0 {
		// a warning with error code 1329 occurs (No data), and make no change to variables
		return sql.RowsToRowIter(sql.Row{}), nil
	}
	if len(rows[0]) != len(i.IntoVars) {
		return nil, sql.ErrColumnNumberDoesNotMatch.New()
	}

	var rowValues = make([]interface{}, len(rows[0]))

	for j, val := range rows[0] {
		rowValues[j] = val
	}

	for j, v := range i.IntoVars {
		switch variable := v.(type) {
		case *expression.UserVar:
			varType := types.ApproximateTypeFromValue(rowValues[j])
			err = ctx.SetUserVariable(ctx, variable.Name, rowValues[j], varType)
			if err != nil {
				return nil, err
			}
		case *expression.ProcedureParam:
			err = variable.Set(rowValues[j], types.ApproximateTypeFromValue(rowValues[j]))
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsupported type for into: %T", variable)
		}
	}

	return sql.RowsToRowIter(sql.Row{}), nil
}

func (i *Into) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}

	return NewInto(children[0], i.IntoVars), nil
}

// CheckPrivileges implements the interface sql.Node.
func (i *Into) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return i.Child.CheckPrivileges(ctx, opChecker)
}

// WithExpressions implements the sql.Expressioner interface.
func (i *Into) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(i.IntoVars) {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(exprs), len(i.IntoVars))
	}

	return NewInto(i.Child, exprs), nil
}

// Expressions implements the sql.Expressioner interface.
func (i *Into) Expressions() []sql.Expression {
	return i.IntoVars
}
