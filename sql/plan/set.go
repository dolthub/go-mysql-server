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

package plan

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Set represents a set statement. This can be variables, but in some instances can also refer to row values.
type Set struct {
	Exprs []sql.Expression
}

// NewSet creates a new Set node.
func NewSet(vars []sql.Expression) *Set {
	return &Set{Exprs: vars}
}

// Resolved implements the sql.Node interface.
func (s *Set) Resolved() bool {
	for _, v := range s.Exprs {
		if !v.Resolved() {
			return false
		}
	}
	return true
}

// Children implements the sql.Node interface.
func (s *Set) Children() []sql.Node { return nil }

// WithChildren implements the sql.Node interface.
func (s *Set) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 0)
	}

	return s, nil
}

// WithExpressions implements the sql.Expressioner interface.
func (s *Set) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(s.Exprs) {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(exprs), len(s.Exprs))
	}

	return NewSet(exprs), nil
}

// Expressions implements the sql.Expressioner interface.
func (s *Set) Expressions() []sql.Expression {
	return s.Exprs
}

// RowIter implements the sql.Node interface.
func (s *Set) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Set")
	defer span.Finish()

	var updateExprs []sql.Expression
	for _, v := range s.Exprs {
		setField, ok := v.(*expression.SetField)
		if !ok {
			return nil, fmt.Errorf("unsupported type for set: %T", v)
		}

		switch left := setField.Left.(type) {
		case *expression.SystemVar:
			err := setSystemVar(ctx, left, setField.Right, row)
			if err != nil {
				return nil, err
			}
		case *expression.UserVar:
			err := setUserVar(ctx, left, setField.Right, row)
			if err != nil {
				return nil, err
			}
		case *expression.ProcedureParam:
			value, err := setField.Right.Eval(ctx, row)
			if err != nil {
				return nil, err
			}
			err = left.Set(value, setField.Right.Type())
			if err != nil {
				return nil, err
			}
		case *expression.GetField:
			updateExprs = append(updateExprs, setField)
		default:
			return nil, fmt.Errorf("unsupported type for set: %T", left)
		}
	}

	var resultRow sql.Row
	if len(updateExprs) > 0 {
		newRow, err := applyUpdateExpressions(ctx, updateExprs, row)
		if err != nil {
			return nil, err
		}
		copy(resultRow, row)
		resultRow = row.Append(newRow)
	}

	return sql.RowsToRowIter(resultRow), nil
}

func setUserVar(ctx *sql.Context, userVar *expression.UserVar, right sql.Expression, row sql.Row) error {
	val, err := right.Eval(ctx, row)
	if err != nil {
		return err
	}
	err = ctx.SetUserVariable(ctx, userVar.Name, val)
	if err != nil {
		return err
	}
	return nil
}

func setSystemVar(ctx *sql.Context, sysVar *expression.SystemVar, right sql.Expression, row sql.Row) error {
	val, err := right.Eval(ctx, row)
	if err != nil {
		return err
	}
	switch sysVar.Scope {
	case sql.SystemVariableScope_Global:
		err = sql.SystemVariables.SetGlobal(sysVar.Name, val)
		if err != nil {
			return err
		}
	case sql.SystemVariableScope_Session:
		err = ctx.SetSessionVariable(ctx, sysVar.Name, val)
		if err != nil {
			return err
		}
	default: // should never be hit
		return fmt.Errorf("unable to set `%s` due to unknown scope `%v`", sysVar.Name, sysVar.Scope)
	}
	return nil
}

// Schema implements the sql.Node interface.
func (s *Set) Schema() sql.Schema {
	return nil
}

func (s *Set) String() string {
	var children = make([]string, len(s.Exprs))
	for i, v := range s.Exprs {
		children[i] = fmt.Sprintf(v.String())
	}
	return strings.Join(children, ", ")
}

func (s *Set) DebugString() string {
	var children = make([]string, len(s.Exprs))
	for i, v := range s.Exprs {
		children[i] = fmt.Sprintf(sql.DebugString(v))
	}
	return strings.Join(children, ", ")
}
