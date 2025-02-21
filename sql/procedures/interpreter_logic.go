// Copyright 2025 Dolthub, Inc.
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

package procedures

import (
	"io"
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// InterpreterNode is an interface that implements an interpreter. These are typically used for functions (which may be
// implemented as a set of operations that are interpreted during runtime).
type InterpreterNode interface {
	GetRunner() sql.StatementRunner
	GetReturn() sql.Type
	GetStatements() []*InterpreterOperation
	SetStatementRunner(ctx *sql.Context, runner sql.StatementRunner) sql.Node
}

type Parameter struct {
	Name  string
	Type  sql.Type
	Value any
}

func unreplaceVariablesInExpr(stack *InterpreterStack, expr ast.SQLNode) ast.SQLNode {
	switch e := expr.(type) {
	case *ast.AliasedExpr:
		newExpr := unreplaceVariablesInExpr(stack, e.Expr)
		e.Expr = newExpr.(ast.Expr)
	case *ast.BinaryExpr:
		newLeftExpr := unreplaceVariablesInExpr(stack, e.Left)
		newRightExpr := unreplaceVariablesInExpr(stack, e.Right)
		e.Left = newLeftExpr.(ast.Expr)
		e.Right = newRightExpr.(ast.Expr)
	case *ast.ComparisonExpr:
		newLeftExpr := unreplaceVariablesInExpr(stack, e.Left)
		newRightExpr := unreplaceVariablesInExpr(stack, e.Right)
		e.Left = newLeftExpr.(ast.Expr)
		e.Right = newRightExpr.(ast.Expr)
	case *ast.FuncExpr:
		for i := range e.Exprs {
			newExpr := unreplaceVariablesInExpr(stack, e.Exprs[i])
			e.Exprs[i] = newExpr.(ast.SelectExpr)
		}
	case *ast.NotExpr:
		newExpr := unreplaceVariablesInExpr(stack, e.Expr)
		e.Expr = newExpr.(ast.Expr)
	case *ast.Set:
		for _, setExpr := range e.Exprs {
			newExpr := unreplaceVariablesInExpr(stack, setExpr.Expr)
			setExpr.Expr = newExpr.(ast.Expr)
		}
	case *ast.SQLVal:
		if oldVal, ok := stack.replaceMap[expr]; ok {
			return oldVal
		}
	}
	return expr
}

func replaceVariablesInExpr(stack *InterpreterStack, expr ast.SQLNode) (ast.SQLNode, error) {
	switch e := expr.(type) {
	case *ast.AliasedExpr:
		newExpr, err := replaceVariablesInExpr(stack, e.Expr)
		if err != nil {
			return nil, err
		}
		e.Expr = newExpr.(ast.Expr)
	case *ast.BinaryExpr:
		newLeftExpr, err := replaceVariablesInExpr(stack, e.Left)
		if err != nil {
			return nil, err
		}
		newRightExpr, err := replaceVariablesInExpr(stack, e.Right)
		if err != nil {
			return nil, err
		}
		e.Left = newLeftExpr.(ast.Expr)
		e.Right = newRightExpr.(ast.Expr)
	case *ast.ComparisonExpr:
		newLeftExpr, err := replaceVariablesInExpr(stack, e.Left)
		if err != nil {
			return nil, err
		}
		newRightExpr, err := replaceVariablesInExpr(stack, e.Right)
		if err != nil {
			return nil, err
		}
		e.Left = newLeftExpr.(ast.Expr)
		e.Right = newRightExpr.(ast.Expr)
	case *ast.FuncExpr:
		for i := range e.Exprs {
			newExpr, err := replaceVariablesInExpr(stack, e.Exprs[i])
			if err != nil {
				return nil, err
			}
			e.Exprs[i] = newExpr.(ast.SelectExpr)
		}
	case *ast.NotExpr:
		newExpr, err := replaceVariablesInExpr(stack, e.Expr)
		if err != nil {
			return nil, err
		}
		e.Expr = newExpr.(ast.Expr)
	case *ast.Set:
		for _, setExpr := range e.Exprs {
			newExpr, err := replaceVariablesInExpr(stack, setExpr.Expr)
			if err != nil {
				return nil, err
			}
			err = stack.SetVariable(nil, setExpr.Name.String(), newExpr)
			if err != nil {
				return nil, err
			}
		}
	case *ast.ColName:
		iv := stack.GetVariable(e.Name.String())
		if iv == nil {
			return expr, nil
		}
		newExpr := iv.ToAST()
		stack.replaceMap[newExpr] = e
		return newExpr, nil
	}
	return expr, nil
}

func query(ctx *sql.Context, runner sql.StatementRunner, stmt ast.Statement) (sql.RowIter, error) {
	_, rowIter, _, err := runner.QueryWithBindings(ctx, "", stmt, nil, nil)
	if err != nil {
		return nil, err
	}
	var rows []sql.Row
	for {
		row, rErr := rowIter.Next(ctx)
		if rErr != nil {
			if rErr == io.EOF {
				break
			}
			return nil, rErr
		}
		rows = append(rows, row)
	}
	if err = rowIter.Close(ctx); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(rows...), nil
}

// Call runs the contained operations on the given runner.
func Call(ctx *sql.Context, iNode InterpreterNode, params []*Parameter) (any, error) {
	// Set up the initial state of the function
	counter := -1 // We increment before accessing, so start at -1
	stack := NewInterpreterStack()
	for _, param := range params {
		stack.NewVariableWithValue(param.Name, param.Type, param.Value)
	}

	// Run the statements
	// TODO: eventually return multiple sql.RowIters
	var rowIters []sql.RowIter
	runner := iNode.GetRunner()
	statements := iNode.GetStatements()
	for {
		counter++
		if counter < 0 {
			panic("negative function counter")
		}
		if counter >= len(statements) {
			break
		}

		operation := statements[counter]
		switch operation.OpCode {
		case OpCode_Select:
			selectStmt := operation.PrimaryData.(*ast.Select)
			if selectStmt.SelectExprs == nil {
				panic("select stmt with no select exprs")
			}
			for i := range selectStmt.SelectExprs {
				newNode, err := replaceVariablesInExpr(&stack, selectStmt.SelectExprs[i])
				if err != nil {
					return nil, err
				}
				selectStmt.SelectExprs[i] = newNode.(ast.SelectExpr)
			}
			rowIter, err := query(ctx, runner, selectStmt)
			if err != nil {
				return nil, err
			}
			rowIters = append(rowIters, rowIter)

			for i := range selectStmt.SelectExprs {
				newNode := unreplaceVariablesInExpr(&stack, selectStmt.SelectExprs[i])
				selectStmt.SelectExprs[i] = newNode.(ast.SelectExpr)
			}
			stack.replaceMap = map[ast.SQLNode]ast.SQLNode{}

		case OpCode_Declare:
			declareStmt := operation.PrimaryData.(*ast.Declare)
			for _, decl := range declareStmt.Variables.Names {
				varType, err := types.ColumnTypeToType(&declareStmt.Variables.VarType)
				if err != nil {
					return nil, err
				}
				varName := strings.ToLower(decl.String())
				if declareStmt.Variables.VarType.Default != nil {
					stack.NewVariableWithValue(varName, varType, declareStmt.Variables.VarType.Default)
				} else {
					stack.NewVariable(varName, varType)
				}
			}
		case OpCode_Set:
			selectStmt := operation.PrimaryData.(*ast.Select)
			if selectStmt.SelectExprs == nil {
				panic("select stmt with no select exprs")
			}
			for i := range selectStmt.SelectExprs {
				newNode, err := replaceVariablesInExpr(&stack, selectStmt.SelectExprs[i])
				if err != nil {
					return nil, err
				}
				selectStmt.SelectExprs[i] = newNode.(ast.SelectExpr)
			}
			_, rowIter, _, err := runner.QueryWithBindings(ctx, "", selectStmt, nil, nil)
			if err != nil {
				return nil, err
			}
			row, err := rowIter.Next(ctx)
			if err != nil {
				return nil, err
			}
			if _, err = rowIter.Next(ctx); err != io.EOF {
				return nil, err
			}
			if err = rowIter.Close(ctx); err != nil {
				return nil, err
			}

			err = stack.SetVariable(nil, strings.ToLower(operation.Target), row[0])
			if err != nil {
				return nil, err
			}

			for i := range selectStmt.SelectExprs {
				newNode := unreplaceVariablesInExpr(&stack, selectStmt.SelectExprs[i])
				selectStmt.SelectExprs[i] = newNode.(ast.SelectExpr)
			}
			stack.replaceMap = map[ast.SQLNode]ast.SQLNode{}

		case OpCode_Exception:
			// TODO: implement
		case OpCode_Execute:
			// TODO: replace variables
			stmt, err := replaceVariablesInExpr(&stack, operation.PrimaryData)
			if err != nil {
				return nil, err
			}
			rowIter, err := query(ctx, runner, stmt.(ast.Statement))
			if err != nil {
				return nil, err
			}
			rowIters = append(rowIters, rowIter)

			stmt = unreplaceVariablesInExpr(&stack, stmt)
			stack.replaceMap = map[ast.SQLNode]ast.SQLNode{}

		case OpCode_Goto:
			// We must compare to the index - 1, so that the increment hits our target
			if counter <= operation.Index {
				for ; counter < operation.Index-1; counter++ {
					switch statements[counter].OpCode {
					case OpCode_ScopeBegin:
						stack.PushScope()
					case OpCode_ScopeEnd:
						stack.PopScope()
					default:
						// No-op
					}
				}
			} else {
				for ; counter > operation.Index-1; counter-- {
					switch statements[counter].OpCode {
					case OpCode_ScopeBegin:
						stack.PopScope()
					case OpCode_ScopeEnd:
						stack.PushScope()
					default:
						// No-op
					}
				}
			}
		case OpCode_If:
			selectStmt := operation.PrimaryData.(*ast.Select)
			if selectStmt.SelectExprs == nil {
				panic("select stmt with no select exprs")
			}
			for i := range selectStmt.SelectExprs {
				newNode, err := replaceVariablesInExpr(&stack, selectStmt.SelectExprs[i])
				if err != nil {
					return nil, err
				}
				selectStmt.SelectExprs[i] = newNode.(ast.SelectExpr)
			}
			_, rowIter, _, err := runner.QueryWithBindings(ctx, "", selectStmt, nil, nil)
			if err != nil {
				return nil, err
			}
			// TODO: exactly one result that is a bool for now
			row, err := rowIter.Next(ctx)
			if err != nil {
				return nil, err
			}
			if _, err = rowIter.Next(ctx); err != io.EOF {
				return nil, err
			}
			if err = rowIter.Close(ctx); err != nil {
				return nil, err
			}

			// go to the appropriate block
			cond := row[0].(bool)
			if !cond {
				counter = operation.Index - 1 // index of the else block, offset by 1
			}

			for i := range selectStmt.SelectExprs {
				newNode := unreplaceVariablesInExpr(&stack, selectStmt.SelectExprs[i])
				selectStmt.SelectExprs[i] = newNode.(ast.SelectExpr)
			}
			stack.replaceMap = map[ast.SQLNode]ast.SQLNode{}

		case OpCode_ScopeBegin:
			stack.PushScope()
		case OpCode_ScopeEnd:
			stack.PopScope()
		default:
			panic("unimplemented opcode")
		}
	}
	if len(rowIters) == 0 {
		panic("no rowIters")
	}
	return rowIters[len(rowIters)-1], nil
}
