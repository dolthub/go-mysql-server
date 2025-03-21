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
	"fmt"
	"github.com/dolthub/vitess/go/mysql"
"io"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

		ast "github.com/dolthub/vitess/go/vt/sqlparser"
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

func replaceVariablesInExpr(stack *InterpreterStack, expr ast.SQLNode) (ast.SQLNode, error) {
	switch e := expr.(type) {
	case *ast.ColName:
		iv := stack.GetVariable(strings.ToLower(e.Name.String()))
		if iv == nil {
			return expr, nil
		}
		newExpr := iv.ToAST()
		return &ast.ColName{
			Name:          e.Name,
			Qualifier:     e.Qualifier,
			StoredProcVal: newExpr,
		}, nil
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
			// TODO: properly handle user scope variables
			newExpr, err := replaceVariablesInExpr(stack, setExpr.Expr)
			if err != nil {
				return nil, err
			}
			setExpr.Expr = newExpr.(ast.Expr)
			if setExpr.Scope == ast.SetScope_User {
				continue
			}
			err = stack.SetVariable(setExpr.Name.String(), newExpr)
			if err != nil {
				return nil, err
			}
		}
	case *ast.Call:
		for i := range e.Params {
			newExpr, err := replaceVariablesInExpr(stack, e.Params[i])
			if err != nil {
				return nil, err
			}
			e.Params[i] = newExpr.(ast.Expr)
		}
	case *ast.Limit:
		newOffset, err := replaceVariablesInExpr(stack, e.Offset)
		if err != nil {
			return nil, err
		}
		newRowCount, err := replaceVariablesInExpr(stack, e.Rowcount)
		if err != nil {
			return nil, err
		}
		if newOffset != nil {
			e.Offset = newOffset.(ast.Expr)
		}
		if newRowCount != nil {
			e.Rowcount = newRowCount.(ast.Expr)
		}
	case *ast.Into:
		// TODO: somehow support select into variables
		for i := range e.Variables {
			newExpr, err := replaceVariablesInExpr(stack, e.Variables[i])
			if err != nil {
				return nil, err
			}
			e.Variables[i] = newExpr.(ast.ColIdent)
		}
	case *ast.Select:
		for i := range e.SelectExprs {
			newExpr, err := replaceVariablesInExpr(stack, e.SelectExprs[i])
			if err != nil {
				return nil, err
			}
			e.SelectExprs[i] = newExpr.(ast.SelectExpr)
		}
		if e.Into != nil {
			newExpr, err := replaceVariablesInExpr(stack, e.Into)
			if err != nil {
				return nil, err
			}
			e.Into = newExpr.(*ast.Into)
		}
		if e.Where != nil {
			newExpr, err := replaceVariablesInExpr(stack, e.Where.Expr)
			if err != nil {
				return nil, err
			}
			e.Where.Expr = newExpr.(ast.Expr)
		}
		if e.Limit != nil {
			newExpr, err := replaceVariablesInExpr(stack, e.Limit)
			if err != nil {
				return nil, err
			}
			e.Limit = newExpr.(*ast.Limit)
		}
	case *ast.Subquery:
		newExpr, err := replaceVariablesInExpr(stack, e.Select)
		if err != nil {
			return nil, err
		}
		e.Select = newExpr.(*ast.Select)
	case *ast.SetOp:
		newLeftExpr, err := replaceVariablesInExpr(stack, e.Left)
		if err != nil {
			return nil, err
		}
		newRightExpr, err := replaceVariablesInExpr(stack, e.Right)
		if err != nil {
			return nil, err
		}
		e.Left = newLeftExpr.(ast.SelectStatement)
		e.Right = newRightExpr.(ast.SelectStatement)
	case ast.ValTuple:
		for i := range e {
			newExpr, err := replaceVariablesInExpr(stack, e[i])
			if err != nil {
				return nil, err
			}
			e[i] = newExpr.(ast.Expr)
		}
	case *ast.AliasedValues:
		for i := range e.Values {
			newExpr, err := replaceVariablesInExpr(stack, e.Values[i])
			if err != nil {
				return nil, err
			}
			e.Values[i] = newExpr.(ast.ValTuple)
		}
	case *ast.Insert:
		newExpr, err := replaceVariablesInExpr(stack, e.Rows)
		if err != nil {
			return nil, err
		}
		e.Rows = newExpr.(ast.InsertRows)
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
func Call(ctx *sql.Context, iNode InterpreterNode, params []*Parameter) (any, *InterpreterStack, error) {
	// Set up the initial state of the function
	counter := -1 // We increment before accessing, so start at -1
	stack := NewInterpreterStack()
	for _, param := range params {
		stack.NewVariableWithValue(param.Name, param.Type, param.Value)
	}

	// TODO: remove this; track last selectRowIter
	var selIter sql.RowIter

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
			if newSelectStmt, err := replaceVariablesInExpr(stack, selectStmt); err == nil {
				selectStmt = newSelectStmt.(*ast.Select)
			} else {
				return nil, nil, err
			}

			if selectStmt.Into == nil {
				rowIter, err := query(ctx, runner, selectStmt)
				if err != nil {
					return nil, nil, err
				}
				rowIters = append(rowIters, rowIter)
				selIter = rowIter
				continue
			}

			selectInto := selectStmt.Into
			selectStmt.Into = nil
			schema, rowIter, _, err := runner.QueryWithBindings(ctx, "", selectStmt, nil, nil)
			if err != nil {
				return nil, nil, err
			}
			row, err := rowIter.Next(ctx)
			if err != nil {
				return nil, nil, err
			}
			if _, err = rowIter.Next(ctx); err != io.EOF {
				return nil, nil, err
			}
			if err = rowIter.Close(ctx); err != nil {
				return nil, nil, err
			}
			if len(row) != len(selectInto.Variables) {
				return nil, nil, sql.ErrColumnNumberDoesNotMatch.New()
			}
			for i := range selectInto.Variables {
				intoVar := strings.ToLower(selectInto.Variables[i].String())
				if strings.HasPrefix(intoVar, "@") {
					err = ctx.SetUserVariable(ctx, intoVar, row[i], schema[i].Type)
					if err != nil {
						return nil, nil, err
					}
				}
				err = stack.SetVariable(intoVar, row[i])
				if err != nil {
					return nil, nil, err
				}
			}

		case OpCode_Declare:
			declareStmt := operation.PrimaryData.(*ast.Declare)

			// TODO: duplicate conditions?
			if cond := declareStmt.Condition; cond != nil {
				condName := strings.ToLower(cond.Name)
				stateVal := cond.SqlStateValue
				var num int64
				var err error
				if stateVal != "" {
					if len(stateVal) != 5 {
						return nil, nil, fmt.Errorf("SQLSTATE VALUE must be a string with length 5 consisting of only integers")
					}
					if stateVal[0:2] == "00" {
						return nil, nil, fmt.Errorf("invalid SQLSTATE VALUE: '%s'", stateVal)
					}
				} else {
					// use our own error
					num, err = strconv.ParseInt(string(cond.MysqlErrorCode.Val), 10, 64)
					if err != nil || num == 0 {
						err = fmt.Errorf("invalid value '%s' for MySQL error code", string(cond.MysqlErrorCode.Val))
						return nil, nil, err
					}
				}
				stack.NewCondition(condName, stateVal, num)
			}

			// TODO: duplicate cursors?
			if cursor := declareStmt.Cursor; cursor != nil {
				cursorName := strings.ToLower(cursor.Name)
				stack.NewCursor(cursorName, cursor.SelectStmt)
			}

			// TODO: duplicate handlers?
			if handler := declareStmt.Handler; handler != nil {
				if len(handler.ConditionValues) != 1 {
					return nil, nil, sql.ErrUnsupportedSyntax.New(ast.String(declareStmt))
				}

				hCond := handler.ConditionValues[0]
				switch hCond.ValueType {
				case ast.DeclareHandlerCondition_NotFound:
				case ast.DeclareHandlerCondition_SqlState:
				default:
					return nil, nil, sql.ErrUnsupportedSyntax.New(ast.String(declareStmt))
				}

				switch handler.Action {
				case ast.DeclareHandlerAction_Continue:
				case ast.DeclareHandlerAction_Exit:
				case ast.DeclareHandlerAction_Undo:
					return nil, nil, fmt.Errorf("unsupported handler action: %s", handler.Action)
				}

				stack.NewHandler(string(hCond.ValueType), string(handler.Action), handler.Statement)
			}

			// TODO: duplicate variables?
			if vars := declareStmt.Variables; vars != nil {
				for _, decl := range vars.Names {
					varType, err := types.ColumnTypeToType(&vars.VarType)
					if err != nil {
						return nil, nil, err
					}
					varName := strings.ToLower(decl.String())
					if vars.VarType.Default == nil {
						stack.NewVariable(varName, varType)
						continue
					}
					stack.NewVariableWithValue(varName, varType, vars.VarType.Default)
				}
			}

		case OpCode_Signal:
			// TODO: copy logic from planbuilder/proc.go: buildSignal()
			signalStmt := operation.PrimaryData.(*ast.Signal)
			var msgTxt string
			var sqlState string
			var mysqlErrNo int
			if signalStmt.ConditionName == "" {
				sqlState = signalStmt.SqlStateValue
				if sqlState[0:2] == "01" {
					return nil, nil, fmt.Errorf("warnings not yet implemented")
				}
			} else {
				cond := stack.GetCondition(strings.ToLower(signalStmt.ConditionName))
				if cond == nil {
					return nil, nil, sql.ErrDeclareConditionNotFound.New(signalStmt.ConditionName)
				}
				sqlState = cond.SQLState
				mysqlErrNo = int(cond.MySQLErrCode)
			}

			if len(sqlState) != 5 {
				return nil, nil, fmt.Errorf("SQLSTATE VALUE must be a string with length 5 consisting of only integers")
			}

			for _, item := range signalStmt.Info {
				switch item.ConditionItemName {
				case ast.SignalConditionItemName_MysqlErrno:
					switch val := item.Value.(type) {
					case *ast.SQLVal:
						num, err := strconv.ParseInt(string(val.Val), 10, 64)
						if err != nil || num == 0 {
							return nil, nil, fmt.Errorf("invalid value '%s' for MySQL error code", string(val.Val))
						}
						mysqlErrNo = int(num)
					case *ast.ColName:
						return nil, nil, fmt.Errorf("unsupported signal message text type: %T", val)
					default:
						return nil, nil, fmt.Errorf("invalid value '%v' for signal condition information item MESSAGE_TEXT", val)
					}
				case ast.SignalConditionItemName_MessageText:
					switch val := item.Value.(type) {
					case *ast.SQLVal:
						msgTxt = string(val.Val)
						if len(msgTxt) > 128 {
							return nil, nil, fmt.Errorf("signal condition information item MESSAGE_TEXT has max length of 128")
						}
					case *ast.ColName:
						return nil, nil, fmt.Errorf("unsupported signal message text type: %T", val)
					default:
						return nil, nil, fmt.Errorf("invalid value '%v' for signal condition information item MESSAGE_TEXT", val)
					}
				default:
					switch val := item.Value.(type) {
					case *ast.SQLVal:
						msgTxt = string(val.Val)
						if len(msgTxt) > 64 {
							return nil, nil, fmt.Errorf("signal condition information item %s has max length of 64", strings.ToUpper(string(item.ConditionItemName)))
						}
					default:
						return nil, nil, fmt.Errorf("invalid value '%v' for signal condition information item '%s''", item.Value, strings.ToUpper(string(item.ConditionItemName)))
					}
				}
			}

			if mysqlErrNo == 0 {
				switch sqlState[0:2] {
				case "01":
					mysqlErrNo = 1642
				case "02":
					mysqlErrNo = 1643
				default:
					mysqlErrNo = 1644
				}
			}

			if msgTxt == "" {
				switch sqlState[0:2] {
				case "00":
					return nil, nil, fmt.Errorf("invalid SQLSTATE VALUE: '%s'", sqlState)
				case "01":
					msgTxt = "Unhandled user-defined warning condition"
				case "02":
					msgTxt = "Unhandled user-defined not found condition"
				default:
					msgTxt = "Unhandled user-defined exception condition"
				}
			}

			return nil, nil, mysql.NewSQLError(mysqlErrNo, sqlState, msgTxt)

		case OpCode_Open:
			openCur := operation.PrimaryData.(*ast.OpenCursor)
			cursor := stack.GetCursor(strings.ToLower(openCur.Name))
			if cursor == nil {
				return nil, nil, sql.ErrCursorNotFound.New(openCur.Name)
			}
			if cursor.RowIter != nil {
				return nil, nil, sql.ErrCursorAlreadyOpen.New(openCur.Name)
			}
			stmt, err := replaceVariablesInExpr(stack, cursor.SelectStmt)
			if err != nil {
				return nil, nil, err
			}
			schema, rowIter, _, err := runner.QueryWithBindings(ctx, "", stmt.(ast.Statement), nil, nil)
			if err != nil {
				return nil, nil, err
			}
			cursor.Schema = schema
			cursor.RowIter = rowIter

		case OpCode_Fetch:
			fetchCur := operation.PrimaryData.(*ast.FetchCursor)
			cursor := stack.GetCursor(strings.ToLower(fetchCur.Name))
			if cursor == nil {
				return nil, nil, sql.ErrCursorNotFound.New(fetchCur.Name)
			}
			if cursor.RowIter == nil {
				return nil, nil, sql.ErrCursorNotOpen.New(fetchCur.Name)
			}
			row, err := cursor.RowIter.Next(ctx)
			if err != nil {
				return nil, nil, err
			}
			if len(row) != len(fetchCur.Variables) {
				return nil, nil, sql.ErrFetchIncorrectCount.New()
			}
			for i := range fetchCur.Variables {
				varName := strings.ToLower(fetchCur.Variables[i])
				if strings.HasPrefix(varName, "@") {
					err = ctx.SetUserVariable(ctx, varName, row[i], cursor.Schema[i].Type)
					if err != nil {
						return nil, nil, err
					}
					continue
				}
				err = stack.SetVariable(varName, row[i])
				if err != nil {
					return nil, nil, err
				}
			}

		case OpCode_Close:
			closeCur := operation.PrimaryData.(*ast.CloseCursor)
			cursor := stack.GetCursor(strings.ToLower(closeCur.Name))
			if cursor == nil {
				return nil, nil, sql.ErrCursorNotFound.New(closeCur.Name)
			}
			if cursor.RowIter == nil {
				return nil, nil, sql.ErrCursorNotOpen.New(closeCur.Name)
			}
			if err := cursor.RowIter.Close(ctx); err != nil {
				return nil, nil, err
			}
			cursor.RowIter = nil

		case OpCode_Set:
			selectStmt := operation.PrimaryData.(*ast.Select)
			if selectStmt.SelectExprs == nil {
				panic("select stmt with no select exprs")
			}
			for i := range selectStmt.SelectExprs {
				newNode, err := replaceVariablesInExpr(stack, selectStmt.SelectExprs[i])
				if err != nil {
					return nil, nil, err
				}
				selectStmt.SelectExprs[i] = newNode.(ast.SelectExpr)
			}
			_, rowIter, _, err := runner.QueryWithBindings(ctx, "", selectStmt, nil, nil)
			if err != nil {
				return nil, nil, err
			}
			row, err := rowIter.Next(ctx)
			if err != nil {
				return nil, nil, err
			}
			if _, err = rowIter.Next(ctx); err != io.EOF {
				return nil, nil, err
			}
			if err = rowIter.Close(ctx); err != nil {
				return nil, nil, err
			}

			err = stack.SetVariable(strings.ToLower(operation.Target), row[0])
			if err != nil {
				return nil, nil, err
			}

		case OpCode_If:
			selectStmt := operation.PrimaryData.(*ast.Select)
			if selectStmt.SelectExprs == nil {
				panic("select stmt with no select exprs")
			}
			for i := range selectStmt.SelectExprs {
				newNode, err := replaceVariablesInExpr(stack, selectStmt.SelectExprs[i])
				if err != nil {
					return nil, nil, err
				}
				selectStmt.SelectExprs[i] = newNode.(ast.SelectExpr)
			}
			_, rowIter, _, err := runner.QueryWithBindings(ctx, "", selectStmt, nil, nil)
			if err != nil {
				return nil, nil, err
			}
			// TODO: exactly one result that is a bool for now
			row, err := rowIter.Next(ctx)
			if err != nil {
				return nil, nil, err
			}
			if _, err = rowIter.Next(ctx); err != io.EOF {
				return nil, nil, err
			}
			if err = rowIter.Close(ctx); err != nil {
				return nil, nil, err
			}

			// go to the appropriate block
			cond, _, err := types.Boolean.Convert(row[0])
			if err != nil {
				return nil, nil, err
			}
			if cond == nil || cond.(int8) == 0 {
				counter = operation.Index - 1 // index of the else block, offset by 1
			}

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

		case OpCode_Execute:
			stmt, err := replaceVariablesInExpr(stack, operation.PrimaryData)
			if err != nil {
				return nil, nil, err
			}
			rowIter, err := query(ctx, runner, stmt.(ast.Statement))
			if err != nil {
				return nil, nil, err
			}
			rowIters = append(rowIters, rowIter)

		case OpCode_Exception:
			return nil, nil, operation.Error

		case OpCode_ScopeBegin:
			stack.PushScope()

		case OpCode_ScopeEnd:
			stack.PopScope()

		default:
			panic("unimplemented opcode")
		}
	}

	if selIter != nil {
		return selIter, stack, nil
	}
	if len(rowIters) == 0 {
		rowIters = append(rowIters, sql.RowsToRowIter(sql.Row{types.NewOkResult(0)}))
	}
	return rowIters[len(rowIters)-1], stack, nil
}
