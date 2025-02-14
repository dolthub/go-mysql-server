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
	"github.com/dolthub/doltgresql/core"
"github.com/dolthub/doltgresql/core/id"
"github.com/dolthub/go-mysql-server/sql"
)

// InterpreterNode is an interface that implements an interpreter. These are typically used for functions (which may be
// implemented as a set of operations that are interpreted during runtime).
type InterpreterNode interface {
	GetRunner() sql.StatementRunner
	GetParameters() []sql.Type
	GetParameterNames() []string
	GetReturn() sql.Type
	GetStatements() []InterpreterOperation
}

// Call runs the contained operations on the given runner.
func Call(ctx *sql.Context, call sql.InterpreterNode, runner sql.StatementRunner) (any, error) {
	// Set up the initial state of the function
	counter := -1 // We increment before accessing, so start at -1
	stack := NewInterpreterStack()
	// Add the parameters
	parameterTypes := call.GetParameters()
	parameterNames := call.GetParameterNames()
	if len(vals) != len(parameterTypes) {
		return nil, fmt.Errorf("parameter count mismatch: expected %d got %d", len(parameterTypes), len(vals))
	}
	for i := range vals {
		stack.NewVariableWithValue(parameterNames[i], parameterTypes[i], vals[i])
	}
	// Run the statements
	statements := call.Ops
	for {
		counter++
		if counter >= len(statements) {
			break
		} else if counter < 0 {
			panic("negative function counter")
		}

		operation := statements[counter]
		switch operation.OpCode {
		case OpCode_Select:

		case OpCode_Declare:
			typeCollection, err := core.GetTypesCollectionFromContext(ctx)
			if err != nil {
				return nil, err
			}
			resolvedType, exists := typeCollection.GetType(id.NewType("pg_catalog", operation.PrimaryData))
			if !exists {
				return nil, pgtypes.ErrTypeDoesNotExist.New(operation.PrimaryData)
			}
			stack.NewVariable(operation.Target, resolvedType)
		case OpCode_Exception:
			// TODO: implement
		case OpCode_Execute:
			if len(operation.Target) > 0 {
				target := stack.GetVariable(operation.Target)
				if target == nil {
					return nil, fmt.Errorf("variable `%s` could not be found", operation.Target)
				}
				retVal, err := iFunc.QuerySingleReturn(ctx, stack, operation.PrimaryData, target.Type, operation.SecondaryData)
				if err != nil {
					return nil, err
				}
				err = stack.SetVariable(ctx, operation.Target, retVal)
				if err != nil {
					return nil, err
				}
			} else {
				rowIter, err := iFunc.QueryMultiReturn(ctx, stack, operation.PrimaryData, operation.SecondaryData)
				if err != nil {
					return nil, err
				}
				if _, err = sql.RowIterToRows(ctx, rowIter); err != nil {
					return nil, err
				}
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
					}
				}
			} else {
				for ; counter > operation.Index-1; counter-- {
					switch statements[counter].OpCode {
					case OpCode_ScopeBegin:
						stack.PopScope()
					case OpCode_ScopeEnd:
						stack.PushScope()
					}
				}
			}
		case OpCode_If:
			sch, rowIter, _, err := runner.QueryWithBindings(ctx, "", operation.PrimaryData, nil, nil)
			if err != nil {
				return nil, err
			}
			row, err := rowIter.Next(ctx)
			if err != nil {
				return nil, err
			}
			rowIter.Close(ctx)
			if retVal.(bool) {
				// We're never changing the scope, so we can just assign it directly.
				// Also, we must assign to index-1, so that the increment hits our target.
				counter = operation.Index - 1
			}

		case OpCode_ScopeBegin:
			stack.PushScope()
		case OpCode_ScopeEnd:
			stack.PopScope()
		default:
			panic("unimplemented opcode")
		}
	}
	return nil, nil
}
