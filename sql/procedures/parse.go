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
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

func ConvertStmt(ops *[]InterpreterOperation, stack *InterpreterStack, stmt ast.Statement) error {
	switch s := stmt.(type) {
	case *ast.BeginEndBlock:
		stack.PushScope()
		startOP := InterpreterOperation{
			OpCode: OpCode_ScopeBegin,
		}
		*ops = append(*ops, startOP)

		// TODO: add declares
		for _, ss := range s.Statements {
			if err := ConvertStmt(ops, stack, ss); err != nil {
				return err
			}
		}
		endOp := InterpreterOperation{
			OpCode: OpCode_ScopeEnd,
		}
		*ops = append(*ops, endOp)
		stack.PopScope()
	case *ast.Select:
		selectOp := InterpreterOperation{
			OpCode: OpCode_Select,
			PrimaryData: s,
		}
		*ops = append(*ops, selectOp)

	case *ast.Declare:
		// TODO:
		//declareOp := InterpreterOperation{}
		//stack.NewVariable

	default:
		execOp := InterpreterOperation{
			OpCode: OpCode_Execute,
			PrimaryData: s,
		}
		*ops = append(*ops, execOp)
	}

	return nil
}

// Parse parses the given CREATE FUNCTION string (which must be the entire string, not just the body) into a Block
// containing the contents of the body.
func Parse(stmt ast.Statement) ([]InterpreterOperation, error) {
	ops := make([]InterpreterOperation, 0, 64)
	stack := NewInterpreterStack()
	err := ConvertStmt(&ops, &stack, stmt)
	if err != nil {
		return nil, err
	}
	return ops, nil
}