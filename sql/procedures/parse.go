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

func ConvertStmt(ops *[]*InterpreterOperation, stack *InterpreterStack, stmt ast.Statement) error {
	switch s := stmt.(type) {
	case *ast.BeginEndBlock:
		stack.PushScope()
		startOP := &InterpreterOperation{
			OpCode: OpCode_ScopeBegin,
		}
		*ops = append(*ops, startOP)

		// TODO: add declares
		for _, ss := range s.Statements {
			if err := ConvertStmt(ops, stack, ss); err != nil {
				return err
			}
		}
		endOp := &InterpreterOperation{
			OpCode: OpCode_ScopeEnd,
		}
		*ops = append(*ops, endOp)
		stack.PopScope()

	case *ast.Select:
		selectOp := &InterpreterOperation{
			OpCode:      OpCode_Select,
			PrimaryData: s,
		}
		*ops = append(*ops, selectOp)

	case *ast.Declare:
		declareOp := &InterpreterOperation{
			OpCode:      OpCode_Declare,
			PrimaryData: s,
		}
		*ops = append(*ops, declareOp)

	case *ast.Set:
		if len(s.Exprs) != 1 {
			panic("unexpected number of set expressions")
		}
		setExpr := s.Exprs[0]
		var setOp *InterpreterOperation
		if len(setExpr.Scope) != 0 {
			setOp = &InterpreterOperation{
				OpCode: OpCode_Execute,
				PrimaryData: s,
			}
		} else {
			selectStmt := &ast.Select{
				SelectExprs: ast.SelectExprs{
					&ast.AliasedExpr{
						Expr: setExpr.Expr,
					},
				},
			}
			setOp = &InterpreterOperation{
				OpCode:      OpCode_Set,
				PrimaryData: selectStmt,
				Target:      setExpr.Name.String(),
			}
		}
		*ops = append(*ops, setOp)

	case *ast.IfStatement:
		// TODO: assume exactly one condition for now
		ifCond := s.Conditions[0]
		// TODO: convert condition into a select query
		selectCond := &ast.Select{
			SelectExprs: ast.SelectExprs{
				&ast.AliasedExpr{
					Expr: ifCond.Expr,
				},
			},
		}
		ifOp := &InterpreterOperation{
			OpCode:      OpCode_If,
			PrimaryData: selectCond,
		}
		*ops = append(*ops, ifOp)

		for _, ifStmt := range ifCond.Statements {
			if err := ConvertStmt(ops, stack, ifStmt); err != nil {
				return err
			}
		}
		gotoOp := &InterpreterOperation{
			OpCode: OpCode_Goto,
		}
		*ops = append(*ops, gotoOp)

		ifOp.Index = len(*ops) // start of else block
		for _, elseStmt := range s.Else {
			if err := ConvertStmt(ops, stack, elseStmt); err != nil {
				return err
			}
		}

		gotoOp.Index = len(*ops) // end of if statement

	case *ast.CaseStatement:
		var caseGotoOps []*InterpreterOperation
		for _, caseStmt := range s.Cases {
			caseExpr := caseStmt.Case
			if s.Expr != nil {
				caseExpr = &ast.ComparisonExpr{
					Operator: ast.EqualStr,
					Left:     s.Expr,
					Right:    caseExpr,
				}
			}
			caseCond := &ast.Select{
				SelectExprs: ast.SelectExprs{
					&ast.AliasedExpr{
						Expr: caseExpr,
					},
				},
			}
			caseOp := &InterpreterOperation{
				OpCode:      OpCode_If,
				PrimaryData: caseCond,
			}
			*ops = append(*ops, caseOp)

			for _, ifStmt := range caseStmt.Statements {
				if err := ConvertStmt(ops, stack, ifStmt); err != nil {
					return err
				}
			}
			gotoOp := &InterpreterOperation{
				OpCode: OpCode_Goto,
			}
			caseGotoOps = append(caseGotoOps, gotoOp)
			*ops = append(*ops, gotoOp)

			caseOp.Index = len(*ops) // start of next case
		}
		if s.Else != nil {
			for _, elseStmt := range s.Else {
				if err := ConvertStmt(ops, stack, elseStmt); err != nil {
					return err
				}
			}
		}

		for _, gotoOp := range caseGotoOps {
			gotoOp.Index = len(*ops) // end of case block
		}

	case *ast.While:
		loopStart := len(*ops)

		whileCond := s.Condition
		selectCond := &ast.Select{
			SelectExprs: ast.SelectExprs{
				&ast.AliasedExpr{
					Expr: whileCond,
				},
			},
		}
		whileOp := &InterpreterOperation{
			OpCode:      OpCode_If,
			PrimaryData: selectCond,
		}
		*ops = append(*ops, whileOp)

		for _, whileStmt := range s.Statements {
			if err := ConvertStmt(ops, stack, whileStmt); err != nil {
				return err
			}
		}
		gotoOp := &InterpreterOperation{
			OpCode: OpCode_Goto,
			Index: loopStart,
		}
		*ops = append(*ops, gotoOp)

		whileOp.Index = len(*ops) // end of while block

	case *ast.Repeat:
		loopStart := len(*ops)

		repeatCond := &ast.NotExpr{Expr: s.Condition}
		selectCond := &ast.Select{
			SelectExprs: ast.SelectExprs{
				&ast.AliasedExpr{
					Expr: repeatCond,
				},
			},
		}
		repeatOp := &InterpreterOperation{
			OpCode:      OpCode_If,
			PrimaryData: selectCond,
		}
		*ops = append(*ops, repeatOp)

		for _, repeatStmt := range s.Statements {
			if err := ConvertStmt(ops, stack, repeatStmt); err != nil {
				return err
			}
		}

		gotoOp := &InterpreterOperation{
			OpCode: OpCode_Goto,
			Index: loopStart,
		}
		*ops = append(*ops, gotoOp)

		repeatOp.Index = len(*ops) // end of repeat block

	case *ast.Loop:
		loopStart := len(*ops)
		for _, loopStmt := range s.Statements {
			if err := ConvertStmt(ops, stack, loopStmt); err != nil {
				return err
			}
		}
		gotoOp := &InterpreterOperation{
			OpCode: OpCode_Goto,
			Index: loopStart,
		}
		*ops = append(*ops, gotoOp)

		// perform second pass over loop statements to add labels
		for idx := loopStart; idx < len(*ops); idx++ {
			op := (*ops)[idx]
			switch op.OpCode {
			case OpCode_Goto:
				if op.Target == s.Label {
					(*ops)[idx].Index = len(*ops)
				}
			default:
				continue
			}
		}

	case *ast.Leave:
		leaveOp := &InterpreterOperation{
			OpCode: OpCode_Goto,
			Target: s.Label, // hacky? way to signal a leave
		}
		*ops = append(*ops, leaveOp)
	default:
		execOp := &InterpreterOperation{
			OpCode:      OpCode_Execute,
			PrimaryData: s,
		}
		*ops = append(*ops, execOp)
	}

	return nil
}

// Parse parses the given CREATE FUNCTION string (which must be the entire string, not just the body) into a Block
// containing the contents of the body.
func Parse(stmt ast.Statement) ([]*InterpreterOperation, error) {
	ops := make([]*InterpreterOperation, 0, 64)
	stack := NewInterpreterStack()
	err := ConvertStmt(&ops, &stack, stmt)
	if err != nil {
		return nil, err
	}
	return ops, nil
}
