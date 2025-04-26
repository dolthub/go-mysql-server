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

import "github.com/dolthub/go-mysql-server/sql"

// Statement represents a Stored Procedure Statement.
type Statement interface {
	// AppendOperations adds the statement to the operation slice.
	AppendOperations(ctx *sql.Context, ops *[]InterpreterOperation, stack *InterpreterStack) error
}

// Assignment represents an assignment statement.
type Assignment struct {
	VariableName  string
	Expression    string
	VariableIndex int32 // TODO: figure out what this is used for, probably to get around shadowed variables?
}

var _ Statement = Assignment{}

// OperationSize implements the interface Statement.
func (Assignment) OperationSize() int32 {
	return 1
}

// AppendOperations implements the interface Statement.
func (stmt Assignment) AppendOperations(ctx *sql.Context, ops *[]InterpreterOperation, stack *InterpreterStack) error {
	//*ops = append(*ops, InterpreterOperation{
	//	OpCode: OpCode_Assign,
	//	Target: stmt.VariableName,
	//})
	return nil
}

// Block contains a collection of statements, alongside the variables that were declared for the block. Only the
// top-level block will contain parameter variables.
type Block struct {
	Variable []Variable
	Body     []Statement
}

var _ Statement = Block{}

// AppendOperations implements the interface Statement.
func (stmt Block) AppendOperations(ctx *sql.Context, ops *[]InterpreterOperation, stack *InterpreterStack) error {
	stack.PushScope()
	*ops = append(*ops, InterpreterOperation{
		OpCode: OpCode_ScopeBegin,
	})
	for _, variable := range stmt.Variable {
		stack.NewVariableWithValue(variable.Name, nil, nil)
	}
	for _, innerStmt := range stmt.Body {
		if err := innerStmt.AppendOperations(ctx, ops, stack); err != nil {
			return err
		}
	}
	*ops = append(*ops, InterpreterOperation{
		OpCode: OpCode_ScopeEnd,
	})
	stack.PopScope(ctx)
	return nil
}

// ExecuteSQL represents a standard SQL statement's execution (including the INTO syntax).
type ExecuteSQL struct {
	Statement string
	Target    string
}

var _ Statement = ExecuteSQL{}

// AppendOperations implements the interface Statement.
func (stmt ExecuteSQL) AppendOperations(_ *sql.Context, ops *[]InterpreterOperation, stack *InterpreterStack) error {
	*ops = append(*ops, InterpreterOperation{
		OpCode: OpCode_Execute,
		Target: stmt.Target,
	})
	return nil
}

// Goto jumps to the counter at the given offset.
type Goto struct {
	Offset int32
}

var _ Statement = Goto{}

// OperationSize implements the interface Statement.
func (Goto) OperationSize() int32 {
	return 1
}

// AppendOperations implements the interface Statement.
func (stmt Goto) AppendOperations(_ *sql.Context, ops *[]InterpreterOperation, _ *InterpreterStack) error {
	*ops = append(*ops, InterpreterOperation{
		OpCode: OpCode_Goto,
		Index:  len(*ops) + int(stmt.Offset),
	})
	return nil
}

// If represents an IF condition, alongside its Goto offset if the condition is true.
type If struct {
	Condition  string
	GotoOffset int32
}

var _ Statement = If{}

// OperationSize implements the interface Statement.
func (If) OperationSize() int32 {
	return 1
}

// AppendOperations implements the interface Statement.
func (stmt If) AppendOperations(_ *sql.Context, _ *[]InterpreterOperation, _ *InterpreterStack) error {
	//*ops = append(*ops, InterpreterOperation{
	//	OpCode:      OpCode_If,
	//	PrimaryData: "SELECT ;",
	//	Index:       len(*ops) + int(stmt.GotoOffset),
	//})
	return nil
}

// Perform represents a PERFORM statement.
type Perform struct {
	Statement string
}

var _ Statement = Perform{}

// OperationSize implements the interface Statement.
func (Perform) OperationSize() int32 {
	return 1
}

// AppendOperations implements the interface Statement.
func (stmt Perform) AppendOperations(_ *sql.Context, _ *[]InterpreterOperation, _ *InterpreterStack) error {
	//*ops = append(*ops, InterpreterOperation{
	//	OpCode: OpCode_Perform,
	//})
	return nil
}

// Return represents a RETURN statement.
type Return struct {
	Expression string
}

var _ Statement = Return{}

// OperationSize implements the interface Statement.
func (Return) OperationSize() int32 {
	return 1
}

// AppendOperations implements the interface Statement.
func (stmt Return) AppendOperations(_ *sql.Context, ops *[]InterpreterOperation, _ *InterpreterStack) error {
	*ops = append(*ops, InterpreterOperation{
		OpCode: OpCode_Return,
	})
	return nil
}

// Variable represents a variable. These are exclusively found within Block.
type Variable struct {
	Name        string
	Type        string
	IsParameter bool
}
