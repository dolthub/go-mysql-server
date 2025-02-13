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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"

	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

// InterpreterVariable is a variable that lives on the stack.
type InterpreterVariable struct {
	Type  *pgtypes.DoltgresType
	Value any
}

// InterpreterScopeDetails contains all of the details that are relevant to a particular scope.
type InterpreterScopeDetails struct {
	variables map[string]*InterpreterVariable
}

// InterpreterStack represents the working information that an interpreter will use during execution. It is not exactly
// the same as a stack in the traditional programming sense, but rather is a loose abstraction that serves the same
// general purpose.
type InterpreterStack struct {
	stack  *utils.Stack[*InterpreterScopeDetails]
	runner analyzer.StatementRunner
}

// NewInterpreterStack creates a new InterpreterStack.
func NewInterpreterStack(runner analyzer.StatementRunner) InterpreterStack {
	stack := utils.NewStack[*InterpreterScopeDetails]()
	// This first push represents the function base, including parameters
	stack.Push(&InterpreterScopeDetails{
		variables: make(map[string]*InterpreterVariable),
	})
	return InterpreterStack{
		stack:  stack,
		runner: runner,
	}
}

// Details returns the details for the current scope.
func (is *InterpreterStack) Details() *InterpreterScopeDetails {
	return is.stack.Peek()
}

// Runner returns the runner that is being used for the function's execution.
func (is *InterpreterStack) Runner() analyzer.StatementRunner {
	return is.runner
}

// GetVariable traverses the stack (starting from the top) to find a variable with a matching name. Returns nil if no
// variable was found.
func (is *InterpreterStack) GetVariable(name string) *InterpreterVariable {
	for i := 0; i < is.stack.Len(); i++ {
		if iv, ok := is.stack.PeekDepth(i).variables[name]; ok {
			return iv
		}
	}
	return nil
}

// ListVariables returns a map with the names of all variables.
func (is *InterpreterStack) ListVariables() map[string]struct{} {
	seen := make(map[string]struct{})
	for i := 0; i < is.stack.Len(); i++ {
		for varName := range is.stack.PeekDepth(i).variables {
			seen[varName] = struct{}{}
		}
	}
	return seen
}

// NewVariable creates a new variable in the current scope. If a variable with the same name exists in a previous scope,
// then that variable will be shadowed until the current scope exits.
func (is *InterpreterStack) NewVariable(name string, typ *pgtypes.DoltgresType) {
	is.NewVariableWithValue(name, typ, typ.Zero())
}

// NewVariableWithValue creates a new variable in the current scope, setting its initial value to the one given.
func (is *InterpreterStack) NewVariableWithValue(name string, typ *pgtypes.DoltgresType, val any) {
	is.stack.Peek().variables[name] = &InterpreterVariable{
		Type:  typ,
		Value: val,
	}
}

// NewVariableAlias creates a new variable alias, named |alias|, in the current frame of this stack,
// pointing to the specified |variable|.
func (is *InterpreterStack) NewVariableAlias(alias string, variable *InterpreterVariable) {
	is.stack.Peek().variables[alias] = variable
}

// PushScope creates a new scope.
func (is *InterpreterStack) PushScope() {
	is.stack.Push(&InterpreterScopeDetails{
		variables: make(map[string]*InterpreterVariable),
	})
}

// PopScope removes the current scope.
func (is *InterpreterStack) PopScope() {
	is.stack.Pop()
}

// SetVariable sets the first variable found, with a matching name, to the value given. This does not ensure that the
// value matches the expectations of the type, so it should be validated before this is called. Returns an error if the
// variable cannot be found.
func (is *InterpreterStack) SetVariable(ctx *sql.Context, name string, val any) error {
	iv := is.GetVariable(name)
	if iv == nil {
		return fmt.Errorf("variable `%s` could not be found", name)
	}
	iv.Value = val
	return nil
}