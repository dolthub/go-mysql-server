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
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

// Stack is a generic stack.
type Stack[T any] struct {
	values []T
}

// NewStack creates a new, empty stack.
func NewStack[T any]() *Stack[T] {
	return &Stack[T]{}
}

// Len returns the size of the stack.
func (s *Stack[T]) Len() int {
	return len(s.values)
}

// Peek returns the top value on the stack without removing it.
func (s *Stack[T]) Peek() (value T) {
	if len(s.values) == 0 {
		return
	}
	return s.values[len(s.values)-1]
}

// PeekDepth returns the n-th value from the top. PeekDepth(0) is equivalent to the standard Peek().
func (s *Stack[T]) PeekDepth(depth int) (value T) {
	if len(s.values) <= depth {
		return
	}
	return s.values[len(s.values)-(1+depth)]
}

// PeekReference returns a reference to the top value on the stack without removing it.
func (s *Stack[T]) PeekReference() *T {
	if len(s.values) == 0 {
		return nil
	}
	return &s.values[len(s.values)-1]
}

// Pop returns the top value on the stack while also removing it from the stack.
func (s *Stack[T]) Pop() (value T) {
	if len(s.values) == 0 {
		return
	}
	value = s.values[len(s.values)-1]
	s.values = s.values[:len(s.values)-1]
	return
}

// Push adds the given value to the stack.
func (s *Stack[T]) Push(value T) {
	s.values = append(s.values, value)
}

// Empty returns whether the stack is empty.
func (s *Stack[T]) Empty() bool {
	return len(s.values) == 0
}

// InterpreterVariable is a variable that lives on the stack.
type InterpreterVariable struct {
	Type  sql.Type
	Value any
}

func (iv *InterpreterVariable) ToAST() ast.Expr {
	if sqlVal, isSQLVal := iv.Value.(*ast.SQLVal); isSQLVal {
		return sqlVal
	}
	if iv.Value == nil {
		return &ast.NullVal{}
	}
	if types.IsInteger(iv.Type) {
		return ast.NewIntVal([]byte(fmt.Sprintf("%d", iv.Value)))
	}
	if types.IsFloat(iv.Type) {
		return ast.NewFloatVal([]byte(strconv.FormatFloat(iv.Value.(float64), 'f', -1, 64)))
	}
	return ast.NewStrVal([]byte(fmt.Sprintf("%s", iv.Value)))
}

// InterpreterScopeDetails contains all of the details that are relevant to a particular scope.
type InterpreterScopeDetails struct {
	variables map[string]*InterpreterVariable
}

// InterpreterStack represents the working information that an interpreter will use during execution. It is not exactly
// the same as a stack in the traditional programming sense, but rather is a loose abstraction that serves the same
// general purpose.
type InterpreterStack struct {
	stack      *Stack[*InterpreterScopeDetails]
}

// NewInterpreterStack creates a new InterpreterStack.
func NewInterpreterStack() *InterpreterStack {
	stack := NewStack[*InterpreterScopeDetails]()
	// This first push represents the function base, including parameters
	stack.Push(&InterpreterScopeDetails{
		variables: make(map[string]*InterpreterVariable),
	})
	return &InterpreterStack{
		stack:      stack,
	}
}

// Details returns the details for the current scope.
func (is *InterpreterStack) Details() *InterpreterScopeDetails {
	return is.stack.Peek()
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
func (is *InterpreterStack) NewVariable(name string, typ sql.Type) {
	is.NewVariableWithValue(name, typ, typ.Zero())
}

// NewVariableWithValue creates a new variable in the current scope, setting its initial value to the one given.
func (is *InterpreterStack) NewVariableWithValue(name string, typ sql.Type, val any) {
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
