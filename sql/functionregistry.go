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

package sql

import (
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/internal/similartext"
)

// ErrFunctionAlreadyRegistered is thrown when a function is already registered
var ErrFunctionAlreadyRegistered = errors.NewKind("function '%s' is already registered")

// ErrFunctionNotFound is thrown when a function is not found
var ErrFunctionNotFound = errors.NewKind("function: '%s' not found")

// ErrInvalidArgumentNumber is returned when the number of arguments to call a
// function is different from the function arity.
var ErrInvalidArgumentNumber = errors.NewKind("function '%s' expected %v arguments, %v received")

// Function is a function defined by the user that can be applied in a SQL query.
type Function interface {
	// NewInstance returns a new instance of the function to evaluate against rows
	NewInstance(*Context, []Expression) (Expression, error)
	// Function name
	name() string
	// isFunction will restrict implementations of Function
	isFunction()
}

type CreateFunc0Args func(ctx *Context) Expression
type CreateFunc1Args func(ctx *Context, e1 Expression) Expression
type CreateFunc2Args func(ctx *Context, e1, e2 Expression) Expression
type CreateFunc3Args func(ctx *Context, e1, e2, e3 Expression) Expression
type CreateFunc4Args func(ctx *Context, e1, e2, e3, e4 Expression) Expression
type CreateFunc5Args func(ctx *Context, e1, e2, e3, e4, e5 Expression) Expression
type CreateFunc6Args func(ctx *Context, e1, e2, e3, e4, e5, e6 Expression) Expression
type CreateFunc7Args func(ctx *Context, e1, e2, e3, e4, e5, e6, e7 Expression) Expression
type CreateFuncNArgs func(ctx *Context, args ...Expression) (Expression, error)

type (
	// Function0 is a function with 0 arguments.
	Function0 struct {
		Name string
		Fn   CreateFunc0Args
	}
	// Function1 is a function with 1 argument.
	Function1 struct {
		Name string
		Fn   CreateFunc1Args
	}
	// Function2 is a function with 2 arguments.
	Function2 struct {
		Name string
		Fn   CreateFunc2Args
	}
	// Function3 is a function with 3 arguments.
	Function3 struct {
		Name string
		Fn   CreateFunc3Args
	}
	// Function4 is a function with 4 arguments.
	Function4 struct {
		Name string
		Fn   CreateFunc4Args
	}
	// Function5 is a function with 5 arguments.
	Function5 struct {
		Name string
		Fn   CreateFunc5Args
	}
	// Function6 is a function with 6 arguments.
	Function6 struct {
		Name string
		Fn   CreateFunc6Args
	}
	// Function7 is a function with 7 arguments.
	Function7 struct {
		Name string
		Fn   CreateFunc7Args
	}
	// FunctionN is a function with variable number of arguments. This function
	// is expected to return ErrInvalidArgumentNumber if the arity does not
	// match, since the check has to be done in the implementation.
	FunctionN struct {
		Name string
		Fn   CreateFuncNArgs
	}
)

type EvalLogic func(*Context, Row) (interface{}, error)

var _ Function = Function0{}
var _ Function = Function1{}
var _ Function = Function2{}
var _ Function = Function3{}
var _ Function = Function4{}
var _ Function = Function5{}
var _ Function = Function6{}
var _ Function = Function7{}
var _ Function = FunctionN{}

func NewFunction0(name string, fn func(ctx *Context) Expression) Function0 {
	return Function0{
		Name: name,
		Fn:   fn,
	}
}

// Call implements the Function interface.
func (fn Function0) NewInstance(ctx *Context, args []Expression) (Expression, error) {
	if len(args) != 0 {
		return nil, ErrInvalidArgumentNumber.New(fn.Name, 0, len(args))
	}

	return fn.Fn(ctx), nil
}

// Call implements the Function interface.
func (fn Function1) NewInstance(ctx *Context, args []Expression) (Expression, error) {
	if len(args) != 1 {
		return nil, ErrInvalidArgumentNumber.New(fn.Name, 1, len(args))
	}

	return fn.Fn(ctx, args[0]), nil
}

// Call implements the Function interface.
func (fn Function2) NewInstance(ctx *Context, args []Expression) (Expression, error) {
	if len(args) != 2 {
		return nil, ErrInvalidArgumentNumber.New(fn.Name, 2, len(args))
	}

	return fn.Fn(ctx, args[0], args[1]), nil
}

// Call implements the Function interface.
func (fn Function3) NewInstance(ctx *Context, args []Expression) (Expression, error) {
	if len(args) != 3 {
		return nil, ErrInvalidArgumentNumber.New(fn.Name, 3, len(args))
	}

	return fn.Fn(ctx, args[0], args[1], args[2]), nil
}

// Call implements the Function interface.
func (fn Function4) NewInstance(ctx *Context, args []Expression) (Expression, error) {
	if len(args) != 4 {
		return nil, ErrInvalidArgumentNumber.New(fn.Name, 4, len(args))
	}

	return fn.Fn(ctx, args[0], args[1], args[2], args[3]), nil
}

// Call implements the Function interface.
func (fn Function5) NewInstance(ctx *Context, args []Expression) (Expression, error) {
	if len(args) != 5 {
		return nil, ErrInvalidArgumentNumber.New(fn.Name, 5, len(args))
	}

	return fn.Fn(ctx, args[0], args[1], args[2], args[3], args[4]), nil
}

// Call implements the Function interface.
func (fn Function6) NewInstance(ctx *Context, args []Expression) (Expression, error) {
	if len(args) != 6 {
		return nil, ErrInvalidArgumentNumber.New(fn.Name, 6, len(args))
	}

	return fn.Fn(ctx, args[0], args[1], args[2], args[3], args[4], args[5]), nil
}

// Call implements the Function interface.
func (fn Function7) NewInstance(ctx *Context, args []Expression) (Expression, error) {
	if len(args) != 7 {
		return nil, ErrInvalidArgumentNumber.New(fn.Name, 7, len(args))
	}

	return fn.Fn(ctx, args[0], args[1], args[2], args[3], args[4], args[5], args[6]), nil
}

// Call implements the Function interface.
func (fn FunctionN) NewInstance(ctx *Context, args []Expression) (Expression, error) {
	return fn.Fn(ctx, args...)
}

func (fn Function0) name() string { return fn.Name }
func (fn Function1) name() string { return fn.Name }
func (fn Function2) name() string { return fn.Name }
func (fn Function3) name() string { return fn.Name }
func (fn Function4) name() string { return fn.Name }
func (fn Function5) name() string { return fn.Name }
func (fn Function6) name() string { return fn.Name }
func (fn Function7) name() string { return fn.Name }
func (fn FunctionN) name() string { return fn.Name }

func (Function0) isFunction() {}
func (Function1) isFunction() {}
func (Function2) isFunction() {}
func (Function3) isFunction() {}
func (Function4) isFunction() {}
func (Function5) isFunction() {}
func (Function6) isFunction() {}
func (Function7) isFunction() {}
func (FunctionN) isFunction() {}

// FunctionRegistry is used to register functions. It is used both for builtin
// and User-Defined Functions.
type FunctionRegistry map[string]Function

// NewFunctionRegistry creates a new FunctionRegistry.
func NewFunctionRegistry() FunctionRegistry {
	return make(FunctionRegistry)
}

// Register registers functions.
// If function with that name is already registered,
// the ErrFunctionAlreadyRegistered will be returned
func (r FunctionRegistry) Register(fn ...Function) error {
	for _, f := range fn {
		if _, ok := r[f.name()]; ok {
			return ErrFunctionAlreadyRegistered.New(f.name())
		}
		r[f.name()] = f
	}
	return nil
}

// MustRegister registers functions.
// If function with that name is already registered, it will panic!
func (r FunctionRegistry) MustRegister(fn ...Function) {
	if err := r.Register(fn...); err != nil {
		panic(err)
	}
}

// Function returns a function with the given name.
func (r FunctionRegistry) Function(name string) (Function, error) {
	if len(r) == 0 {
		return nil, ErrFunctionNotFound.New(name)
	}

	if fn, ok := r[name]; ok {
		return fn, nil
	}
	similar := similartext.FindFromMap(r, name)
	return nil, ErrFunctionNotFound.New(name + similar)
}
