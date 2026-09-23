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

// Function is a function defined by the user that can be applied in a SQL query.
type Function interface {
	// NewInstance returns a new instance of the function to evaluate against rows
	NewInstance(ctx *Context, exprs []Expression) (Expression, error)
	// Name returns the name of this function
	Name() string
	// isFunction is a private method to restrict implementations of Function
	isFunction()
}

// FunctionProvider is an interface that allows custom functions to be provided. It's usually (but not always)
// implemented by a DatabaseProvider.
type FunctionProvider interface {
	// Function returns the schema and function with the name provided, case-insensitive
	Function(ctx *Context, schema, name string) (Function, bool)
}

// DistinctWindowFunctionValidator lets a resolved function expression validate DISTINCT window usage.
// Function providers that need dialect-specific validation must implement this interface on the
// FunctionExpression returned by Function.NewInstance, not on the provider or function definition.
type DistinctWindowFunctionValidator interface {
	FunctionExpression

	// ValidateDistinctWindow returns an engine-specific error for DISTINCT window usage after the
	// function expression has resolved its exact overload.
	ValidateDistinctWindow(schema, name string) error
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
		Fn   CreateFunc0Args
		name string
	}
	// Function1 is a function with 1 argument.
	Function1 struct {
		Fn   CreateFunc1Args
		name string
	}
	// Function2 is a function with 2 arguments.
	Function2 struct {
		Fn   CreateFunc2Args
		name string
	}
	// Function3 is a function with 3 arguments.
	Function3 struct {
		Fn   CreateFunc3Args
		name string
	}
	// Function4 is a function with 4 arguments.
	Function4 struct {
		Fn   CreateFunc4Args
		name string
	}
	// Function5 is a function with 5 arguments.
	Function5 struct {
		Fn   CreateFunc5Args
		name string
	}
	// Function6 is a function with 6 arguments.
	Function6 struct {
		Fn   CreateFunc6Args
		name string
	}
	// Function7 is a function with 7 arguments.
	Function7 struct {
		Fn   CreateFunc7Args
		name string
	}
	// FunctionN is a function with variable number of arguments. This function
	// is expected to return ErrInvalidArgumentNumber if the arity does not
	// match, since the check has to be done in the implementation.
	FunctionN struct {
		Fn   CreateFuncNArgs
		name string
	}
)

var _ Function = Function0{}
var _ Function = Function1{}
var _ Function = Function2{}
var _ Function = Function3{}
var _ Function = Function4{}
var _ Function = Function5{}
var _ Function = Function6{}
var _ Function = Function7{}
var _ Function = FunctionN{}

// NewFunction0 returns a new Function0 with the given name and creation function.
func NewFunction0(name string, fn CreateFunc0Args) Function0 {
	return Function0{name: name, Fn: fn}
}

// NewFunction1 returns a new Function1 with the given name and creation function.
func NewFunction1(name string, fn CreateFunc1Args) Function1 {
	return Function1{name: name, Fn: fn}
}

// NewFunction2 returns a new Function2 with the given name and creation function.
func NewFunction2(name string, fn CreateFunc2Args) Function2 {
	return Function2{name: name, Fn: fn}
}

// NewFunction3 returns a new Function3 with the given name and creation function.
func NewFunction3(name string, fn CreateFunc3Args) Function3 {
	return Function3{name: name, Fn: fn}
}

// NewFunction4 returns a new Function4 with the given name and creation function.
func NewFunction4(name string, fn CreateFunc4Args) Function4 {
	return Function4{name: name, Fn: fn}
}

// NewFunction5 returns a new Function5 with the given name and creation function.
func NewFunction5(name string, fn CreateFunc5Args) Function5 {
	return Function5{name: name, Fn: fn}
}

// NewFunction6 returns a new Function6 with the given name and creation function.
func NewFunction6(name string, fn CreateFunc6Args) Function6 {
	return Function6{name: name, Fn: fn}
}

// NewFunction7 returns a new Function7 with the given name and creation function.
func NewFunction7(name string, fn CreateFunc7Args) Function7 {
	return Function7{name: name, Fn: fn}
}

// NewFunctionN returns a new FunctionN with the given name and creation function.
func NewFunctionN(name string, fn CreateFuncNArgs) FunctionN {
	return FunctionN{name: name, Fn: fn}
}

// NewInstance implements the interface Function.
func (fn Function0) NewInstance(ctx *Context, exprs []Expression) (Expression, error) {
	if len(exprs) != 0 {
		return nil, ErrInvalidArgumentNumber.New(fn.name, 0, len(exprs))
	}

	return fn.Fn(ctx), nil
}

// NewInstance implements the interface Function.
func (fn Function1) NewInstance(ctx *Context, exprs []Expression) (Expression, error) {
	if len(exprs) != 1 {
		return nil, ErrInvalidArgumentNumber.New(fn.name, 1, len(exprs))
	}

	return fn.Fn(ctx, exprs[0]), nil
}

// NewInstance implements the interface Function.
func (fn Function2) NewInstance(ctx *Context, exprs []Expression) (Expression, error) {
	if len(exprs) != 2 {
		return nil, ErrInvalidArgumentNumber.New(fn.name, 2, len(exprs))
	}

	return fn.Fn(ctx, exprs[0], exprs[1]), nil
}

// NewInstance implements the interface Function.
func (fn Function3) NewInstance(ctx *Context, exprs []Expression) (Expression, error) {
	if len(exprs) != 3 {
		return nil, ErrInvalidArgumentNumber.New(fn.name, 3, len(exprs))
	}

	return fn.Fn(ctx, exprs[0], exprs[1], exprs[2]), nil
}

// NewInstance implements the interface Function.
func (fn Function4) NewInstance(ctx *Context, exprs []Expression) (Expression, error) {
	if len(exprs) != 4 {
		return nil, ErrInvalidArgumentNumber.New(fn.name, 4, len(exprs))
	}

	return fn.Fn(ctx, exprs[0], exprs[1], exprs[2], exprs[3]), nil
}

// NewInstance implements the interface Function.
func (fn Function5) NewInstance(ctx *Context, exprs []Expression) (Expression, error) {
	if len(exprs) != 5 {
		return nil, ErrInvalidArgumentNumber.New(fn.name, 5, len(exprs))
	}

	return fn.Fn(ctx, exprs[0], exprs[1], exprs[2], exprs[3], exprs[4]), nil
}

// NewInstance implements the interface Function.
func (fn Function6) NewInstance(ctx *Context, exprs []Expression) (Expression, error) {
	if len(exprs) != 6 {
		return nil, ErrInvalidArgumentNumber.New(fn.name, 6, len(exprs))
	}

	return fn.Fn(ctx, exprs[0], exprs[1], exprs[2], exprs[3], exprs[4], exprs[5]), nil
}

// NewInstance implements the interface Function.
func (fn Function7) NewInstance(ctx *Context, exprs []Expression) (Expression, error) {
	if len(exprs) != 7 {
		return nil, ErrInvalidArgumentNumber.New(fn.name, 7, len(exprs))
	}

	return fn.Fn(ctx, exprs[0], exprs[1], exprs[2], exprs[3], exprs[4], exprs[5], exprs[6]), nil
}

// NewInstance implements the interface Function.
func (fn FunctionN) NewInstance(ctx *Context, exprs []Expression) (Expression, error) {
	return fn.Fn(ctx, exprs...)
}

func (fn Function0) Name() string { return fn.name }
func (fn Function1) Name() string { return fn.name }
func (fn Function2) Name() string { return fn.name }
func (fn Function3) Name() string { return fn.name }
func (fn Function4) Name() string { return fn.name }
func (fn Function5) Name() string { return fn.name }
func (fn Function6) Name() string { return fn.name }
func (fn Function7) Name() string { return fn.name }
func (fn FunctionN) Name() string { return fn.name }

func (Function0) isFunction() {}
func (Function1) isFunction() {}
func (Function2) isFunction() {}
func (Function3) isFunction() {}
func (Function4) isFunction() {}
func (Function5) isFunction() {}
func (Function6) isFunction() {}
func (Function7) isFunction() {}
func (FunctionN) isFunction() {}

// UnsupportedFunctionStub is a marker interface for function stubs that are unsupported
type UnsupportedFunctionStub interface {
	IsUnsupported() bool
}

// FunctionExpression is an Expression that represents a function.
type FunctionExpression interface {
	Expression
	Name() string
	Description() string
}

// IsFunctionExpression returns whether |e| is a FunctionExpression. FunctionExpressions implement Nameable, but their
// Name is the name of the function rather than of the expression itself, so callers deriving a column name from a
// Nameable expression should skip them.
func IsFunctionExpression(e Expression) bool {
	_, ok := e.(FunctionExpression)
	return ok
}

// ExtendedTableFunction is an extension for table function wrapper to access schema returned from functions.
type ExtendedTableFunction interface {
	// OutParametersSchema returns schema of output parameters including name and types.
	// It's nil, if there is no output parameter defined in the routine.
	OutParametersSchema() Schema
	// Unwrap converts record values to sql.Row (unwrapped) for routines with output parameters.
	// When a routine returns more than one output parameter, it becomes RECORD type, which the result
	// is a record value. It returns the given value in sql.Row without any changes for all other types.
	Unwrap(any) Row
}
