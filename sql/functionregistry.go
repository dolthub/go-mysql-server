package sql

import (
	"gopkg.in/src-d/go-errors.v1"
)

// ErrFunctionNotFound is thrown when a function is not found
var ErrFunctionNotFound = errors.NewKind("function not found: %s")

// ErrInvalidArgumentNumber is returned when the number of arguments to call a
// function is different from the function arity.
var ErrInvalidArgumentNumber = errors.NewKind("expecting %v arguments for calling this function, %d received")

// Function is a function defined by the user that can be applied in a SQL
// query.
type Function interface {
	// Call invokes the function.
	Call(...Expression) (Expression, error)
	// isFunction will restrict implementations of Function
	isFunction()
}

type (
	// Function1 is a function with 1 argument.
	Function1 func(e Expression) Expression
	// Function2 is a function with 2 arguments.
	Function2 func(e1, e2 Expression) Expression
	// Function3 is a function with 3 arguments.
	Function3 func(e1, e2, e3 Expression) Expression
	// Function4 is a function with 4 arguments.
	Function4 func(e1, e2, e3, e4 Expression) Expression
	// Function5 is a function with 5 arguments.
	Function5 func(e1, e2, e3, e4, e5 Expression) Expression
	// Function6 is a function with 6 arguments.
	Function6 func(e1, e2, e3, e4, e5, e6 Expression) Expression
	// Function7 is a function with 7 arguments.
	Function7 func(e1, e2, e3, e4, e5, e6, e7 Expression) Expression
	// FunctionN is a function with variable number of arguments. This function
	// is expected to return ErrInvalidArgumentNumber if the arity does not
	// match, since the check has to be done in the implementation.
	FunctionN func(...Expression) (Expression, error)
)

// Call implements the Function interface.
func (fn Function1) Call(args ...Expression) (Expression, error) {
	if len(args) != 1 {
		return nil, ErrInvalidArgumentNumber.New(1, len(args))
	}

	return fn(args[0]), nil
}

// Call implements the Function interface.
func (fn Function2) Call(args ...Expression) (Expression, error) {
	if len(args) != 2 {
		return nil, ErrInvalidArgumentNumber.New(2, len(args))
	}

	return fn(args[0], args[1]), nil
}

// Call implements the Function interface.
func (fn Function3) Call(args ...Expression) (Expression, error) {
	if len(args) != 3 {
		return nil, ErrInvalidArgumentNumber.New(3, len(args))
	}

	return fn(args[0], args[1], args[2]), nil
}

// Call implements the Function interface.
func (fn Function4) Call(args ...Expression) (Expression, error) {
	if len(args) != 4 {
		return nil, ErrInvalidArgumentNumber.New(4, len(args))
	}

	return fn(args[0], args[1], args[2], args[3]), nil
}

// Call implements the Function interface.
func (fn Function5) Call(args ...Expression) (Expression, error) {
	if len(args) != 5 {
		return nil, ErrInvalidArgumentNumber.New(5, len(args))
	}

	return fn(args[0], args[1], args[2], args[3], args[4]), nil
}

// Call implements the Function interface.
func (fn Function6) Call(args ...Expression) (Expression, error) {
	if len(args) != 6 {
		return nil, ErrInvalidArgumentNumber.New(6, len(args))
	}

	return fn(args[0], args[1], args[2], args[3], args[4], args[5]), nil
}

// Call implements the Function interface.
func (fn Function7) Call(args ...Expression) (Expression, error) {
	if len(args) != 7 {
		return nil, ErrInvalidArgumentNumber.New(7, len(args))
	}

	return fn(args[0], args[1], args[2], args[3], args[4], args[5], args[6]), nil
}

// Call implements the Function interface.
func (fn FunctionN) Call(args ...Expression) (Expression, error) {
	return fn(args...)
}

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

// Functions is a map of functions identified by their name.
type Functions map[string]Function

// NewFunctionRegistry creates a new FunctionRegistry.
func NewFunctionRegistry() FunctionRegistry {
	return make(FunctionRegistry)
}

// RegisterFunction registers a function with the given name.
func (r FunctionRegistry) RegisterFunction(name string, f Function) {
	r[name] = f
}

// RegisterFunctions registers a map of functions.
func (r FunctionRegistry) RegisterFunctions(funcs Functions) {
	for name, f := range funcs {
		r[name] = f
	}
}

// Function returns a function with the given name.
func (r FunctionRegistry) Function(name string) (Function, error) {
	e, ok := r[name]
	if !ok {
		return nil, ErrFunctionNotFound.New(name)
	}

	return e, nil
}
