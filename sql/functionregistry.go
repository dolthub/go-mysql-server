package sql

import (
	"errors"
	"fmt"
	"reflect"
)

// ExpressionBuilder can build an Expression out of a given list of expressions.
type ExpressionBuilder interface {
	Build(...Expression) (Expression, error)
}

// FunctionRegistry is used to register functions. It is used both for builtin
// and User-Defined Functions.
type FunctionRegistry map[string]ExpressionBuilder

// NewFunctionRegistry creates a new FunctionRegistry.
func NewFunctionRegistry() FunctionRegistry {
	return FunctionRegistry{}
}

// RegisterFunction registers a function with the given name.
func (r FunctionRegistry) RegisterFunction(name string, f interface{}) error {
	e, err := inspectFunction(f)
	if err != nil {
		return err
	}

	r[name] = e
	return nil
}

// Function returns an ExpressionBuilder for the given function name.
func (r FunctionRegistry) Function(name string) (ExpressionBuilder, error) {
	e, ok := r[name]
	if !ok {
		return nil, fmt.Errorf("function not found: %s", name)
	}

	return e, nil
}

type functionEntry struct {
	v reflect.Value
}

func (e *functionEntry) Build(args ...Expression) (Expression, error) {
	t := e.v.Type()
	if !t.IsVariadic() && len(args) != t.NumIn() {
		return nil, fmt.Errorf("expected %d args, got %d",
			t.NumIn(), len(args))
	}

	if t.IsVariadic() && len(args) < t.NumIn()-1 {
		return nil, fmt.Errorf("expected at least %d args, got %d",
			t.NumIn(), len(args))
	}

	var in []reflect.Value
	for _, arg := range args {
		in = append(in, reflect.ValueOf(arg))
	}

	out := e.v.Call(in)
	if len(out) != 1 {
		return nil, fmt.Errorf("expected 1 return value, got %d: ", len(out))
	}

	expr, ok := out[0].Interface().(Expression)
	if !ok {
		return nil, errors.New("return value doesn't implement Expression")
	}

	return expr, nil
}

var (
	expressionType      = buildExpressionType()
	expressionSliceType = buildExpressionSliceType()
)

func buildExpressionType() reflect.Type {
	var v Expression
	return reflect.ValueOf(&v).Elem().Type()
}

func buildExpressionSliceType() reflect.Type {
	var v []Expression
	return reflect.ValueOf(&v).Elem().Type()
}

func inspectFunction(f interface{}) (*functionEntry, error) {
	v := reflect.ValueOf(f)
	t := v.Type()
	if t.Kind() != reflect.Func {
		return nil, fmt.Errorf("expected function, got: %s", t.Kind())
	}

	if t.NumOut() != 1 {
		return nil, errors.New("function builders must return a single Expression")
	}

	out := t.Out(0)
	if !out.Implements(expressionType) {
		return nil, fmt.Errorf("return value doesn't implement Expression: %s", out)
	}

	for i := 0; i < t.NumIn(); i++ {
		in := t.In(i)
		if i == t.NumIn()-1 && t.IsVariadic() && in == expressionSliceType {
			continue
		}

		if in != expressionType {
			return nil, fmt.Errorf("input argument %d is not a Expression", i)
		}
	}

	return &functionEntry{v}, nil
}
