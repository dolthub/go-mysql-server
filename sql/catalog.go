package sql

import (
	"errors"
	"fmt"
	"reflect"
)

type Catalog struct {
	Databases []Database
	Functions map[string]*FunctionEntry
}

func NewCatalog() *Catalog {
	return &Catalog{
		Functions: map[string]*FunctionEntry{},
	}
}

func (c Catalog) Database(name string) (Database, error) {
	for _, db := range c.Databases {
		if db.Name() == name {
			return db, nil
		}
	}

	return nil, fmt.Errorf("database not found: %s", name)
}

func (c Catalog) Table(dbName string, tableName string) (Table, error) {
	db, err := c.Database(dbName)
	if err != nil {
		return nil, err
	}

	tables := db.Tables()
	table, found := tables[tableName]
	if !found {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	return table, nil
}

func (c Catalog) RegisterFunction(name string, f interface{}) error {
	e, err := inspectFunction(f)
	if err != nil {
		return err
	}

	c.Functions[name] = e
	return nil
}

func (c Catalog) Function(name string) (*FunctionEntry, error) {
	e, ok := c.Functions[name]
	if !ok {
		return nil, fmt.Errorf("function not found: %s", name)
	}

	return e, nil
}

type FunctionEntry struct {
	v reflect.Value
}

func (e *FunctionEntry) Build(args ...Expression) (Expression, error) {
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

func inspectFunction(f interface{}) (*FunctionEntry, error) {
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

	return &FunctionEntry{v}, nil
}
