package sql

import (
	"fmt"
	"reflect"
	"strings"
)

type Schema []Field

type Field struct {
	Name string
	Type Type
}

type Type interface {
	Name() string
	InternalType() reflect.Kind
	Check(interface{}) bool
	Convert(interface{}) (interface{}, error)
	Compare(interface{}, interface{}) int
}

var Integer = integerType{}

type integerType struct{}

func (t integerType) Name() string {
	return "integer"
}

func (t integerType) InternalType() reflect.Kind {
	return reflect.Int32
}

func (t integerType) Check(v interface{}) bool {
	return checkInt32(v)
}

func (t integerType) Convert(v interface{}) (interface{}, error) {
	return convertToInt32(v)
}

func (t integerType) Compare(a interface{}, b interface{}) int {
	return compareInt32(a, b)
}

var BigInteger = bigIntegerType{}

type bigIntegerType struct{}

func (t bigIntegerType) Name() string {
	return "biginteger"
}

func (t bigIntegerType) InternalType() reflect.Kind {
	return reflect.Int64
}

func (t bigIntegerType) Check(v interface{}) bool {
	return checkInt64(v)
}

func (t bigIntegerType) Convert(v interface{}) (interface{}, error) {
	return convertToInt64(v)
}

func (t bigIntegerType) Compare(a interface{}, b interface{}) int {
	return compareInt64(a, b)
}

var Timestamp = timestampType{}

type timestampType struct{}

func (t timestampType) Name() string {
	return "timestamp"
}

func (t timestampType) InternalType() reflect.Kind {
	return reflect.Int64
}

func (t timestampType) Check(v interface{}) bool {
	return checkInt64(v)
}

func (t timestampType) Convert(v interface{}) (interface{}, error) {
	return convertToInt64(v)
}

func (t timestampType) Compare(a interface{}, b interface{}) int {
	return compareInt64(a, b)
}

var String = stringType{}

type stringType struct{}

func (t stringType) Name() string {
	return "string"
}

func (t stringType) InternalType() reflect.Kind {
	return reflect.String
}

func (t stringType) Check(v interface{}) bool {
	return checkString(v)
}

func (t stringType) Convert(v interface{}) (interface{}, error) {
	return convertToString(v)
}

func (t stringType) Compare(a interface{}, b interface{}) int {
	return compareString(a, b)
}

var Boolean Type = booleanType{}

type booleanType struct{}

func (t booleanType) Name() string {
	return "boolean"
}

func (t booleanType) InternalType() reflect.Kind {
	return reflect.Bool
}

func (t booleanType) Check(v interface{}) bool {
	return checkString(v)
}

func (t booleanType) Convert(v interface{}) (interface{}, error) {
	return convertToString(v)
}

func (t booleanType) Compare(a interface{}, b interface{}) int {
	av := a.(bool)
	bv := b.(bool)
	if av == bv {
		return 0
	} else if av == false {
		return -1
	} else {
		return 1
	}
}

func checkString(v interface{}) bool {
	switch v.(type) {
	case string:
		return true
	default:
		return false
	}
}

func convertToString(v interface{}) (interface{}, error) {
	switch v.(type) {
	case string:
		return v.(string), nil
	case fmt.Stringer:
		return v.(fmt.Stringer).String(), nil
	default:
		return nil, ErrInvalidType
	}
}

func compareString(a interface{}, b interface{}) int {
	av := a.(string)
	bv := b.(string)
	return strings.Compare(av, bv)
}

func checkInt32(v interface{}) bool {
	switch v.(type) {
	case int32:
		return true
	default:
		return false
	}
}

func convertToInt32(v interface{}) (interface{}, error) {
	switch v.(type) {
	case int:
		return int32(v.(int)), nil
	case int8:
		return int32(v.(int8)), nil
	case int16:
		return int32(v.(int16)), nil
	case int32:
		return v.(int32), nil
	default:
		return nil, ErrInvalidType
	}
}

func compareInt32(a interface{}, b interface{}) int {
	av := a.(int32)
	bv := b.(int32)
	if av < bv {
		return -1
	} else if av > bv {
		return 1
	}
	return 0
}

func checkInt64(v interface{}) bool {
	switch v.(type) {
	case int64:
		return true
	default:
		return false
	}
}

func convertToInt64(v interface{}) (interface{}, error) {
	switch v.(type) {
	case int:
		return int64(v.(int)), nil
	case int8:
		return int64(v.(int8)), nil
	case int16:
		return int64(v.(int16)), nil
	case int32:
		return int64(v.(int32)), nil
	case int64:
		return v.(int64), nil
	default:
		return nil, ErrInvalidType
	}
}

func checkBoolean(v interface{}) bool {
	_, ok := v.(bool)
	return ok
}

func convertToBool(v interface{}) (interface{}, error) {
	switch v.(type) {
	case bool:
		return v.(bool), nil
	default:
		return nil, ErrInvalidType
	}
}

func compareInt64(a interface{}, b interface{}) int {
	av := a.(int64)
	bv := b.(int64)
	if av < bv {
		return -1
	} else if av > bv {
		return 1
	}
	return 0
}
