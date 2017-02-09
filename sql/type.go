package sql

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Schema []Column

func (s Schema) CheckRow(row Row) error {
	expected := len(s)
	got := len(row)
	if expected != got {
		return fmt.Errorf("expected %d values, got %d", expected, got)
	}

	for idx, f := range s {
		v := row[idx]
		if f.Type.Check(v) {
			continue
		}

		typ := reflect.TypeOf(v).String()
		return fmt.Errorf("value at %d has unexpected type: %s",
			idx, typ)

	}

	return nil
}

type Column struct {
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

var Null = nullType{}

type nullType struct{}

func (t nullType) Name() string {
	return "null"
}

func (t nullType) InternalType() reflect.Kind {
	return reflect.Interface
}

func (t nullType) Check(v interface{}) bool {
	return v == nil
}

func (t nullType) Convert(v interface{}) (interface{}, error) {
	if v != nil {
		return nil, fmt.Errorf("value not nil: %#v", v)
	}

	return nil, nil
}

func (t nullType) Compare(a interface{}, b interface{}) int {
	//XXX: Note that while this returns 0 (equals) for ordering purposes, in
	//     SQL NULL != NULL.
	return 0
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

// TimestampWithTimezone is a timestamp with timezone.
var TimestampWithTimezone = timestampWithTimeZoneType{}

type timestampWithTimeZoneType struct{}

func (t timestampWithTimeZoneType) Name() string {
	return "timestamp with timezone"
}

func (t timestampWithTimeZoneType) InternalType() reflect.Kind {
	return reflect.Struct
}

func (t timestampWithTimeZoneType) Check(v interface{}) bool {
	return checkTimestamp(v)
}

func (t timestampWithTimeZoneType) Convert(v interface{}) (interface{}, error) {
	return convertToTimestamp(v)
}

func (t timestampWithTimeZoneType) Compare(a interface{}, b interface{}) int {
	return compareTimestamp(a, b)
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
	return checkBoolean(v)
}

func (t booleanType) Convert(v interface{}) (interface{}, error) {
	return convertToBool(v)
}

func (t booleanType) Compare(a interface{}, b interface{}) int {
	return compareBool(a, b)
}

var Float Type = floatType{}

type floatType struct{}

func (t floatType) Name() string {
	return "float"
}

func (t floatType) InternalType() reflect.Kind {
	return reflect.Float64
}

func (t floatType) Check(v interface{}) bool {
	return checkFloat64(v)
}

func (t floatType) Convert(v interface{}) (interface{}, error) {
	return convertToFloat64(v)
}

func (t floatType) Compare(a interface{}, b interface{}) int {
	return compareFloat64(a, b)
}

func checkString(v interface{}) bool {
	_, ok := v.(string)
	return ok
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
	_, ok := v.(int32)
	return ok
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
	case int64:
		i64 := v.(int64)
		if i64 > (1<<31)-1 || i64 < -(1<<31) {
			return nil, fmt.Errorf("value %d overflows int32", i64)
		}
		return int32(i64), nil
	case uint8:
		return int32(v.(uint8)), nil
	case uint16:
		return int32(v.(uint16)), nil
	case uint:
		u := v.(uint)
		if u > (1<<31)-1 {
			return nil, fmt.Errorf("value %d overflows int32", v)
		}
		return int32(u), nil
	case uint32:
		u := v.(uint32)
		if u > (1<<31)-1 {
			return nil, fmt.Errorf("value %d overflows int32", v)
		}
		return int32(u), nil
	case uint64:
		u := v.(uint64)
		if u > (1<<31)-1 {
			return nil, fmt.Errorf("value %d overflows int32", v)
		}
		return int32(u), nil
	case string:
		s := v.(string)
		i, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("value %q can't be converted to int32", v)
		}
		return int32(i), nil
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
	_, ok := v.(int64)
	return ok
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
	case uint:
		return int64(v.(uint)), nil
	case uint8:
		return int64(v.(uint8)), nil
	case uint16:
		return int64(v.(uint16)), nil
	case uint32:
		return int64(v.(uint32)), nil
	case uint64:
		u := v.(uint64)
		if u >= 1<<63 {
			return nil, fmt.Errorf("value %d overflows int64", v)
		}
		return int64(u), nil
	case string:
		s := v.(string)
		i, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("value %q can't be converted to int64", v)
		}
		return int64(i), nil
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

func compareBool(a interface{}, b interface{}) int {
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

func checkFloat64(v interface{}) bool {
	_, ok := v.(float32)
	return ok
}

func convertToFloat64(v interface{}) (interface{}, error) {
	switch v.(type) {
	case float32:
		return v.(float32), nil
	default:
		return nil, ErrInvalidType
	}
}

func compareFloat64(a interface{}, b interface{}) int {
	av := a.(float32)
	bv := b.(float32)
	if av < bv {
		return -1
	} else if av > bv {
		return 1
	}
	return 0
}

func checkTimestamp(v interface{}) bool {
	_, ok := v.(time.Time)
	return ok
}

const timestampLayout = "2006-01-02 15:04:05.000000"

func convertToTimestamp(v interface{}) (interface{}, error) {
	switch v.(type) {
	case string:
		t, err := time.Parse(timestampLayout, v.(string))
		if err != nil {
			return nil, fmt.Errorf("value %q can't be converted to int64", v)
		}
		return t, nil
	default:
		if !BigInteger.Check(v) {
			return nil, ErrInvalidType
		}

		bi, err := BigInteger.Convert(v)
		if err != nil {
			return nil, ErrInvalidType
		}

		return time.Unix(bi.(int64), 0), nil
	}
}

func compareTimestamp(a interface{}, b interface{}) int {
	av := a.(time.Time)
	bv := b.(time.Time)
	if av.Before(bv) {
		return -1
	} else if av.After(bv) {
		return 1
	}
	return 0
}
