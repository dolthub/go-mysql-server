package sql

import "reflect"

type Type struct {
	Name string
	InternalType reflect.Kind
}

type Schema []Field

type Field struct {
	Name string
	Type Type
}

var (
	Integer = Type{"integer", reflect.Int32}
	BigInteger = Type{"biginteger", reflect.Int64}
	String = Type{"string", reflect.String}
	Timestamp = Type{"timestamp", reflect.Int64}
)
