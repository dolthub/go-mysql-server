package sql

import (
	"gopkg.in/src-d/go-errors.v1"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

var (
	Null NullType = nullType{}

	// ErrValueNotNil is thrown when a value that was expected to be nil, is not
	ErrValueNotNil = errors.NewKind("value not nil: %#v")
)

type NullType interface {
	Type
}

type nullType struct{}

// Compare implements Type interface. Note that while this returns 0 (equals)
// for ordering purposes, in SQL NULL != NULL.
func (t nullType) Compare(a interface{}, b interface{}) (int, error) {
	return 0, nil
}

// Convert implements Type interface.
func (t nullType) Convert(v interface{}) (interface{}, error) {
	if v != nil {
		return nil, ErrValueNotNil.New(v)
	}

	return nil, nil
}

// MustConvert implements the Type interface.
func (t nullType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Promote implements the Type interface.
func (t nullType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t nullType) SQL(interface{}) (sqltypes.Value, error) {
	return sqltypes.NULL, nil
}

// String implements Type interface.
func (t nullType) String() string {
	return "NULL"
}

// Type implements Type interface.
func (t nullType) Type() query.Type {
	return sqltypes.Null
}

// Zero implements Type interface.
func (t nullType) Zero() interface{} {
	return nil
}
