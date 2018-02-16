package sql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cast"
	"gopkg.in/src-d/go-vitess.v0/sqltypes"
	"gopkg.in/src-d/go-vitess.v0/vt/proto/query"
)

// Schema is the definition of a table.
type Schema []*Column

// CheckRow checks the row conforms to the schema.
func (s Schema) CheckRow(row Row) error {
	expected := len(s)
	got := len(row)
	if expected != got {
		return fmt.Errorf("expected %d values, got %d", expected, got)
	}

	for idx, f := range s {
		v := row[idx]
		if f.Check(v) {
			continue
		}

		typ := reflect.TypeOf(v).String()
		return fmt.Errorf("value at %d has unexpected type: %s",
			idx, typ)

	}

	return nil
}

// Column is the definition of a table column.
// As SQL:2016 puts it:
//   A column is a named component of a table. It has a data type, a default,
//   and a nullability characteristic.
type Column struct {
	// Name is the name of the column.
	Name string
	// Type is the data type of the column.
	Type Type
	// Default contains the default value of the column or nil if it is NULL.
	Default interface{}
	// Nullable is true if the column can contain NULL values, or false
	// otherwise.
	Nullable bool
}

// Check ensures the value is correct for this column.
func (c *Column) Check(v interface{}) bool {
	if v == nil {
		return c.Nullable
	}

	_, err := c.Type.Convert(v)
	return err == nil
}

// Type represent a SQL type.
type Type interface {
	// Type returns the query.Type for the given Type.
	Type() query.Type
	// Covert a value of a compatible type to a most accurate type.
	Convert(interface{}) (interface{}, error)
	// Compare returns an integer comparing two values.
	// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
	Compare(interface{}, interface{}) int
	// SQL returns the sqltypes.Value for the given value.
	SQL(interface{}) sqltypes.Value
}

var (
	// Null represents the null type.
	Null nullT

	// Numeric types

	// Int32 is an integer of 32 bits.
	Int32 = numberT{t: sqltypes.Int32}
	// Int64 is an integer of 64 bytes.
	Int64 = numberT{t: sqltypes.Int64}
	// Uint32 is an unsigned integer of 32 bytes.
	Uint32 = numberT{t: sqltypes.Uint32}
	// Uint64 is an unsigned integer of 64 bytes.
	Uint64 = numberT{t: sqltypes.Uint64}
	// Float32 is a floating point number of 32 bytes.
	Float32 = numberT{t: sqltypes.Float32}
	// Float64 is a floating point number of 64 bytes.
	Float64 = numberT{t: sqltypes.Float64}

	// Timestamp is an UNIX timestamp.
	Timestamp timestampT
	// Text is a string type.
	Text textT
	// Boolean is a boolean type.
	Boolean booleanT
	// JSON is a type that holds any valid JSON object.
	JSON jsonT
	// Blob is a type that holds a chunk of binary data.
	Blob blobT
)

type nullT struct{}

// Type implements Type interface.
func (t nullT) Type() query.Type {
	return sqltypes.Null
}

// SQL implements Type interface.
func (t nullT) SQL(interface{}) sqltypes.Value {
	return sqltypes.NULL
}

// Convert implements Type interface.
func (t nullT) Convert(v interface{}) (interface{}, error) {
	if v != nil {
		return nil, fmt.Errorf("value not nil: %#v", v)
	}

	return nil, nil
}

// Compare implements Type interface. Note that while this returns 0 (equals)
// for ordering purposes, in SQL NULL != NULL.
func (t nullT) Compare(a interface{}, b interface{}) int {
	return 0
}

type numberT struct {
	t query.Type
}

// Type implements Type interface.
func (t numberT) Type() query.Type {
	return t.t
}

// SQL implements Type interface.
func (t numberT) SQL(v interface{}) sqltypes.Value {
	return sqltypes.MakeTrusted(t.t, strconv.AppendInt(nil, cast.ToInt64(v), 10))
}

// Convert implements Type interface.
func (t numberT) Convert(v interface{}) (interface{}, error) {
	switch t.t {
	case sqltypes.Int32:
		return cast.ToInt32E(v)
	case sqltypes.Int64:
		return cast.ToInt64E(v)
	case sqltypes.Uint32:
		return cast.ToUint32E(v)
	case sqltypes.Uint64:
		return cast.ToUint64E(v)
	case sqltypes.Float32:
		return cast.ToFloat32E(v)
	case sqltypes.Float64:
		return cast.ToFloat64E(v)
	default:
		return nil, ErrInvalidType.New(t.t)
	}

}

// Compare implements Type interface.
func (t numberT) Compare(a interface{}, b interface{}) int {
	if a == b {
		return 0
	}

	switch t.t {
	case sqltypes.Int32:
		if a.(int32) < b.(int32) {
			return -1
		}
	case sqltypes.Int64:
		if a.(int64) < b.(int64) {
			return -1
		}
	case sqltypes.Uint32:
		if a.(uint32) < b.(uint32) {
			return -1
		}
	case sqltypes.Uint64:
		if a.(uint64) < b.(uint64) {
			return -1
		}
	case sqltypes.Float32:
		if a.(float32) < b.(float32) {
			return -1
		}
	case sqltypes.Float64:
		if a.(float64) < b.(float64) {
			return -1
		}
	default:
		if cast.ToInt64(a) < cast.ToInt64(b) {
			return -1
		}
	}

	return +1
}

type timestampT struct{}

// Type implements Type interface.
func (t timestampT) Type() query.Type {
	return sqltypes.Timestamp
}

// TimestampLayout is the formatting string with the layout of the timestamp
// using the format of Go "time" package.
const TimestampLayout = "2006-01-02 15:04:05"

// SQL implements Type interface.
func (t timestampT) SQL(v interface{}) sqltypes.Value {
	time := MustConvert(t, v).(time.Time)
	return sqltypes.MakeTrusted(
		sqltypes.Timestamp,
		[]byte(time.Format(TimestampLayout)),
	)
}

// Convert implements Type interface.
func (t timestampT) Convert(v interface{}) (interface{}, error) {
	switch value := v.(type) {
	case time.Time:
		return value, nil
	case string:
		t, err := time.Parse(TimestampLayout, value)
		if err != nil {
			return nil, fmt.Errorf("value %q can't be converted to time.Time", v)
		}
		return t, nil
	default:
		ts, err := Int64.Convert(v)
		if err != nil {
			return nil, ErrInvalidType.New(reflect.TypeOf(v))
		}

		return time.Unix(ts.(int64), 0), nil
	}
}

// Compare implements Type interface.
func (t timestampT) Compare(a interface{}, b interface{}) int {
	av := a.(time.Time)
	bv := b.(time.Time)
	if av.Before(bv) {
		return -1
	} else if av.After(bv) {
		return 1
	}
	return 0
}

type textT struct{}

// Type implements Type interface.
func (t textT) Type() query.Type {
	return sqltypes.Text
}

// SQL implements Type interface.
func (t textT) SQL(v interface{}) sqltypes.Value {
	return sqltypes.MakeTrusted(sqltypes.Text, []byte(MustConvert(t, v).(string)))
}

// Convert implements Type interface.
func (t textT) Convert(v interface{}) (interface{}, error) {
	return cast.ToStringE(v)
}

// Compare implements Type interface.
func (t textT) Compare(a interface{}, b interface{}) int {
	return strings.Compare(a.(string), b.(string))
}

type booleanT struct{}

// Type implements Type interface.
func (t booleanT) Type() query.Type {
	return sqltypes.Bit
}

// SQL implements Type interface.
func (t booleanT) SQL(v interface{}) sqltypes.Value {
	b := []byte{'0'}
	if cast.ToBool(v) {
		b[0] = '1'
	}

	return sqltypes.MakeTrusted(sqltypes.Bit, b)
}

// Convert implements Type interface.
func (t booleanT) Convert(v interface{}) (interface{}, error) {
	return cast.ToBoolE(v)
}

// Compare implements Type interface.
func (t booleanT) Compare(a interface{}, b interface{}) int {
	if a == b {
		return 0
	}

	if a.(bool) == false {
		return -1
	}

	return +1
}

type blobT struct{}

// Type implements Type interface.
func (t blobT) Type() query.Type {
	return sqltypes.Blob
}

// SQL implements Type interface.
func (t blobT) SQL(v interface{}) sqltypes.Value {
	return sqltypes.MakeTrusted(sqltypes.Blob, MustConvert(t, v).([]byte))
}

// Convert implements Type interface.
func (t blobT) Convert(v interface{}) (interface{}, error) {
	switch value := v.(type) {
	case []byte:
		return value, nil
	case string:
		return []byte(value), nil
	case fmt.Stringer:
		return []byte(value.String()), nil
	default:
		return nil, ErrInvalidType.New(reflect.TypeOf(v))
	}
}

// Compare implements Type interface.
func (t blobT) Compare(a interface{}, b interface{}) int {
	return bytes.Compare(a.([]byte), b.([]byte))
}

type jsonT struct{}

// Type implements Type interface.
func (t jsonT) Type() query.Type {
	return sqltypes.TypeJSON
}

// SQL implements Type interface.
func (t jsonT) SQL(v interface{}) sqltypes.Value {
	return sqltypes.MakeTrusted(sqltypes.TypeJSON, MustConvert(t, v).([]byte))
}

// Convert implements Type interface.
func (t jsonT) Convert(v interface{}) (interface{}, error) {
	return json.Marshal(v)
}

// Compare implements Type interface.
func (t jsonT) Compare(a interface{}, b interface{}) int {
	return bytes.Compare(a.([]byte), b.([]byte))
}

// MustConvert calls the Convert function from a given Type, it err panics.
func MustConvert(t Type, v interface{}) interface{} {
	c, err := t.Convert(v)
	if err != nil {
		panic(err)
	}

	return c
}
