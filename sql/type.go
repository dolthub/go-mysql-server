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
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-vitess.v0/sqltypes"
	"gopkg.in/src-d/go-vitess.v0/vt/proto/query"
)

// ErrTypeNotSupported is thrown when a specific type is not supported
var ErrTypeNotSupported = errors.NewKind("Type not supported: %s")

// ErrUnexpectedType is thrown when a received type is not the expected
var ErrUnexpectedType = errors.NewKind("value at %d has unexpected type: %s")

// ErrConvertingToTime is thrown when a value cannot be converted to a Time
var ErrConvertingToTime = errors.NewKind("value %q can't be converted to time.Time")

// ErrValueNotNil is thrown when a value that was expected to be nil, is not
var ErrValueNotNil = errors.NewKind("value not nil: %#v")

// Schema is the definition of a table.
type Schema []*Column

// CheckRow checks the row conforms to the schema.
func (s Schema) CheckRow(row Row) error {
	expected := len(s)
	got := len(row)
	if expected != got {
		return ErrUnexpectedRowLength.New(expected, got)
	}

	for idx, f := range s {
		v := row[idx]
		if f.Check(v) {
			continue
		}

		typ := reflect.TypeOf(v).String()
		return ErrUnexpectedType.New(idx, typ)
	}

	return nil
}

// Contains returns whether the schema contains a column with the given name.
func (s Schema) Contains(column string) bool {
	for _, col := range s {
		if col.Name == column {
			return true
		}
	}
	return false
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
	// Source is the name of the table this column came from.
	Source string
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
	// Date is a date with day, month and year.
	Date dateT
	// Text is a string type.
	Text textT
	// Boolean is a boolean type.
	Boolean booleanT
	// JSON is a type that holds any valid JSON object.
	JSON jsonT
	// Blob is a type that holds a chunk of binary data.
	Blob blobT
)

// MysqlTypeToType gets the column type using the mysql type
func MysqlTypeToType(sql query.Type) (Type, error) {
	switch sql {
	case sqltypes.Null:
		return Null, nil
	case sqltypes.Int32:
		return Int32, nil
	case sqltypes.Int64:
		return Int64, nil
	case sqltypes.Uint32:
		return Uint32, nil
	case sqltypes.Uint64:
		return Uint64, nil
	case sqltypes.Float32:
		return Float32, nil
	case sqltypes.Float64:
		return Float64, nil
	case sqltypes.Timestamp:
		return Timestamp, nil
	case sqltypes.Date:
		return Date, nil
	case sqltypes.Text, sqltypes.VarChar:
		return Text, nil
	case sqltypes.Bit:
		return Boolean, nil
	case sqltypes.TypeJSON:
		return JSON, nil
	case sqltypes.Blob:
		return Blob, nil
	default:
		return nil, ErrTypeNotSupported.New(sql)
	}
}

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
		return nil, ErrValueNotNil.New(v)
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
		return value.UTC(), nil
	case string:
		t, err := time.Parse(TimestampLayout, value)
		if err != nil {
			return nil, ErrConvertingToTime.Wrap(err, v)
		}
		return t.UTC(), nil
	default:
		ts, err := Int64.Convert(v)
		if err != nil {
			return nil, ErrInvalidType.New(reflect.TypeOf(v))
		}

		return time.Unix(ts.(int64), 0).UTC(), nil
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

type dateT struct{}

// DateLayout is the layout of the MySQL date format in the representation
// Go understands.
const DateLayout = "2006-01-02"

func truncateDate(t time.Time) time.Time {
	return t.Truncate(24 * time.Hour)
}

func (t dateT) Type() query.Type {
	return sqltypes.Date
}

func (t dateT) SQL(v interface{}) sqltypes.Value {
	time := MustConvert(t, v).(time.Time)
	return sqltypes.MakeTrusted(
		sqltypes.Timestamp,
		[]byte(time.Format(DateLayout)),
	)
}

func (t dateT) Convert(v interface{}) (interface{}, error) {
	switch value := v.(type) {
	case time.Time:
		return truncateDate(value).UTC(), nil
	case string:
		t, err := time.Parse(DateLayout, value)
		if err != nil {
			return nil, ErrConvertingToTime.Wrap(err, v)
		}
		return truncateDate(t).UTC(), nil
	default:
		ts, err := Int64.Convert(v)
		if err != nil {
			return nil, ErrInvalidType.New(reflect.TypeOf(v))
		}

		return truncateDate(time.Unix(ts.(int64), 0)).UTC(), nil
	}
}

func (t dateT) Compare(a, b interface{}) int {
	av := truncateDate(a.(time.Time))
	bv := truncateDate(b.(time.Time))
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

// IsNumber checks if t is a number type
func IsNumber(t Type) bool {
	return IsSigned(t) || IsUnsigned(t) || IsDecimal(t)
}

// IsSigned checks if t is a signed type.
func IsSigned(t Type) bool {
	return t == Int32 || t == Int64
}

// IsUnsigned checks if t is an unsigned type.
func IsUnsigned(t Type) bool {
	return t == Uint32 || t == Uint64
}

// IsDecimal checks if t is decimal type.
func IsDecimal(t Type) bool {
	return t == Float32 || t == Float64
}

// IsText checks if t is a text type.
func IsText(t Type) bool {
	return t == Text || t == Blob || t == JSON
}
