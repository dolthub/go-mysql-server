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

var (
	// ErrTypeNotSupported is thrown when a specific type is not supported
	ErrTypeNotSupported = errors.NewKind("Type not supported: %s")

	// ErrUnexpectedType is thrown when a received type is not the expected
	ErrUnexpectedType = errors.NewKind("value at %d has unexpected type: %s")

	// ErrConvertingToTime is thrown when a value cannot be converted to a Time
	ErrConvertingToTime = errors.NewKind("value %q can't be converted to time.Time")

	// ErrValueNotNil is thrown when a value that was expected to be nil, is not
	ErrValueNotNil = errors.NewKind("value not nil: %#v")

	// ErrNotTuple is retuned when the value is not a tuple.
	ErrNotTuple = errors.NewKind("value of type %T is not a tuple")

	// ErrInvalidColumnNumber is returned when a tuple has an invalid number of
	// arguments.
	ErrInvalidColumnNumber = errors.NewKind("tuple should contain %d column(s), but has %d")

	// ErrNotArray is returned when the value is not an array.
	ErrNotArray = errors.NewKind("value of type %T is not an array")
)

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
func (s Schema) Contains(column string, source string) bool {
	return s.IndexOf(column, source) >= 0
}

// IndexOf returns the index of the given column in the schema or -1 if it's
// not present.
func (s Schema) IndexOf(column, source string) int {
	for i, col := range s {
		if col.Name == column && col.Source == source {
			return i
		}
	}
	return -1
}

// Equals checks whether the given schema is equal to this one.
func (s Schema) Equals(s2 Schema) bool {
	if len(s) != len(s2) {
		return false
	}

	for i := range s {
		if !s[i].Equals(s2[i]) {
			return false
		}
	}

	return true
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

// Equals checks whether two columns are equal.
func (c *Column) Equals(c2 *Column) bool {
	return c.Name == c2.Name &&
		c.Source == c2.Source &&
		c.Nullable == c2.Nullable &&
		reflect.DeepEqual(c.Default, c2.Default) &&
		reflect.DeepEqual(c.Type, c2.Type)
}

// Type represent a SQL type.
type Type interface {
	// Type returns the query.Type for the given Type.
	Type() query.Type
	// Covert a value of a compatible type to a most accurate type.
	Convert(interface{}) (interface{}, error)
	// Compare returns an integer comparing two values.
	// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
	Compare(interface{}, interface{}) (int, error)
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

// Tuple returns a new tuple type with the given element types.
func Tuple(types ...Type) Type {
	return tupleT(types)
}

// Array returns a new Array type of the given underlying type.
func Array(underlying Type) Type {
	return arrayT{underlying}
}

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
func (t nullT) Compare(a interface{}, b interface{}) (int, error) {
	return 0, nil
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
func (t numberT) Compare(a interface{}, b interface{}) (int, error) {
	if IsUnsigned(t) {
		return compareUnsigned(a, b)
	}

	return compareSigned(a, b)
}

func compareSigned(a interface{}, b interface{}) (int, error) {
	ca, err := cast.ToInt64E(a)
	if err != nil {
		return 0, err
	}
	cb, err := cast.ToInt64E(b)
	if err != nil {
		return 0, err
	}

	if ca == cb {
		return 0, nil
	}

	if ca < cb {
		return -1, nil
	}

	return +1, nil
}

func compareUnsigned(a interface{}, b interface{}) (int, error) {
	ca, err := cast.ToUint64E(a)
	if err != nil {
		return 0, err
	}
	cb, err := cast.ToUint64E(b)
	if err != nil {
		return 0, err
	}

	if ca == cb {
		return 0, nil
	}

	if ca < cb {
		return -1, nil
	}

	return +1, nil
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
func (t timestampT) Compare(a interface{}, b interface{}) (int, error) {
	av := a.(time.Time)
	bv := b.(time.Time)
	if av.Before(bv) {
		return -1, nil
	} else if av.After(bv) {
		return 1, nil
	}
	return 0, nil
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

func (t dateT) Compare(a, b interface{}) (int, error) {
	av := truncateDate(a.(time.Time))
	bv := truncateDate(b.(time.Time))
	if av.Before(bv) {
		return -1, nil
	} else if av.After(bv) {
		return 1, nil
	}
	return 0, nil
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
func (t textT) Compare(a interface{}, b interface{}) (int, error) {
	return strings.Compare(a.(string), b.(string)), nil
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
func (t booleanT) Compare(a interface{}, b interface{}) (int, error) {
	if a == b {
		return 0, nil
	}

	if a.(bool) == false {
		return -1, nil
	}

	return +1, nil
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
func (t blobT) Compare(a interface{}, b interface{}) (int, error) {
	return bytes.Compare(a.([]byte), b.([]byte)), nil
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
func (t jsonT) Compare(a interface{}, b interface{}) (int, error) {
	return bytes.Compare(a.([]byte), b.([]byte)), nil
}

type tupleT []Type

func (t tupleT) Type() query.Type {
	return sqltypes.Expression
}

func (t tupleT) SQL(v interface{}) sqltypes.Value {
	panic("unable to convert tuple type to SQL")
}

func (t tupleT) Convert(v interface{}) (interface{}, error) {
	if vals, ok := v.([]interface{}); ok {
		if len(vals) != len(t) {
			return nil, ErrInvalidColumnNumber.New(len(t), len(vals))
		}

		var result = make([]interface{}, len(t))
		for i, typ := range t {
			var err error
			result[i], err = typ.Convert(vals[i])
			if err != nil {
				return nil, err
			}
		}

		return result, nil
	}
	return nil, ErrNotTuple.New(v)
}

func (t tupleT) Compare(a, b interface{}) (int, error) {
	a, err := t.Convert(a)
	if err != nil {
		return 0, err
	}

	b, err = t.Convert(b)
	if err != nil {
		return 0, err
	}

	left := a.([]interface{})
	right := b.([]interface{})
	for i := range left {
		cmp, err := t[i].Compare(left[i], right[i])
		if err != nil {
			return 0, err
		}

		if cmp != 0 {
			return cmp, nil
		}
	}

	return 0, nil
}

type arrayT struct {
	underlying Type
}

func (t arrayT) Type() query.Type {
	return sqltypes.TypeJSON
}

func (t arrayT) SQL(v interface{}) sqltypes.Value {
	return JSON.SQL(v)
}

func (t arrayT) Convert(v interface{}) (interface{}, error) {
	if vals, ok := v.([]interface{}); ok {
		var result = make([]interface{}, len(vals))
		for i, v := range vals {
			var err error
			result[i], err = t.underlying.Convert(v)
			if err != nil {
				return nil, err
			}
		}

		return result, nil
	}
	return nil, ErrNotArray.New(v)
}

func (t arrayT) Compare(a, b interface{}) (int, error) {
	a, err := t.Convert(a)
	if err != nil {
		return 0, err
	}

	b, err = t.Convert(b)
	if err != nil {
		return 0, err
	}

	left := a.([]interface{})
	right := b.([]interface{})

	if len(left) < len(right) {
		return -1, nil
	} else if len(left) > len(right) {
		return 1, nil
	}

	for i := range left {
		cmp, err := t.underlying.Compare(left[i], right[i])
		if err != nil {
			return 0, err
		}

		if cmp != 0 {
			return cmp, nil
		}
	}

	return 0, nil
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
	return IsInteger(t) || IsDecimal(t)
}

// IsSigned checks if t is a signed type.
func IsSigned(t Type) bool {
	return t == Int32 || t == Int64
}

// IsUnsigned checks if t is an unsigned type.
func IsUnsigned(t Type) bool {
	return t == Uint32 || t == Uint64
}

// IsInteger check if t is a (U)Int32/64 type
func IsInteger(t Type) bool {
	return IsSigned(t) || IsUnsigned(t)
}

// IsDecimal checks if t is decimal type.
func IsDecimal(t Type) bool {
	return t == Float32 || t == Float64
}

// IsText checks if t is a text type.
func IsText(t Type) bool {
	return t == Text || t == Blob || t == JSON
}

// IsTuple checks if t is a tuple type.
// Note that tupleT instances with just 1 value are not considered
// as a tuple, but a parenthesized value.
func IsTuple(t Type) bool {
	v, ok := t.(tupleT)
	return ok && len(v) > 1
}

// IsArray returns whether the given type is an array.
func IsArray(t Type) bool {
	_, ok := t.(arrayT)
	return ok
}

// NumColumns returns the number of columns in a type. This is one for all
// types, except tuples.
func NumColumns(t Type) int {
	v, ok := t.(tupleT)
	if !ok {
		return 1
	}
	return len(v)
}
