package sql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/spf13/cast"
	"gopkg.in/src-d/go-errors.v1"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

var (
	// ErrTypeNotSupported is thrown when a specific type is not supported
	ErrTypeNotSupported = errors.NewKind("BaseType not supported: %s")

	// ErrCharTruncation is thrown when a Char value is textually longer than the destination capacity
	ErrCharTruncation = errors.NewKind("string value of %q is longer than destination capacity %d")

	// ErrVarCharTruncation is thrown when a VarChar value is textually longer than the destination capacity
	ErrVarCharTruncation = errors.NewKind("string value of %q is longer than destination capacity %d")

	// ErrNotTuple is returned when the value is not a tuple.
	ErrNotTuple = errors.NewKind("value of type %T is not a tuple")

	// ErrInvalidColumnNumber is returned when a tuple has an invalid number of
	// arguments.
	ErrInvalidColumnNumber = errors.NewKind("tuple should contain %d column(s), but has %d")

	ErrInvalidBaseType = errors.NewKind("%v is not a valid %v base type")

	// ErrNotArray is returned when the value is not an array.
	ErrNotArray = errors.NewKind("value of type %T is not an array")

	// ErrConvertToSQL is returned when Convert failed.
	// It makes an error less verbose comparing to what spf13/cast returns.
	ErrConvertToSQL = errors.NewKind("incompatible conversion to SQL type: %s")
)

// Type represents a SQL type.
type Type interface {
	// BaseType returns the BaseType for the given Type.
	BaseType() BaseType
	// Compare returns an integer comparing two values.
	// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
	Compare(interface{}, interface{}) (int, error)
	// Convert a value of a compatible type to a most accurate type.
	Convert(interface{}) (interface{}, error)
	// SQL returns the sqltypes.Value for the given value.
	SQL(interface{}) (sqltypes.Value, error)
	// Zero returns the golang zero value for this type
	Zero() interface{}
	fmt.Stringer
}

var maxTime = time.Date(9999, time.December, 31, 23, 59, 59, 999999999, time.UTC)

// ValidateTime receives a time and returns either that time or nil if it's
// not a valid time.
func ValidateTime(t time.Time) interface{} {
	if t.After(maxTime) {
		return nil
	}
	return t
}

var (
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

// Char returns a new Char type of the given length.
func Char(length int) Type {
	return charT{length: length}
}

// VarChar returns a new VarChar type of the given length.
func VarChar(length int) Type {
	return varCharT{length: length}
}

// MysqlTypeToType gets the column type using the mysql type
func MysqlTypeToType(sql query.Type) (Type, error) {
	switch sql {
	case sqltypes.Null:
		return Null, nil
	case sqltypes.Int8:
		return Int8, nil
	case sqltypes.Uint8:
		return Uint8, nil
	case sqltypes.Int16:
		return Int16, nil
	case sqltypes.Uint16:
		return Uint16, nil
	case sqltypes.Int24:
		return Int24, nil
	case sqltypes.Uint24:
		return Uint24, nil
	case sqltypes.Int32:
		return Int32, nil
	case sqltypes.Uint32:
		return Uint32, nil
	case sqltypes.Int64:
		return Int64, nil
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
	case sqltypes.Text:
		return Text, nil
	case sqltypes.Char:
		// Since we can't get the size of the sqltypes.Char to instantiate a
		// specific Char(length) type we return a Text here
		return Text, nil
	case sqltypes.VarChar:
		// Since we can't get the size of the sqltypes.VarChar to instantiate a
		// specific VarChar(length) type we return a Text here
		return Text, nil
	case sqltypes.Datetime:
		return Datetime, nil
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

// IsNull returns true if expression is nil or is Null BaseType, otherwise false.
func IsNull(ex Expression) bool {
	return ex == nil || ex.Type() == Null
}

func compareFloats(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	ca, err := cast.ToFloat64E(a)
	if err != nil {
		return 0, err
	}
	cb, err := cast.ToFloat64E(b)
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

func compareSignedInts(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

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

func compareUnsignedInts(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

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

type charT struct {
	length int
}

func (t charT) Zero() interface{} {
	return ""
}

func (t charT) Capacity() int { return t.length }

func (t charT) String() string { return fmt.Sprintf("CHAR(%d)", t.length) }

func (t charT) BaseType() BaseType {
	return BaseType_CHAR
}

func (t charT) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.MakeTrusted(sqltypes.Char, nil), nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	return sqltypes.MakeTrusted(sqltypes.Char, []byte(v.(string))), nil
}

// Converts any value that can be casted to a string
func (t charT) Convert(v interface{}) (interface{}, error) {
	val, err := cast.ToStringE(v)
	if err != nil {
		return nil, ErrConvertToSQL.New(t)
	}

	if len(val) > t.length {
		return nil, ErrCharTruncation.New(val, t.length)
	}
	return val, nil
}

// Compares two strings lexicographically
func (t charT) Compare(a interface{}, b interface{}) (int, error) {
	return strings.Compare(a.(string), b.(string)), nil
}

type varCharT struct {
	length int
}

func (t varCharT) Zero() interface{} {
	return ""
}

func (t varCharT) Capacity() int { return t.length }

func (t varCharT) String() string { return fmt.Sprintf("VARCHAR(%d)", t.length) }

// BaseType implements Type interface
func (t varCharT) BaseType() BaseType {
	return BaseType_VARCHAR
}

// SQL implements Type interface
func (t varCharT) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.MakeTrusted(sqltypes.VarChar, nil), nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	return sqltypes.MakeTrusted(sqltypes.VarChar, []byte(v.(string))), nil
}

// Convert implements Type interface
func (t varCharT) Convert(v interface{}) (interface{}, error) {
	val, err := cast.ToStringE(v)
	if err != nil {
		return nil, ErrConvertToSQL.New(t)
	}

	if len(val) > t.length {
		return nil, ErrVarCharTruncation.New(val, t.length)
	}
	return val, nil
}

// Compare implements Type interface.
func (t varCharT) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}
	return strings.Compare(a.(string), b.(string)), nil
}

type textT struct{}

func (t textT) Zero() interface{} {
	return ""
}

func (t textT) String() string { return "TEXT" }

// BaseType implements Type interface.
func (t textT) BaseType() BaseType {
	return BaseType_TEXT
}

// SQL implements Type interface.
func (t textT) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	return sqltypes.MakeTrusted(sqltypes.Text, []byte(v.(string))), nil
}

// Convert implements Type interface.
func (t textT) Convert(v interface{}) (interface{}, error) {
	val, err := cast.ToStringE(v)
	if err != nil {
		return nil, ErrConvertToSQL.New(t)
	}
	return val, nil
}

// Compare implements Type interface.
func (t textT) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}
	return strings.Compare(a.(string), b.(string)), nil
}

type booleanT struct{}

func (t booleanT) Zero() interface{} {
	return false
}

func (t booleanT) String() string { return "BOOLEAN" }

// BaseType implements Type interface.
func (t booleanT) BaseType() BaseType {
	return BaseType_BOOLEAN
}

// SQL implements Type interface.
func (t booleanT) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	b := []byte{'0'}
	if cast.ToBool(v) {
		b[0] = '1'
	}

	return sqltypes.MakeTrusted(sqltypes.Bit, b), nil
}

// Convert implements Type interface.
func (t booleanT) Convert(v interface{}) (interface{}, error) {
	switch b := v.(type) {
	case bool:
		return b, nil
	case int, int64, int32, int16, int8, uint, uint64, uint32, uint16, uint8:
		return b != 0, nil
	case time.Duration:
		return int64(b) != 0, nil
	case time.Time:
		return b.UnixNano() != 0, nil
	case float32, float64:
		return int(math.Round(v.(float64))) != 0, nil
	case string:
		return false, nil

	case nil:
		return nil, fmt.Errorf("unable to cast nil to bool")

	default:
		return nil, fmt.Errorf("unable to cast %#v of type %T to bool", v, v)
	}
}

// Compare implements Type interface.
func (t booleanT) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	if a == b {
		return 0, nil
	}

	if a == false {
		return -1, nil
	}

	return 1, nil
}

type blobT struct{}

func (t blobT) Zero() interface{} {
	return nil
}

func (t blobT) String() string { return "BLOB" }

// BaseType implements Type interface.
func (t blobT) BaseType() BaseType {
	return BaseType_BLOB
}

// SQL implements Type interface.
func (t blobT) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	return sqltypes.MakeTrusted(sqltypes.Blob, v.([]byte)), nil
}

// Convert implements Type interface.
func (t blobT) Convert(v interface{}) (interface{}, error) {
	switch value := v.(type) {
	case nil:
		return []byte(nil), nil
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
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}
	return bytes.Compare(a.([]byte), b.([]byte)), nil
}

type jsonT struct{}

func (t jsonT) Zero() interface{} {
	return []byte(`""`)
}

func (t jsonT) String() string { return "JSON" }

// BaseType implements Type interface.
func (t jsonT) BaseType() BaseType {
	return BaseType_JSON
}

// SQL implements Type interface.
func (t jsonT) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	return sqltypes.MakeTrusted(sqltypes.TypeJSON, v.([]byte)), nil
}

// Convert implements Type interface.
func (t jsonT) Convert(v interface{}) (interface{}, error) {
	switch v := v.(type) {
	case string:
		var doc interface{}
		if err := json.Unmarshal([]byte(v), &doc); err != nil {
			return json.Marshal(v)
		}
		return json.Marshal(doc)
	default:
		return json.Marshal(v)
	}
}

// Compare implements Type interface.
func (t jsonT) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}
	return bytes.Compare(a.([]byte), b.([]byte)), nil
}

type tupleT []Type

func (t tupleT) Zero() interface{} {
	zeroes := make([]interface{}, len(t))
	for i, tt := range t {
		zeroes[i] = tt.Zero()
	}
	return zeroes
}

func (t tupleT) String() string {
	var elems = make([]string, len(t))
	for i, el := range t {
		elems[i] = el.String()
	}
	return fmt.Sprintf("TUPLE(%s)", strings.Join(elems, ", "))
}

func (t tupleT) BaseType() BaseType {
	return BaseType_Internal_EXPRESSION
}

func (t tupleT) SQL(v interface{}) (sqltypes.Value, error) {
	return sqltypes.Value{}, fmt.Errorf("unable to convert tuple type to SQL")
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

func (t arrayT) Zero() interface{} {
	return nil
}

func (t arrayT) String() string { return fmt.Sprintf("ARRAY(%s)", t.underlying) }

func (t arrayT) BaseType() BaseType {
	return BaseType_JSON
}

func (t arrayT) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := convertForJSON(t, v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	val, err := json.Marshal(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	return sqltypes.MakeTrusted(sqltypes.TypeJSON, val), nil
}

func (t arrayT) Convert(v interface{}) (interface{}, error) {
	switch v := v.(type) {
	case []interface{}:
		var result = make([]interface{}, len(v))
		for i, v := range v {
			var err error
			result[i], err = t.underlying.Convert(v)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	case Generator:
		var values []interface{}
		for {
			val, err := v.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}

			val, err = t.underlying.Convert(val)
			if err != nil {
				return nil, err
			}

			values = append(values, val)
		}

		if err := v.Close(); err != nil {
			return nil, err
		}

		return values, nil
	default:
		return nil, ErrNotArray.New(v)
	}
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

// IsNumber checks if t is a number type
func IsNumber(t Type) bool {
	return IsInteger(t) || IsDecimal(t)
}

// IsSigned checks if t is a signed type.
func IsSigned(t Type) bool {
	return t == Int8 || t == Int16 || t == Int32 || t == Int64
}

// IsUnsigned checks if t is an unsigned type.
func IsUnsigned(t Type) bool {
	return t == Uint8 || t == Uint16 || t == Uint32 || t == Uint64
}

// IsInteger checks if t is a (U)Int32/64 type.
func IsInteger(t Type) bool {
	return IsSigned(t) || IsUnsigned(t)
}

// IsTime checks if t is a timestamp, date or datetime
func IsTime(t Type) bool {
	return t == Timestamp || t == Date || t == Datetime
}

// IsDecimal checks if t is decimal type.
func IsDecimal(t Type) bool {
	return t == Float32 || t == Float64
}

// IsText checks if t is a text type.
func IsText(t Type) bool {
	return t == Text || t == Blob || t == JSON || IsVarChar(t) || IsChar(t)
}

// IsChar checks if t is a Char type.
func IsChar(t Type) bool {
	_, ok := t.(charT)
	return ok
}

// IsVarChar checks if t is a varchar type.
func IsVarChar(t Type) bool {
	_, ok := t.(varCharT)
	return ok
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

// MySQLTypeName returns the MySQL display name for the given type.
func MySQLTypeName(t Type) string {
	switch t.BaseType() {
	case sqltypes.Int8:
		return "TINYINT"
	case sqltypes.Uint8:
		return "TINYINT UNSIGNED"
	case sqltypes.Int16:
		return "SMALLINT"
	case sqltypes.Uint16:
		return "SMALLINT UNSIGNED"
	case sqltypes.Int32:
		return "INTEGER"
	case sqltypes.Int64:
		return "BIGINT"
	case sqltypes.Uint32:
		return "INTEGER UNSIGNED"
	case sqltypes.Uint64:
		return "BIGINT UNSIGNED"
	case sqltypes.Float32:
		return "FLOAT"
	case sqltypes.Float64:
		return "DOUBLE"
	case sqltypes.Timestamp:
		return "TIMESTAMP"
	case sqltypes.Datetime:
		return "DATETIME"
	case sqltypes.Date:
		return "DATE"
	case sqltypes.Char:
		return fmt.Sprintf("CHAR(%v)", t.(charT).Capacity())
	case sqltypes.VarChar:
		return fmt.Sprintf("VARCHAR(%v)", t.(varCharT).Capacity())
	case sqltypes.Text:
		return "TEXT"
	case sqltypes.Bit:
		return "BIT"
	case sqltypes.TypeJSON:
		return "JSON"
	case sqltypes.Blob:
		return "BLOB"
	default:
		return "UNKNOWN"
	}
}

// UnderlyingType returns the underlying type of an array if the type is an
// array, or the type itself in any other case.
func UnderlyingType(t Type) Type {
	a, ok := t.(arrayT)
	if !ok {
		return t
	}

	return a.underlying
}

func convertForJSON(t Type, v interface{}) (interface{}, error) {
	switch t := t.(type) {
	case jsonT:
		val, err := t.Convert(v)
		if err != nil {
			return nil, err
		}

		var doc interface{}
		err = json.Unmarshal(val.([]byte), &doc)
		if err != nil {
			return nil, err
		}

		return doc, nil
	case arrayT:
		return convertArrayForJSON(t, v)
	default:
		return t.Convert(v)
	}
}

func convertArrayForJSON(t arrayT, v interface{}) (interface{}, error) {
	switch v := v.(type) {
	case []interface{}:
		var result = make([]interface{}, len(v))
		for i, v := range v {
			var err error
			result[i], err = convertForJSON(t.underlying, v)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	case Generator:
		var values []interface{}
		for {
			val, err := v.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}

			val, err = convertForJSON(t.underlying, val)
			if err != nil {
				return nil, err
			}

			values = append(values, val)
		}

		if err := v.Close(); err != nil {
			return nil, err
		}

		return values, nil
	default:
		return nil, ErrNotArray.New(v)
	}
}

// compareNulls compares two values, and returns true if either is null.
// The returned integer represents the ordering, with a rule that states nulls
// as being ordered before non-nulls.
func compareNulls(a interface{}, b interface{}) (bool, int) {
	aIsNull := a == nil
	bIsNull := b == nil
	if aIsNull && bIsNull {
		return true, 0
	} else if aIsNull && !bIsNull {
		return true, -1
	} else if !aIsNull && bIsNull {
		return true, 1
	}
	return false, 0
}
