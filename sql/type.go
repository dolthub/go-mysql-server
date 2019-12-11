package sql

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"vitess.io/vitess/go/vt/sqlparser"

	"gopkg.in/src-d/go-errors.v1"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

var (
	// ErrTypeNotSupported is thrown when a specific type is not supported
	ErrTypeNotSupported = errors.NewKind("Type not supported: %s")

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

	ErrConvertToByteArray = errors.NewKind("cannot encode type as byte array: %T")
)

// Type represents a SQL type.
type Type interface {
	// Type returns the query.Type for the given Type.
	Type() query.Type
	// Compare returns an integer comparing two values.
	// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
	Compare(interface{}, interface{}) (int, error)
	// Convert a value of a compatible type to a most accurate type.
	Convert(interface{}) (interface{}, error)
	// MustConvert converts a value of a compatible type to a most accurate type, causing a panic on failure.
	MustConvert(interface{}) interface{}
	// SQL returns the sqltypes.Value for the given value.
	SQL(interface{}) (sqltypes.Value, error)
	// Zero returns the golang zero value for this type
	Zero() interface{}
	fmt.Stringer
}

var (
	// JSON is a type that holds any valid JSON object.
	JSON jsonType
)

// Tuple returns a new tuple type with the given element types.
func Tuple(types ...Type) Type {
	return tupleT(types)
}

// Array returns a new Array type of the given underlying type.
func Array(underlying Type) Type {
	return arrayT{underlying}
}

// ColumnTypeToType gets the column type using the column definition.
func ColumnTypeToType(ct *sqlparser.ColumnType) (Type, error) {
	switch ct.Type {
	case "boolean", "bool":
		return Int8, nil
	case "tinyint":
		if ct.Unsigned {
			return Uint8, nil
		}
		return Int8, nil
	case "smallint":
		if ct.Unsigned {
			return Uint16, nil
		}
		return Int16, nil
	case "mediumint":
		if ct.Unsigned {
			return Uint24, nil
		}
		return Int24, nil
	case "int", "integer":
		if ct.Unsigned {
			return Uint32, nil
		}
		return Int32, nil
	case "bigint":
		if ct.Unsigned {
			return Uint64, nil
		}
		return Int64, nil
	case "float":
		return Float32, nil
	case "double", "real":
		return Float64, nil
	case "decimal", "fixed", "dec":
	case "bit":
		length := int64(1)
		if ct.Length != nil {
			var err error
			length, err = strconv.ParseInt(string(ct.Length.Val), 10, 8)
			if err != nil {
				return nil, err
			}
		}
		return CreateBitType(uint8(length))
	case "tinyblob":
		return TinyBlob, nil
	case "blob":
		if ct.Length == nil {
			return Blob, nil
		}
		length, err := strconv.ParseInt(string(ct.Length.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		return CreateBlob(sqltypes.Blob, length)
	case "mediumblob":
		return MediumBlob, nil
	case "longblob":
		return LongBlob, nil
	case "tinytext":
		collation, err := ParseCollation(&ct.Charset, &ct.Collate, false)
		if err != nil {
			return nil, err
		}
		return CreateString(sqltypes.Text, tinyTextBlobMax / collation.CharacterSet().MaxLength(), collation)
	case "text":
		collation, err := ParseCollation(&ct.Charset, &ct.Collate, false)
		if err != nil {
			return nil, err
		}
		if ct.Length == nil {
			return CreateString(sqltypes.Text, textBlobMax / collation.CharacterSet().MaxLength(), collation)
		}
		length, err := strconv.ParseInt(string(ct.Length.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		return CreateString(sqltypes.Text, length, collation)
	case "mediumtext", "long":
		collation, err := ParseCollation(&ct.Charset, &ct.Collate, false)
		if err != nil {
			return nil, err
		}
		return CreateString(sqltypes.Text, mediumTextBlobMax / collation.CharacterSet().MaxLength(), collation)
	case "longtext":
		collation, err := ParseCollation(&ct.Charset, &ct.Collate, false)
		if err != nil {
			return nil, err
		}
		return CreateString(sqltypes.Text, longTextBlobMax / collation.CharacterSet().MaxLength(), collation)
	case "char", "character":
		collation, err := ParseCollation(&ct.Charset, &ct.Collate, false)
		if err != nil {
			return nil, err
		}
		length := int64(1)
		if ct.Length != nil {
			var err error
			length, err = strconv.ParseInt(string(ct.Length.Val), 10, 64)
			if err != nil {
				return nil, err
			}
		}
		return CreateString(sqltypes.Char, length, collation)
	case "varchar":
		collation, err := ParseCollation(&ct.Charset, &ct.Collate, false)
		if err != nil {
			return nil, err
		}
		if ct.Length == nil {
			return nil, fmt.Errorf("VARCHAR requires a length")
		}
		length, err := strconv.ParseInt(string(ct.Length.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		return CreateString(sqltypes.VarChar, length, collation)
	case "binary":
		length := int64(1)
		if ct.Length != nil {
			var err error
			length, err = strconv.ParseInt(string(ct.Length.Val), 10, 64)
			if err != nil {
				return nil, err
			}
		}
		return CreateString(sqltypes.Binary, length, Collation_binary)
	case "varbinary":
		if ct.Length == nil {
			return nil, fmt.Errorf("VARBINARY requires a length")
		}
		length, err := strconv.ParseInt(string(ct.Length.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		return CreateString(sqltypes.VarBinary, length, Collation_binary)
	case "year":
		return Year, nil
	case "date":
		return Date, nil
	case "time":
		return Time, nil
	case "timestamp":
		return Timestamp, nil
	case "datetime":
		return Datetime, nil
	case "enum":
	case "set":
	case "json":
		return JSON, nil
	case "geometry":
	case "geometrycollection":
	case "linestring":
	case "multilinestring":
	case "point":
	case "multipoint":
	case "polygon":
	case "multipolygon":
	default:
		return nil, fmt.Errorf("unknown type: %v", ct.Type)
	}
	return nil, fmt.Errorf("type not yet implemented: %v", ct.Type)
}

// IsNull returns true if expression is nil or is Null Type, otherwise false.
func IsNull(ex Expression) bool {
	return ex == nil || ex.Type() == Null
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

func (t tupleT) Type() query.Type {
	return sqltypes.Expression
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

func (t tupleT) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
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

func (t arrayT) Type() query.Type {
	return sqltypes.TypeJSON
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

func (t arrayT) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
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
	_, ok := t.(stringType)
	return ok || t == JSON
}

// IsChar checks if t is a Char type.
func IsChar(t Type) bool {
	if st, ok := t.(stringType); ok {
		if st.baseType == sqltypes.Char {
			return true
		}
	}
	return false
}

// IsVarChar checks if t is a varchar type.
func IsVarChar(t Type) bool {
	if st, ok := t.(stringType); ok {
		if st.baseType == sqltypes.VarChar {
			return true
		}
	}
	return false
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
	case jsonType:
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

func BooleanConcrete(v interface{}) bool {
	if v == False {
		return false
	}
	return true
}

func BooleanParse(v interface{}) (int8, error) {
	switch b := v.(type) {
	case bool:
		if b {
			return True, nil
		}
		return False, nil
	case int, int64, int32, int16, int8, uint, uint64, uint32, uint16, uint8:
		if b == 0 {
			return False, nil
		}
		return True, nil
	case time.Duration:
		if b == 0 {
			return False, nil
		}
		return True, nil
	case time.Time:
		if b.UnixNano() == 0 {
			return False, nil
		}
		return True, nil
	case float32, float64:
		if b == 0 {
			return False, nil
		}
		return True, nil
	case string:
		return False, nil
	case nil:
		return False, fmt.Errorf("unable to cast nil to bool")
	default:
		return False, fmt.Errorf("unable to cast %#v of type %T to bool", v, v)
	}
}
