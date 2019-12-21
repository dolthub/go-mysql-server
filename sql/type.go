package sql

import (
	"encoding/json"
	"fmt"
	"gopkg.in/src-d/go-errors.v1"
	"io"
	"strconv"
	"time"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

var (
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
	// Compare returns an integer comparing two values.
	// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
	Compare(interface{}, interface{}) (int, error)
	// Convert a value of a compatible type to a most accurate type.
	Convert(interface{}) (interface{}, error)
	// MustConvert converts a value of a compatible type to a most accurate type, causing a panic on failure.
	MustConvert(interface{}) interface{}
	// Promote will promote the current type to the largest representing type of the same kind, such as Int8 to Int64.
	Promote() Type
	// SQL returns the sqltypes.Value for the given value.
	SQL(interface{}) (sqltypes.Value, error)
	// Type returns the query.Type for the given Type.
	Type() query.Type
	// Zero returns the golang zero value for this type
	Zero() interface{}
	fmt.Stringer
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
	case "decimal", "fixed", "dec", "numeric":
		precision := int64(0)
		scale := int64(0)
		if ct.Length != nil {
			var err error
			precision, err = strconv.ParseInt(string(ct.Length.Val), 10, 8)
			if err != nil {
				return nil, err
			}
		}
		if ct.Scale != nil {
			var err error
			scale, err = strconv.ParseInt(string(ct.Scale.Val), 10, 8)
			if err != nil {
				return nil, err
			}
		}
		return CreateDecimalType(uint8(precision), uint8(scale))
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

func ConvertToBool(v interface{}) (bool, error) {
	switch b := v.(type) {
	case bool:
		if b {
			return true, nil
		}
		return false, nil
	case int:
		return ConvertToBool(int64(b))
	case int64:
		if b == 0 {
			return false, nil
		}
		return true, nil
	case int32:
		return ConvertToBool(int64(b))
	case int16:
		return ConvertToBool(int64(b))
	case int8:
		return ConvertToBool(int64(b))
	case uint:
		return ConvertToBool(int64(b))
	case uint64:
		return ConvertToBool(int64(b))
	case uint32:
		return ConvertToBool(int64(b))
	case uint16:
		return ConvertToBool(int64(b))
	case uint8:
		return ConvertToBool(int64(b))
	case time.Duration:
		if b == 0 {
			return false, nil
		}
		return true, nil
	case time.Time:
		if b.UnixNano() == 0 {
			return false, nil
		}
		return true, nil
	case float32:
		return ConvertToBool(float64(b))
	case float64:
		if b == 0 {
			return false, nil
		}
		return true, nil
	case string:
		return false, nil
	case nil:
		return false, fmt.Errorf("unable to cast nil to bool")
	default:
		return false, fmt.Errorf("unable to cast %#v of type %T to bool", v, v)
	}
}

// IsArray returns whether the given type is an array.
func IsArray(t Type) bool {
	_, ok := t.(arrayType)
	return ok
}

// IsBlob checks if t is BINARY, VARBINARY, or BLOB
func IsBlob(t Type) bool {
	switch t.Type() {
	case sqltypes.Binary, sqltypes.VarBinary, sqltypes.Blob:
		return true
	default:
		return false
	}
}

// IsFloat checks if t is float type.
func IsFloat(t Type) bool {
	return t == Float32 || t == Float64
}

// IsInteger checks if t is an integer type.
func IsInteger(t Type) bool {
	return IsSigned(t) || IsUnsigned(t)
}

// IsNull returns true if expression is nil or is Null Type, otherwise false.
func IsNull(ex Expression) bool {
	return ex == nil || ex.Type() == Null
}

// IsNumber checks if t is a number type
func IsNumber(t Type) bool {
	_, ok := t.(numberTypeImpl)
	return ok
}

// IsSigned checks if t is a signed type.
func IsSigned(t Type) bool {
	return t == Int8 || t == Int16 || t == Int32 || t == Int64
}

// IsText checks if t is a text type.
func IsText(t Type) bool {
	_, ok := t.(stringType)
	return ok || t == JSON
}

// IsTextOnly checks if t is CHAR, VARCHAR, or one of the TEXTs.
func IsTextOnly(t Type) bool {
	switch t.Type() {
	case sqltypes.Char, sqltypes.VarChar, sqltypes.Text:
		return true
	default:
		return false
	}
}

// IsTime checks if t is a timestamp, date or datetime
func IsTime(t Type) bool {
	_, ok := t.(datetimeType)
	return ok
}

// IsTuple checks if t is a tuple type.
// Note that tupleType instances with just 1 value are not considered
// as a tuple, but a parenthesized value.
func IsTuple(t Type) bool {
	v, ok := t.(tupleType)
	return ok && len(v) > 1
}

// IsUnsigned checks if t is an unsigned type.
func IsUnsigned(t Type) bool {
	return t == Uint8 || t == Uint16 || t == Uint32 || t == Uint64
}

// NumColumns returns the number of columns in a type. This is one for all
// types, except tuples.
func NumColumns(t Type) int {
	v, ok := t.(tupleType)
	if !ok {
		return 1
	}
	return len(v)
}

// UnderlyingType returns the underlying type of an array if the type is an
// array, or the type itself in any other case.
func UnderlyingType(t Type) Type {
	a, ok := t.(arrayType)
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
	case arrayType:
		return convertArrayForJSON(t, v)
	default:
		return t.Convert(v)
	}
}

func convertArrayForJSON(t arrayType, v interface{}) (interface{}, error) {
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
