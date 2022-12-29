// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/shopspring/decimal"
	"gopkg.in/src-d/go-errors.v1"
)

var (
	// ErrNotTuple is returned when the value is not a tuple.
	ErrNotTuple = errors.NewKind("value of type %T is not a tuple")

	// ErrInvalidColumnNumber is returned when a tuple has an invalid number of
	// arguments.
	ErrInvalidColumnNumber = errors.NewKind("tuple should contain %d column(s), but has %d")

	ErrInvalidBaseType = errors.NewKind("%v is not a valid %v base type")

	// ErrConvertToSQL is returned when Convert failed.
	// It makes an error less verbose comparing to what spf13/cast returns.
	ErrConvertToSQL = errors.NewKind("incompatible conversion to SQL type: %s")
)

const (
	// DateLayout is the layout of the MySQL date format in the representation
	// Go understands.
	DateLayout = "2006-01-02"

	// TimestampDatetimeLayout is the formatting string with the layout of the timestamp
	// using the format of Go "time" package.
	TimestampDatetimeLayout = "2006-01-02 15:04:05.999999"
)

const (
	// False is the numeric representation of False as defined by MySQL.
	False = int8(0)
	// True is the numeric representation of True as defined by MySQL.
	True = int8(1)
)

// Type represents a SQL type.
type Type interface {
	// Compare returns an integer comparing two values.
	// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
	Compare(interface{}, interface{}) (int, error)
	// Convert a value of a compatible type to a most accurate type.
	Convert(interface{}) (interface{}, error)
	// Equals returns whether the given type is equivalent to the calling type. All parameters are included in the
	// comparison, so ENUM("a", "b") is not equivalent to ENUM("a", "b", "c").
	Equals(otherType Type) bool
	// MaxTextResponseByteLength returns the maximum number of bytes needed to serialize an instance of this type as a string in a response over the wire for MySQL's text protocol – in other words, this is the maximum bytes needed to serialize any value of this type as human-readable text, NOT in a more compact, binary representation.
	MaxTextResponseByteLength() uint32
	// Promote will promote the current type to the largest representing type of the same kind, such as Int8 to Int64.
	Promote() Type
	// SQL returns the sqltypes.Value for the given value.
	// Implementations can optionally use |dest| to append
	// serialized data, but should not mutate existing data.
	SQL(ctx *Context, dest []byte, v interface{}) (sqltypes.Value, error)
	// Type returns the query.Type for the given Type.
	Type() query.Type
	// ValueType returns the Go type of the value returned by Convert().
	ValueType() reflect.Type
	// Zero returns the golang zero value for this type
	Zero() interface{}
	fmt.Stringer
}

type NullType interface {
	Type
}

// NumberType represents all integer and floating point types.
// https://dev.mysql.com/doc/refman/8.0/en/integer-types.html
// https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html
// The type of the returned value is one of the following: int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64.
type NumberType interface {
	Type
	IsSigned() bool
	IsFloat() bool
}

// DatetimeType represents DATE, DATETIME, and TIMESTAMP.
// https://dev.mysql.com/doc/refman/8.0/en/datetime.html
// The type of the returned value is time.Time.
type DatetimeType interface {
	Type
	ConvertWithoutRangeCheck(v interface{}) (time.Time, error)
	MaximumTime() time.Time
	MinimumTime() time.Time
}

// TimeType represents the TIME type.
// https://dev.mysql.com/doc/refman/8.0/en/time.html
// TIME is implemented as TIME(6).
// The type of the returned value is Timespan.
// TODO: implement parameters on the TIME type
type TimeType interface {
	Type
	// ConvertToTimespan returns a Timespan from the given interface. Follows the same conversion rules as
	// Convert(), in that this will process the value based on its base-10 visual representation (for example, Convert()
	// will interpret the value `1234` as 12 minutes and 34 seconds). Returns an error for nil values.
	ConvertToTimespan(v interface{}) (types.Timespan, error)
	// ConvertToTimeDuration returns a time.Duration from the given interface. Follows the same conversion rules as
	// Convert(), in that this will process the value based on its base-10 visual representation (for example, Convert()
	// will interpret the value `1234` as 12 minutes and 34 seconds). Returns an error for nil values.
	ConvertToTimeDuration(v interface{}) (time.Duration, error)
	// MicrosecondsToTimespan returns a Timespan from the given number of microseconds. This differs from Convert(), as
	// that will process the value based on its base-10 visual representation (for example, Convert() will interpret
	// the value `1234` as 12 minutes and 34 seconds). This clamps the given microseconds to the allowed range.
	MicrosecondsToTimespan(v int64) types.Timespan
}

// YearType represents the YEAR type.
// https://dev.mysql.com/doc/refman/8.0/en/year.html
// The type of the returned value is int16.
type YearType interface {
	Type
}

// SetType represents the SET type.
// https://dev.mysql.com/doc/refman/8.0/en/set.html
// The type of the returned value is uint64.
type SetType interface {
	Type
	CharacterSet() CharacterSetID
	Collation() CollationID
	// NumberOfElements returns the number of elements in this set.
	NumberOfElements() uint16
	// BitsToString takes a previously-converted value and returns it as a string.
	BitsToString(bits uint64) (string, error)
	// Values returns all of the set's values in ascending order according to their corresponding bit value.
	Values() []string
}

// EnumType represents the ENUM type.
// https://dev.mysql.com/doc/refman/8.0/en/enum.html
// The type of the returned value is uint16.
type EnumType interface {
	Type
	// At returns the string at the given index, as well if the string was found.
	At(index int) (string, bool)
	CharacterSet() CharacterSetID
	Collation() CollationID
	// IndexOf returns the index of the given string. If the string was not found, then this returns -1.
	IndexOf(v string) int
	// NumberOfElements returns the number of enumerations.
	NumberOfElements() uint16
	// Values returns the elements, in order, of every enumeration.
	Values() []string
}

// DecimalType represents the DECIMAL type.
// https://dev.mysql.com/doc/refman/8.0/en/fixed-point-types.html
// The type of the returned value is decimal.Decimal.
type DecimalType interface {
	Type
	// ConvertToNullDecimal converts the given value to a decimal.NullDecimal if it has a compatible type. It is worth
	// noting that Convert() returns a nil value for nil inputs, and also returns decimal.Decimal rather than
	// decimal.NullDecimal.
	ConvertToNullDecimal(v interface{}) (decimal.NullDecimal, error)
	//ConvertNoBoundsCheck normalizes an interface{} to a decimal type without performing expensive bound checks
	ConvertNoBoundsCheck(v interface{}) (decimal.Decimal, error)
	// BoundsCheck rounds and validates a decimal
	BoundsCheck(v decimal.Decimal) (decimal.Decimal, error)
	// ExclusiveUpperBound returns the exclusive upper bound for this Decimal.
	// For example, DECIMAL(5,2) would return 1000, as 999.99 is the max represented.
	ExclusiveUpperBound() decimal.Decimal
	// MaximumScale returns the maximum scale allowed for the current precision.
	MaximumScale() uint8
	// Precision returns the base-10 precision of the type, which is the total number of digits. For example, a
	// precision of 3 means that 999, 99.9, 9.99, and .999 are all valid maximums (depending on the scale).
	Precision() uint8
	// Scale returns the scale, or number of digits after the decimal, that may be held.
	// This will always be less than or equal to the precision.
	Scale() uint8
}

// JsonType represents the JSON type.
// https://dev.mysql.com/doc/refman/8.0/en/json.html
// The type of the returned value is JSONValue.
type JsonType interface {
	Type
}

type Type2 interface {
	Type

	// Compare2 returns an integer comparing two Values.
	Compare2(Value, Value) (int, error)
	// Convert2 converts a value of a compatible type.
	Convert2(Value) (Value, error)
	// Zero2 returns the zero Value for this type.
	Zero2() Value
	// SQL2 returns the sqltypes.Value for the given value
	SQL2(Value) (sqltypes.Value, error)
}

// SpatialColumnType is a node that contains a reference to all spatial types.
type SpatialColumnType interface {
	// GetSpatialTypeSRID returns the SRID value for spatial types.
	GetSpatialTypeSRID() (uint32, bool)
	// SetSRID sets SRID value for spatial types.
	SetSRID(uint32) Type
	// MatchSRID returns nil if column type SRID matches given value SRID otherwise returns error.
	MatchSRID(interface{}) error
}

// SystemVariableType represents a SQL type specifically (and only) used in system variables. Assigning any non-system
// variables a SystemVariableType will cause errors.
type SystemVariableType interface {
	Type
	// EncodeValue returns the given value as a string for storage.
	EncodeValue(interface{}) (string, error)
	// DecodeValue returns the original value given to EncodeValue from the given string. This is different from `Convert`,
	// as the encoded value may technically be an "illegal" value according to the type rules.
	DecodeValue(string) (interface{}, error)
}

// ApproximateTypeFromValue returns the closest matching type to the given value. For example, an int16 will return SMALLINT.
func ApproximateTypeFromValue(val interface{}) Type {
	switch v := val.(type) {
	case bool:
		return types.Boolean
	case int:
		if strconv.IntSize == 32 {
			return types.Int32
		}
		return types.Int64
	case int64:
		return types.Int64
	case int32:
		return types.Int32
	case int16:
		return types.Int16
	case int8:
		return types.Int8
	case uint:
		if strconv.IntSize == 32 {
			return types.Uint32
		}
		return types.Uint64
	case uint64:
		return types.Uint64
	case uint32:
		return types.Uint32
	case uint16:
		return types.Uint16
	case uint8:
		return types.Uint8
	case types.Timespan, time.Duration:
		return types.Time
	case time.Time:
		return types.Datetime
	case float32:
		return types.Float32
	case float64:
		return types.Float64
	case string:
		typ, err := types.CreateString(sqltypes.VarChar, int64(len(v)), Collation_Default)
		if err != nil {
			typ, err = types.CreateString(sqltypes.Text, int64(len(v)), Collation_Default)
			if err != nil {
				typ = types.LongText
			}
		}
		return typ
	case []byte:
		typ, err := types.CreateBinary(sqltypes.VarBinary, int64(len(v)))
		if err != nil {
			typ, err = types.CreateBinary(sqltypes.Blob, int64(len(v)))
			if err != nil {
				typ = types.LongBlob
			}
		}
		return typ
	case decimal.Decimal:
		str := v.String()
		dotIdx := strings.Index(str, ".")
		if len(str) > 66 {
			return types.Float64
		} else if dotIdx == -1 {
			typ, err := types.CreateDecimalType(uint8(len(str)), 0)
			if err != nil {
				return types.Float64
			}
			return typ
		} else {
			precision := uint8(len(str) - 1)
			scale := uint8(len(str) - dotIdx - 1)
			typ, err := types.CreateDecimalType(precision, scale)
			if err != nil {
				return types.Float64
			}
			return typ
		}
	case decimal.NullDecimal:
		if !v.Valid {
			return types.Float64
		}
		return ApproximateTypeFromValue(v.Decimal)
	case nil:
		return types.Null
	default:
		return types.LongText
	}
}

// ColumnTypeToType gets the column type using the column definition.
func ColumnTypeToType(ct *sqlparser.ColumnType) (Type, error) {
	switch strings.ToLower(ct.Type) {
	case "boolean", "bool":
		return types.Int8, nil
	case "tinyint":
		if ct.Unsigned {
			return types.Uint8, nil
		}
		return types.Int8, nil
	case "smallint":
		if ct.Unsigned {
			return types.Uint16, nil
		}
		return types.Int16, nil
	case "mediumint":
		if ct.Unsigned {
			return types.Uint24, nil
		}
		return types.Int24, nil
	case "int", "integer":
		if ct.Unsigned {
			return types.Uint32, nil
		}
		return types.Int32, nil
	case "bigint":
		if ct.Unsigned {
			return types.Uint64, nil
		}
		return types.Int64, nil
	case "float":
		if ct.Length != nil {
			precision, err := strconv.ParseInt(string(ct.Length.Val), 10, 8)
			if err != nil {
				return nil, err
			}
			if precision > 53 || precision < 0 {
				return nil, ErrInvalidColTypeDefinition.New(ct.String(), "Valid range for precision is 0-24 or 25-53")
			} else if precision > 24 {
				return types.Float64, nil
			} else {
				return types.Float32, nil
			}
		}
		return types.Float32, nil
	case "double", "real", "double precision":
		return types.Float64, nil
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
		return types.CreateColumnDecimalType(uint8(precision), uint8(scale))
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
		return types.TinyBlob, nil
	case "blob":
		if ct.Length == nil {
			return types.Blob, nil
		}
		length, err := strconv.ParseInt(string(ct.Length.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		return types.CreateBinary(sqltypes.Blob, length)
	case "mediumblob":
		return types.MediumBlob, nil
	case "longblob":
		return types.LongBlob, nil
	case "tinytext":
		collation, err := ParseCollation(&ct.Charset, &ct.Collate, ct.BinaryCollate)
		if err != nil {
			return nil, err
		}
		return types.CreateString(sqltypes.Text, types.TinyTextBlobMax/collation.CharacterSet().MaxLength(), collation)
	case "text":
		collation, err := ParseCollation(&ct.Charset, &ct.Collate, ct.BinaryCollate)
		if err != nil {
			return nil, err
		}
		if ct.Length == nil {
			return types.CreateString(sqltypes.Text, types.TextBlobMax/collation.CharacterSet().MaxLength(), collation)
		}
		length, err := strconv.ParseInt(string(ct.Length.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		return types.CreateString(sqltypes.Text, length, collation)
	case "mediumtext", "long", "long varchar":
		collation, err := ParseCollation(&ct.Charset, &ct.Collate, ct.BinaryCollate)
		if err != nil {
			return nil, err
		}
		return types.CreateString(sqltypes.Text, types.MediumTextBlobMax/collation.CharacterSet().MaxLength(), collation)
	case "longtext":
		collation, err := ParseCollation(&ct.Charset, &ct.Collate, ct.BinaryCollate)
		if err != nil {
			return nil, err
		}
		return types.CreateString(sqltypes.Text, types.LongTextBlobMax/collation.CharacterSet().MaxLength(), collation)
	case "char", "character":
		collation, err := ParseCollation(&ct.Charset, &ct.Collate, ct.BinaryCollate)
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
		return types.CreateString(sqltypes.Char, length, collation)
	case "nchar", "national char", "national character":
		length := int64(1)
		if ct.Length != nil {
			var err error
			length, err = strconv.ParseInt(string(ct.Length.Val), 10, 64)
			if err != nil {
				return nil, err
			}
		}
		return types.CreateString(sqltypes.Char, length, Collation_utf8mb3_general_ci)
	case "varchar", "character varying":
		collation, err := ParseCollation(&ct.Charset, &ct.Collate, ct.BinaryCollate)
		if err != nil {
			return nil, err
		}
		if ct.Length == nil {
			return nil, fmt.Errorf("VARCHAR requires a length")
		}

		var strLen = string(ct.Length.Val)
		var length int64
		if strings.ToLower(strLen) == "max" {
			length = 16383
		} else {
			length, err = strconv.ParseInt(strLen, 10, 64)
			if err != nil {
				return nil, err
			}
		}
		return types.CreateString(sqltypes.VarChar, length, collation)
	case "nvarchar", "national varchar", "national character varying":
		if ct.Length == nil {
			return nil, fmt.Errorf("VARCHAR requires a length")
		}
		length, err := strconv.ParseInt(string(ct.Length.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		return types.CreateString(sqltypes.VarChar, length, Collation_utf8mb3_general_ci)
	case "binary":
		length := int64(1)
		if ct.Length != nil {
			var err error
			length, err = strconv.ParseInt(string(ct.Length.Val), 10, 64)
			if err != nil {
				return nil, err
			}
		}
		return types.CreateString(sqltypes.Binary, length, Collation_binary)
	case "varbinary":
		if ct.Length == nil {
			return nil, fmt.Errorf("VARBINARY requires a length")
		}
		length, err := strconv.ParseInt(string(ct.Length.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		return types.CreateString(sqltypes.VarBinary, length, Collation_binary)
	case "year":
		return types.Year, nil
	case "date":
		return types.Date, nil
	case "time":
		if ct.Length != nil {
			length, err := strconv.ParseInt(string(ct.Length.Val), 10, 64)
			if err != nil {
				return nil, err
			}
			switch length {
			case 0, 1, 2, 3, 4, 5:
				return nil, fmt.Errorf("TIME length not yet supported")
			case 6:
				return types.Time, nil
			default:
				return nil, fmt.Errorf("TIME only supports a length from 0 to 6")
			}
		}
		return types.Time, nil
	case "timestamp":
		return types.Timestamp, nil
	case "datetime":
		return types.Datetime, nil
	case "enum":
		collation, err := ParseCollation(&ct.Charset, &ct.Collate, ct.BinaryCollate)
		if err != nil {
			return nil, err
		}
		return types.CreateEnumType(ct.EnumValues, collation)
	case "set":
		collation, err := ParseCollation(&ct.Charset, &ct.Collate, ct.BinaryCollate)
		if err != nil {
			return nil, err
		}
		return types.CreateSetType(ct.EnumValues, collation)
	case "json":
		return types.JSON, nil
	case "geometry":
		return types.GeometryType{}, nil
	case "geometrycollection":
		return types.GeomCollType{}, nil
	case "linestring":
		return types.LineStringType{}, nil
	case "multilinestring":
		return types.MultiLineStringType{}, nil
	case "point":
		return types.PointType{}, nil
	case "multipoint":
		return types.MultiPointType{}, nil
	case "polygon":
		return types.PolygonType{}, nil
	case "multipolygon":
		return types.MultiPolygonType{}, nil
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
		if b == 0 {
			return false, nil
		}
		return true, nil
	case uint32:
		return ConvertToBool(uint64(b))
	case uint16:
		return ConvertToBool(uint64(b))
	case uint8:
		return ConvertToBool(uint64(b))
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
		bFloat, err := strconv.ParseFloat(b, 64)
		if err != nil {
			// In MySQL, if the string does not represent a float then it's false
			return false, nil
		}
		return bFloat != 0, nil
	case nil:
		return false, fmt.Errorf("unable to cast nil to bool")
	default:
		return false, fmt.Errorf("unable to cast %#v of type %T to bool", v, v)
	}
}

// NumColumns returns the number of columns in a type. This is one for all
// types, except tuples.
func NumColumns(t Type) int {
	v, ok := t.(types.TupleType)
	if !ok {
		return 1
	}
	return len(v)
}

// ErrIfMismatchedColumns returns an operand error if the number of columns in
// t1 is not equal to the number of columns in t2. If the number of columns is
// equal, and both types are tuple types, it recurses into each subtype,
// asserting that those subtypes are structurally identical as well.
func ErrIfMismatchedColumns(t1, t2 Type) error {
	if NumColumns(t1) != NumColumns(t2) {
		return ErrInvalidOperandColumns.New(NumColumns(t1), NumColumns(t2))
	}
	v1, ok1 := t1.(types.TupleType)
	v2, ok2 := t2.(types.TupleType)
	if ok1 && ok2 {
		for i := range v1 {
			if err := ErrIfMismatchedColumns(v1[i], v2[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

// ErrIfMismatchedColumnsInTuple returns an operand error is t2 is not a tuple
// type whose subtypes are structurally identical to t1.
func ErrIfMismatchedColumnsInTuple(t1, t2 Type) error {
	v2, ok2 := t2.(types.TupleType)
	if !ok2 {
		return ErrInvalidOperandColumns.New(NumColumns(t1), NumColumns(t2))
	}
	for _, v := range v2 {
		if err := ErrIfMismatchedColumns(t1, v); err != nil {
			return err
		}
	}
	return nil
}

// CompareNulls compares two values, and returns true if either is null.
// The returned integer represents the ordering, with a rule that states nulls
// as being ordered before non-nulls.
func CompareNulls(a interface{}, b interface{}) (bool, int) {
	aIsNull := a == nil
	bIsNull := b == nil
	if aIsNull && bIsNull {
		return true, 0
	} else if aIsNull && !bIsNull {
		return true, 1
	} else if !aIsNull && bIsNull {
		return true, -1
	}
	return false, 0
}
