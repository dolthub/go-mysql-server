// Copyright 2022 Dolthub, Inc.
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

package types

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/shopspring/decimal"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

// ApproximateTypeFromValue returns the closest matching type to the given value. For example, an int16 will return SMALLINT.
func ApproximateTypeFromValue(val interface{}) sql.Type {
	switch v := val.(type) {
	case bool:
		return Boolean
	case int:
		if strconv.IntSize == 32 {
			return Int32
		}
		return Int64
	case int64:
		return Int64
	case int32:
		return Int32
	case int16:
		return Int16
	case int8:
		return Int8
	case uint:
		if strconv.IntSize == 32 {
			return Uint32
		}
		return Uint64
	case uint64:
		return Uint64
	case uint32:
		return Uint32
	case uint16:
		return Uint16
	case uint8:
		return Uint8
	case Timespan, time.Duration:
		return Time
	case time.Time:
		return DatetimeMaxPrecision
	case float32:
		return Float32
	case float64:
		return Float64
	case string:
		typ, err := CreateString(sqltypes.VarChar, int64(len(v)), sql.Collation_Default)
		if err != nil {
			typ, err = CreateString(sqltypes.Text, int64(len(v)), sql.Collation_Default)
			if err != nil {
				typ = LongText
			}
		}
		return typ
	case []byte:
		typ, err := CreateBinary(sqltypes.VarBinary, int64(len(v)))
		if err != nil {
			typ, err = CreateBinary(sqltypes.Blob, int64(len(v)))
			if err != nil {
				typ = LongBlob
			}
		}
		return typ
	case decimal.Decimal:
		str := v.String()
		dotIdx := strings.Index(str, ".")
		if len(str) > 66 {
			return Float64
		} else if dotIdx == -1 {
			typ, err := CreateDecimalType(uint8(len(str)), 0)
			if err != nil {
				return Float64
			}
			return typ
		} else {
			precision := uint8(len(str) - 1)
			scale := uint8(len(str) - dotIdx - 1)
			typ, err := CreateDecimalType(precision, scale)
			if err != nil {
				return Float64
			}
			return typ
		}
	case decimal.NullDecimal:
		if !v.Valid {
			return Float64
		}
		return ApproximateTypeFromValue(v.Decimal)
	case nil:
		return Null
	default:
		return LongText
	}
}

// IsBinary returns whether the type represents binary data.
func IsBinary(sqlType query.Type) bool {
	switch sqlType {
	case sqltypes.Binary,
		sqltypes.VarBinary,
		sqltypes.Blob,
		sqltypes.TypeJSON,
		sqltypes.Geometry:
		return true
	}
	return false
}

func allowsCharSet(sqlType query.Type) bool {
	switch sqlType {
	case sqltypes.VarChar,
		sqltypes.Char,
		sqltypes.Text,
		sqltypes.Enum,
		sqltypes.Set:
		return true
	}
	return false
}

var ErrCharacterSetOnInvalidType = errors.NewKind("Only character columns, enums, and sets can have a CHARACTER SET option")

// ColumnTypeToType gets the column type using the column definition.
func ColumnTypeToType(ct *sqlparser.ColumnType) (sql.Type, error) {
	if resolvedType, ok := ct.ResolvedType.(sql.Type); ok {
		return resolvedType, nil
	}
	sqlType := ct.SQLType()

	if !allowsCharSet(sqlType) && len(ct.Charset) != 0 {
		return nil, ErrCharacterSetOnInvalidType.New()
	}

	collate := ct.Collate
	if IsBinary(sqlType) && collate == "" {
		collate = sql.Collation_binary.Name()
	}

	switch strings.ToLower(ct.Type) {
	case "boolean", "bool":
		return Boolean, nil
	case "tinyint":
		if ct.Length != nil {
			displayWidth, err := strconv.Atoi(string(ct.Length.Val))
			if err != nil {
				return nil, fmt.Errorf("unable to parse display width value: %w", err)
			}

			// As of MySQL 8.1.0, TINYINT is the only integer type for which MySQL will retain a display width,
			// and ONLY if it's 1. All other types and display width values are dropped. TINYINT(1) seems to be
			// left for backwards compatibility with ORM tools like ActiveRecord that rely on it for mapping to
			// a boolean type.
			if !ct.Unsigned && displayWidth == 1 {
				return Boolean, nil
			}
		}

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
		if ct.Length != nil {
			precision, err := strconv.ParseInt(string(ct.Length.Val), 10, 8)
			if err != nil {
				return nil, err
			}
			if precision > 53 || precision < 0 {
				return nil, sql.ErrInvalidColTypeDefinition.New(ct.String(), "Valid range for precision is 0-24 or 25-53")
			} else if precision > 24 {
				return Float64, nil
			} else {
				return Float32, nil
			}
		}
		return Float32, nil
	case "double", "real", "double precision":
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
		return CreateColumnDecimalType(uint8(precision), uint8(scale))
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
	case "tinytext", "tinyblob":
		collation, err := sql.ParseCollation(ct.Charset, collate, ct.BinaryCollate)
		if err != nil {
			return nil, err
		}
		return CreateString(sqltypes.Text, TinyTextBlobMax/collation.CharacterSet().MaxLength(), collation)
	case "text", "blob":
		collation, err := sql.ParseCollation(ct.Charset, collate, ct.BinaryCollate)
		if err != nil {
			return nil, err
		}
		if ct.Length == nil {
			return CreateString(sqltypes.Text, TextBlobMax/collation.CharacterSet().MaxLength(), collation)
		}
		length, err := strconv.ParseInt(string(ct.Length.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		return CreateString(sqltypes.Text, length, collation)
	case "mediumtext", "mediumblob", "long", "long varchar":
		collation, err := sql.ParseCollation(ct.Charset, collate, ct.BinaryCollate)
		if err != nil {
			return nil, err
		}
		return CreateString(sqltypes.Text, MediumTextBlobMax/collation.CharacterSet().MaxLength(), collation)
	case "longtext", "longblob":
		collation, err := sql.ParseCollation(ct.Charset, collate, ct.BinaryCollate)
		if err != nil {
			return nil, err
		}
		return CreateString(sqltypes.Text, LongTextBlobMax/collation.CharacterSet().MaxLength(), collation)
	case "char", "character", "binary":
		collation, err := sql.ParseCollation(ct.Charset, collate, ct.BinaryCollate)
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
	case "nchar", "national char", "national character":
		length := int64(1)
		if ct.Length != nil {
			var err error
			length, err = strconv.ParseInt(string(ct.Length.Val), 10, 64)
			if err != nil {
				return nil, err
			}
		}
		return CreateString(sqltypes.Char, length, sql.Collation_utf8mb3_general_ci)
	case "varchar", "char varying", "character varying":
		collation, err := sql.ParseCollation(ct.Charset, collate, ct.BinaryCollate)
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
		return CreateString(sqltypes.VarChar, length, collation)
	case "nchar varchar", "nchar varying", "nvarchar", "national varchar", "national char varying", "national character varying":
		if ct.Length == nil {
			return nil, fmt.Errorf("VARCHAR requires a length")
		}
		length, err := strconv.ParseInt(string(ct.Length.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		return CreateString(sqltypes.VarChar, length, sql.Collation_utf8mb3_general_ci)
	case "varbinary":
		collation, err := sql.ParseCollation(ct.Charset, collate, ct.BinaryCollate)
		if err != nil {
			return nil, err
		}
		if ct.Length == nil {
			return nil, fmt.Errorf("VARBINARY requires a length")
		}
		length, err := strconv.ParseInt(string(ct.Length.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		// we need to have a separate check for varbinary, as CreateString checks varbinary against json limit
		if length > varcharVarbinaryMax {
			return nil, ErrLengthTooLarge.New(length, varcharVarbinaryMax)
		}
		return CreateString(sqltypes.VarBinary, length, collation)
	case "year":
		return Year, nil
	case "date":
		return CreateDatetimeType(sqltypes.Date, 0)
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
				return Time, nil
			default:
				return nil, fmt.Errorf("TIME only supports a length from 0 to 6")
			}
		}
		return Time, nil
	case "timestamp":
		precision := int64(0)
		if ct.Length != nil {
			var err error
			precision, err = strconv.ParseInt(string(ct.Length.Val), 10, 64)
			if err != nil {
				return nil, err
			}

			if precision > 6 || precision < 0 {
				return nil, fmt.Errorf("TIMESTAMP supports precision from 0 to 6")
			}
		}

		return CreateDatetimeType(sqltypes.Timestamp, int(precision))
	case "datetime":
		precision := int64(0)
		if ct.Length != nil {
			var err error
			precision, err = strconv.ParseInt(string(ct.Length.Val), 10, 64)
			if err != nil {
				return nil, err
			}

			if precision > 6 || precision < 0 {
				return nil, fmt.Errorf("DATETIME supports precision from 0 to 6")
			}
		}

		return CreateDatetimeType(sqltypes.Datetime, int(precision))
	case "enum":
		collation, err := sql.ParseCollation(ct.Charset, collate, ct.BinaryCollate)
		if err != nil {
			return nil, err
		}
		if collation.Sorter() == nil {
			return nil, sql.ErrCollationNotYetImplementedTemp.New(collation.Name())
		}
		return CreateEnumType(ct.EnumValues, collation)
	case "set":
		collation, err := sql.ParseCollation(ct.Charset, collate, ct.BinaryCollate)
		if err != nil {
			return nil, err
		}
		if collation.Sorter() == nil {
			return nil, sql.ErrCollationNotYetImplementedTemp.New(collation.Name())
		}
		return CreateSetType(ct.EnumValues, collation)
	case "json":
		return JSON, nil
	case "geometry":
		return GeometryType{}, nil
	case "geometrycollection":
		return GeomCollType{}, nil
	case "linestring":
		return LineStringType{}, nil
	case "multilinestring":
		return MultiLineStringType{}, nil
	case "point":
		return PointType{}, nil
	case "multipoint":
		return MultiPointType{}, nil
	case "polygon":
		return PolygonType{}, nil
	case "multipolygon":
		return MultiPolygonType{}, nil
	default:
		return nil, fmt.Errorf("unknown type: %v", ct.Type)
	}
	return nil, fmt.Errorf("type not yet implemented: %v", ct.Type)
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

// NumColumns returns the number of columns in a type. This is one for all
// types, except tuples.
func NumColumns(t sql.Type) int {
	v, ok := t.(TupleType)
	if !ok {
		return 1
	}
	return len(v)
}

// ErrIfMismatchedColumns returns an operand error if the number of columns in
// t1 is not equal to the number of columns in t2. If the number of columns is
// equal, and both types are tuple types, it recurses into each subtype,
// asserting that those subtypes are structurally identical as well.
func ErrIfMismatchedColumns(t1, t2 sql.Type) error {
	if NumColumns(t1) != NumColumns(t2) {
		return sql.ErrInvalidOperandColumns.New(NumColumns(t1), NumColumns(t2))
	}
	v1, ok1 := t1.(TupleType)
	v2, ok2 := t2.(TupleType)
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
func ErrIfMismatchedColumnsInTuple(t1, t2 sql.Type) error {
	v2, ok2 := t2.(TupleType)
	if !ok2 {
		return sql.ErrInvalidOperandColumns.New(NumColumns(t1), NumColumns(t2))
	}
	for _, v := range v2 {
		if err := ErrIfMismatchedColumns(t1, v); err != nil {
			return err
		}
	}
	return nil
}

// TypesEqual compares two Types and returns whether they are equivalent.
func TypesEqual(a, b sql.Type) bool {
	// TODO: replace all of the Type() == Type() calls with TypesEqual

	// We can assume they have the same implementing type if this passes, so we have to check the parameters
	if a == nil || b == nil || a.Type() != b.Type() {
		return false
	}
	// Some types cannot be compared structurally as they contain non-comparable types (such as slices), so we handle
	// those separately.
	switch at := a.(type) {
	case EnumType:
		aEnumType := at
		bEnumType := b.(EnumType)
		if len(aEnumType.indexToVal) != len(bEnumType.indexToVal) {
			return false
		}
		for i := 0; i < len(aEnumType.indexToVal); i++ {
			if aEnumType.indexToVal[i] != bEnumType.indexToVal[i] {
				return false
			}
		}
		return aEnumType.collation == bEnumType.collation
	case SetType:
		aSetType := at
		bSetType := b.(SetType)
		if len(aSetType.bitToVal) != len(bSetType.bitToVal) {
			return false
		}
		for bit, aVal := range aSetType.bitToVal {
			if bVal, ok := bSetType.bitToVal[bit]; ok && aVal != bVal {
				return false
			}
		}
		return aSetType.collation == bSetType.collation
	case TupleType:
		if tupA, ok := a.(TupleType); ok {
			if tupB, ok := b.(TupleType); ok && len(tupA) == len(tupB) {
				for i := range tupA {
					if !TypesEqual(tupA[i], tupB[i]) {
						return false
					}
				}
				return true
			}
		}
		return false
	default:
		return a == b
	}
}
