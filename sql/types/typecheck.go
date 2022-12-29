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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"
)

// AreComparable returns whether the given types are either the same or similar enough that values can meaningfully be
// compared across all permutations. Int8 and Int64 are comparable types, where as VarChar and Int64 are not. In the case
// of the latter example, not all possible values of a VarChar are comparable to an Int64, while this is true for the
// former example.
func AreComparable(types ...sql.Type) bool {
	if len(types) <= 1 {
		return true
	}
	typeNums := make([]int, len(types))
	for i, typ := range types {
		switch typ.Type() {
		case sqltypes.Int8, sqltypes.Uint8, sqltypes.Int16,
			sqltypes.Uint16, sqltypes.Int24, sqltypes.Uint24,
			sqltypes.Int32, sqltypes.Uint32, sqltypes.Int64,
			sqltypes.Uint64, sqltypes.Float32, sqltypes.Float64,
			sqltypes.Decimal, sqltypes.Bit, sqltypes.Year:
			typeNums[i] = 1
		case sqltypes.Timestamp, sqltypes.Date, sqltypes.Datetime:
			typeNums[i] = 2
		case sqltypes.Time:
			typeNums[i] = 3
		case sqltypes.Text, sqltypes.Blob, sqltypes.VarChar,
			sqltypes.VarBinary, sqltypes.Char, sqltypes.Binary:
			typeNums[i] = 4
		case sqltypes.Enum:
			typeNums[i] = 5
		case sqltypes.Set:
			typeNums[i] = 6
		case sqltypes.Geometry:
			typeNums[i] = 7
		case sqltypes.TypeJSON:
			typeNums[i] = 8
		default:
			return false
		}
	}
	for i := 1; i < len(typeNums); i++ {
		if typeNums[i-1] != typeNums[i] {
			return false
		}
	}
	return true
}

// IsBlobType checks if t is BLOB
func IsBlobType(t sql.Type) bool {
	switch t.Type() {
	case sqltypes.Blob:
		return true
	default:
		return false
	}
}

// IsBinaryType checks if t is BINARY, VARBINARY, or BLOB
func IsBinaryType(t sql.Type) bool {
	switch t.Type() {
	case sqltypes.Binary, sqltypes.VarBinary, sqltypes.Blob:
		return true
	default:
		return false
	}
}

// IsDecimal checks if t is a DECIMAL type.
func IsDecimal(t sql.Type) bool {
	_, ok := t.(sql.DecimalType_)
	return ok
}

// IsBit checks if t is a BIT type.
func IsBit(t sql.Type) bool {
	_, ok := t.(sql.BitType_)
	return ok
}

// IsFloat checks if t is float type.
func IsFloat(t sql.Type) bool {
	return t == sql.Float32 || t == sql.Float64
}

// IsInteger checks if t is an integer type.
func IsInteger(t sql.Type) bool {
	return IsSigned(t) || IsUnsigned(t)
}

// IsJSON returns true if the specified type is a JSON type.
func IsJSON(t sql.Type) bool {
	_, ok := t.(sql.JsonType_)
	return ok
}

// IsGeometry returns true if the specified type is a Geometry type.
func IsGeometry(t sql.Type) bool {
	switch t.(type) {
	case sql.GeometryType, sql.PointType, sql.LineStringType, sql.PolygonType:
		return true
	default:
		return false
	}
}

// IsNull returns true if expression is nil or is Null Type, otherwise false.
func IsNull(ex sql.Expression) bool {
	return ex == nil || ex.Type() == sql.Null
}

// IsNumber checks if t is a number type
func IsNumber(t sql.Type) bool {
	switch t.(type) {
	case sql.NumberTypeImpl_, sql.DecimalType_, sql.BitType_, sql.YearType_, sql.SystemBoolType_:
		return true
	default:
		return false
	}
}

// IsSigned checks if t is a signed type.
func IsSigned(t sql.Type) bool {
	// systemBoolType is Int8
	if _, ok := t.(sql.SystemBoolType_); ok {
		return true
	}
	return t == sql.Int8 || t == sql.Int16 || t == sql.Int24 || t == sql.Int32 || t == sql.Int64
}

// IsText checks if t is a CHAR, VARCHAR, TEXT, BINARY, VARBINARY, or BLOB (including TEXT and BLOB variants).
func IsText(t sql.Type) bool {
	_, ok := t.(sql.StringType_)
	return ok
}

// IsTextBlob checks if t is one of the TEXTs or BLOBs.
func IsTextBlob(t sql.Type) bool {
	switch t.Type() {
	case sqltypes.Text, sqltypes.Blob:
		return true
	default:
		return false
	}
}

// IsTextOnly checks if t is CHAR, VARCHAR, or one of the TEXTs.
func IsTextOnly(t sql.Type) bool {
	if t == nil {
		return false
	}
	switch t.Type() {
	case sqltypes.Char, sqltypes.VarChar, sqltypes.Text:
		return true
	default:
		return false
	}
}

// IsTimespan checks if t is a time (timespan)
func IsTimespan(t sql.Type) bool {
	_, ok := t.(sql.TimespanType_)
	return ok
}

// IsTime checks if t is a timestamp, date or datetime
func IsTime(t sql.Type) bool {
	_, ok := t.(datetimeType)
	return ok
}

// IsDateType checks if t is a date
func IsDateType(t sql.Type) bool {
	dt, ok := t.(datetimeType)
	return ok && dt.baseType == sqltypes.Date
}

// IsDatetimeType checks if t is a datetime
func IsDatetimeType(t sql.Type) bool {
	dt, ok := t.(datetimeType)
	return ok && dt.baseType == sqltypes.Datetime
}

// IsTimestampType checks if t is a timestamp
func IsTimestampType(t sql.Type) bool {
	dt, ok := t.(datetimeType)
	return ok && dt.baseType == sqltypes.Timestamp
}

// IsEnum checks if t is a enum
func IsEnum(t sql.Type) bool {
	_, ok := t.(sql.EnumType_)
	return ok
}

// IsEnum checks if t is a set
func IsSet(t sql.Type) bool {
	_, ok := t.(sql.SetType_)
	return ok
}

// IsTuple checks if t is a tuple type.
// Note that TupleType instances with just 1 value are not considered
// as a tuple, but a parenthesized value.
func IsTuple(t sql.Type) bool {
	v, ok := t.(sql.TupleType)
	return ok && len(v) > 1
}

// IsUnsigned checks if t is an unsigned type.
func IsUnsigned(t sql.Type) bool {
	return t == sql.Uint8 || t == sql.Uint16 || t == sql.Uint24 || t == sql.Uint32 || t == sql.Uint64
}

