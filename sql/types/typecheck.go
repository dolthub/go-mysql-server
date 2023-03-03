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
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
)

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
	if t == nil {
		return false
	}
	switch t.Type() {
	case sqltypes.Binary, sqltypes.VarBinary, sqltypes.Blob:
		return true
	default:
		return false
	}
}

// IsDecimal checks if t is a DECIMAL type.
func IsDecimal(t sql.Type) bool {
	_, ok := t.(DecimalType_)
	return ok
}

// IsBit checks if t is a BIT type.
func IsBit(t sql.Type) bool {
	_, ok := t.(BitType_)
	return ok
}

// IsFloat checks if t is float type.
func IsFloat(t sql.Type) bool {
	return t == Float32 || t == Float64
}

// IsInteger checks if t is an integer type.
func IsInteger(t sql.Type) bool {
	return IsSigned(t) || IsUnsigned(t)
}

// IsJSON returns true if the specified type is a JSON type.
func IsJSON(t sql.Type) bool {
	_, ok := t.(JsonType)
	return ok
}

// IsGeometry returns true if the specified type is a Geometry type.
func IsGeometry(t sql.Type) bool {
	switch t.(type) {
	case GeometryType, PointType, LineStringType, PolygonType:
		return true
	default:
		return false
	}
}

// IsNull returns true if expression is nil or is Null Type, otherwise false.
func IsNull(ex sql.Expression) bool {
	return ex == nil || ex.Type() == Null
}

// IsNumber checks if t is a number type
func IsNumber(t sql.Type) bool {
	switch t.(type) {
	case NumberTypeImpl_, DecimalType_, BitType_, YearType_, SystemBoolType_:
		return true
	default:
		return false
	}
}

// IsSigned checks if t is a signed type.
func IsSigned(t sql.Type) bool {
	// systemBoolType is Int8
	if _, ok := t.(SystemBoolType_); ok {
		return true
	}
	return t == Int8 || t == Int16 || t == Int24 || t == Int32 || t == Int64
}

// IsText checks if t is a CHAR, VARCHAR, TEXT, BINARY, VARBINARY, or BLOB (including TEXT and BLOB variants).
func IsText(t sql.Type) bool {
	_, ok := t.(StringType)
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
	_, ok := t.(TimespanType_)
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
	_, ok := t.(EnumType)
	return ok
}

// IsSet checks if t is a set
func IsSet(t sql.Type) bool {
	_, ok := t.(SetType)
	return ok
}

// IsTuple checks if t is a tuple type.
// Note that TupleType instances with just 1 value are not considered
// as a tuple, but a parenthesized value.
func IsTuple(t sql.Type) bool {
	v, ok := t.(TupleType)
	return ok && len(v) > 1
}

// IsUnsigned checks if t is an unsigned type.
func IsUnsigned(t sql.Type) bool {
	return t == Uint8 || t == Uint16 || t == Uint24 || t == Uint32 || t == Uint64
}
