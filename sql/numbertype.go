package sql

import (
	"fmt"
	"strconv"
	"time"

	"github.com/spf13/cast"
	"vitess.io/vitess/go/sqltypes"
)

var (
	// Int8 is an integer of 8 bits
	Int8 = CreateNumberType(BaseType_TINYINT, false)
	// Uint8 is an unsigned integer of 8 bits
	Uint8 = CreateNumberType(BaseType_TINYINT, true)
	// Int16 is an integer of 16 bits
	Int16 = CreateNumberType(BaseType_SMALLINT, false)
	// Uint16 is an unsigned integer of 16 bits
	Uint16 = CreateNumberType(BaseType_SMALLINT, true)
	// Int24 is an integer of 24 bits.
	Int24 = CreateNumberType(BaseType_MEDIUMINT, false)
	// Uint24 is an unsigned integer of 24 bits.
	Uint24 = CreateNumberType(BaseType_MEDIUMINT, true)
	// Int32 is an integer of 32 bits.
	Int32 = CreateNumberType(BaseType_INT, false)
	// Uint32 is an unsigned integer of 32 bits.
	Uint32 = CreateNumberType(BaseType_INT, true)
	// Int64 is an integer of 64 bytes.
	Int64 = CreateNumberType(BaseType_BIGINT, false)
	// Uint64 is an unsigned integer of 64 bits.
	Uint64 = CreateNumberType(BaseType_BIGINT, true)
	// Float32 is a floating point number of 32 bits.
	Float32 = CreateNumberType(BaseType_FLOAT, false)
	// Float64 is a floating point number of 64 bits.
	Float64 = CreateNumberType(BaseType_DOUBLE, false)
)

type NumberType interface {
	Type
	IsUnsigned() bool
	IsSigned() bool
}

type numberTypeImpl struct {
	baseType BaseType
	unsigned bool
}

// CreateNumberType
func CreateNumberType(baseType BaseType, unsigned bool) NumberType {
	switch baseType {
	case BaseType_TINYINT, BaseType_SMALLINT, BaseType_MEDIUMINT, BaseType_INT, BaseType_BIGINT:
		return numberTypeImpl{
			baseType: baseType,
			unsigned: unsigned,
		}
	case BaseType_FLOAT, BaseType_DOUBLE:
		return numberTypeImpl{
			baseType: baseType,
			unsigned: false,
		}
	}
	panic(fmt.Sprintf("%v is not a valid number base type", baseType.String()))
}

// BaseType implements Type interface.
func (t numberTypeImpl) BaseType() BaseType {
	return t.baseType
}

// Convert implements Type interface.
func (t numberTypeImpl) Convert(v interface{}) (interface{}, error) {
	if ti, ok := v.(time.Time); ok {
		v = float64(ti.Unix()) + (float64(ti.Nanosecond()) / float64(time.Second/time.Nanosecond))
	}

	if t.unsigned {
		switch t.baseType {
		case BaseType_TINYINT:
			return cast.ToUint8E(v)
		case BaseType_SMALLINT:
			return cast.ToUint16E(v)
		case BaseType_MEDIUMINT:
			return cast.ToUint32E(v)
		case BaseType_INT:
			return cast.ToUint32E(v)
		case BaseType_BIGINT:
			return cast.ToUint64E(v)
		}
	} else {
		switch t.baseType {
		case BaseType_TINYINT:
			return cast.ToInt8E(v)
		case BaseType_SMALLINT:
			return cast.ToInt16E(v)
		case BaseType_MEDIUMINT:
			return cast.ToInt32E(v)
		case BaseType_INT:
			return cast.ToInt32E(v)
		case BaseType_BIGINT:
			return cast.ToInt64E(v)
		case BaseType_FLOAT:
			return cast.ToFloat32E(v)
		case BaseType_DOUBLE:
			return cast.ToFloat64E(v)
		}
	}
	return nil, ErrInvalidType.New(t.baseType.String())
}

// Compare implements Type interface.
func (t numberTypeImpl) Compare(a interface{}, b interface{}) (int, error) {
	if t.unsigned {
		return compareUnsignedInts(a, b)
	}

	switch t.baseType {
	case BaseType_FLOAT, BaseType_DOUBLE:
		return compareFloats(a, b)
	default:
		return compareSignedInts(a, b)
	}
}

// SQL implements Type interface.
func (t numberTypeImpl) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	if t.unsigned {
		switch t.baseType {
		case BaseType_TINYINT:
			return sqltypes.MakeTrusted(sqltypes.Uint8, strconv.AppendUint(nil, cast.ToUint64(v), 10)), nil
		case BaseType_SMALLINT:
			return sqltypes.MakeTrusted(sqltypes.Uint16, strconv.AppendUint(nil, cast.ToUint64(v), 10)), nil
		case BaseType_MEDIUMINT:
			return sqltypes.MakeTrusted(sqltypes.Uint24, strconv.AppendUint(nil, cast.ToUint64(v), 10)), nil
		case BaseType_INT:
			return sqltypes.MakeTrusted(sqltypes.Uint32, strconv.AppendUint(nil, cast.ToUint64(v), 10)), nil
		case BaseType_BIGINT:
			return sqltypes.MakeTrusted(sqltypes.Uint64, strconv.AppendUint(nil, cast.ToUint64(v), 10)), nil
		}
	} else {
		switch t.baseType {
		case BaseType_TINYINT:
			return sqltypes.MakeTrusted(sqltypes.Int8, strconv.AppendInt(nil, cast.ToInt64(v), 10)), nil
		case BaseType_SMALLINT:
			return sqltypes.MakeTrusted(sqltypes.Int16, strconv.AppendInt(nil, cast.ToInt64(v), 10)), nil
		case BaseType_MEDIUMINT:
			return sqltypes.MakeTrusted(sqltypes.Int24, strconv.AppendUint(nil, cast.ToUint64(v), 10)), nil
		case BaseType_INT:
			return sqltypes.MakeTrusted(sqltypes.Int32, strconv.AppendInt(nil, cast.ToInt64(v), 10)), nil
		case BaseType_BIGINT:
			return sqltypes.MakeTrusted(sqltypes.Int64, strconv.AppendInt(nil, cast.ToInt64(v), 10)), nil
		case BaseType_FLOAT:
			return sqltypes.MakeTrusted(sqltypes.Float32, strconv.AppendFloat(nil, cast.ToFloat64(v), 'f', -1, 64)), nil
		case BaseType_DOUBLE:
			return sqltypes.MakeTrusted(sqltypes.Float64, strconv.AppendFloat(nil, cast.ToFloat64(v), 'f', -1, 64)), nil
		}
	}

	return sqltypes.MakeTrusted(sqltypes.Int64, []byte{}), nil
}

// String implements Type interface.
func (t numberTypeImpl) String() string {
	str := t.baseType.String()
	if t.unsigned {
		str += " UNSIGNED"
	}
	return str
}

// Zero implements Type interface.
func (t numberTypeImpl) Zero() interface{} {
	switch t.baseType {
	case BaseType_TINYINT:
		if t.unsigned {
			return uint8(0)
		}
		return int8(0)
	case BaseType_SMALLINT:
		if t.unsigned {
			return uint16(0)
		}
		return int16(0)
	case BaseType_MEDIUMINT:
		if t.unsigned {
			return uint32(0)
		}
		return int32(0)
	case BaseType_INT:
		if t.unsigned {
			return uint32(0)
		}
		return int32(0)
	case BaseType_BIGINT:
		if t.unsigned {
			return uint64(0)
		}
		return int64(0)
	case BaseType_FLOAT:
		return float32(0)
	case BaseType_DOUBLE:
		return float64(0)
	}
	panic(fmt.Sprintf("%v is not a valid number base type", t.baseType.String()))
}

// IsUnsigned implements NumberType interface.
func (t numberTypeImpl) IsUnsigned() bool {
	return t.unsigned
}

// IsSigned implements NumberType interface.
func (t numberTypeImpl) IsSigned() bool {
	return !t.unsigned
}