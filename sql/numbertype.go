package sql

import (
	"fmt"
	"strconv"
	"time"
	"vitess.io/vitess/go/vt/proto/query"

	"github.com/spf13/cast"
	"vitess.io/vitess/go/sqltypes"
)

var (
	// Boolean is a synonym for TINYINT
	Boolean = Int8
	// Int8 is an integer of 8 bits
	Int8 = MustCreateNumberType(sqltypes.Int8)
	// Uint8 is an unsigned integer of 8 bits
	Uint8 = MustCreateNumberType(sqltypes.Uint8)
	// Int16 is an integer of 16 bits
	Int16 = MustCreateNumberType(sqltypes.Int16)
	// Uint16 is an unsigned integer of 16 bits
	Uint16 = MustCreateNumberType(sqltypes.Uint16)
	// Int24 is an integer of 24 bits.
	Int24 = MustCreateNumberType(sqltypes.Int24)
	// Uint24 is an unsigned integer of 24 bits.
	Uint24 = MustCreateNumberType(sqltypes.Uint24)
	// Int32 is an integer of 32 bits.
	Int32 = MustCreateNumberType(sqltypes.Int32)
	// Uint32 is an unsigned integer of 32 bits.
	Uint32 = MustCreateNumberType(sqltypes.Uint32)
	// Int64 is an integer of 64 bytes.
	Int64 = MustCreateNumberType(sqltypes.Int64)
	// Uint64 is an unsigned integer of 64 bits.
	Uint64 = MustCreateNumberType(sqltypes.Uint64)
	// Float32 is a floating point number of 32 bits.
	Float32 = MustCreateNumberType(sqltypes.Float32)
	// Float64 is a floating point number of 64 bits.
	Float64 = MustCreateNumberType(sqltypes.Float64)
)

type NumberType interface {
	Type
	IsUnsigned() bool
	IsSigned() bool
}

type numberTypeImpl struct {
	baseType query.Type
}

// CreateNumberType creates a NumberType.
func CreateNumberType(baseType query.Type) (NumberType, error) {
	switch baseType {
	case sqltypes.Int8, sqltypes.Uint8, sqltypes.Int16, sqltypes.Uint16, sqltypes.Int24, sqltypes.Uint24,
	sqltypes.Int32, sqltypes.Uint32, sqltypes.Int64, sqltypes.Uint64, sqltypes.Float32, sqltypes.Float64:
		return numberTypeImpl{
			baseType: baseType,
		}, nil
	}
	return nil, fmt.Errorf("%v is not a valid number base type", baseType.String())
}

// MustCreateNumberType is the same as CreateNumberType except it panics on errors.
func MustCreateNumberType(baseType query.Type) NumberType {
	nt, err := CreateNumberType(baseType)
	if err != nil {
		panic(err)
	}
	return nt
}

// Convert implements Type interface.
func (t numberTypeImpl) Convert(v interface{}) (interface{}, error) {
	if ti, ok := v.(time.Time); ok {
		v = float64(ti.Unix()) + (float64(ti.Nanosecond()) / float64(time.Second/time.Nanosecond))
	}

	switch t.baseType {
	case sqltypes.Int8:
		return cast.ToInt8E(v)
	case sqltypes.Uint8:
		return cast.ToUint8E(v)
	case sqltypes.Int16:
		return cast.ToInt16E(v)
	case sqltypes.Uint16:
		return cast.ToUint16E(v)
	case sqltypes.Int24:
		return cast.ToInt32E(v)
	case sqltypes.Uint24:
		return cast.ToUint32E(v)
	case sqltypes.Int32:
		return cast.ToInt32E(v)
	case sqltypes.Uint32:
		return cast.ToUint32E(v)
	case sqltypes.Int64:
		return cast.ToInt64E(v)
	case sqltypes.Uint64:
		return cast.ToUint64E(v)
	case sqltypes.Float32:
		return cast.ToFloat32E(v)
	case sqltypes.Float64:
		return cast.ToFloat64E(v)
	}
	return nil, ErrInvalidType.New(t.baseType.String())
}

// Compare implements Type interface.
func (t numberTypeImpl) Compare(a interface{}, b interface{}) (int, error) {
	switch t.baseType {
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64:
		return compareUnsignedInts(a, b)
	case sqltypes.Float32, sqltypes.Float64:
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

	switch t.baseType {
	case sqltypes.Int8:
		return sqltypes.MakeTrusted(sqltypes.Int8, strconv.AppendInt(nil, cast.ToInt64(v), 10)), nil
	case sqltypes.Uint8:
		return sqltypes.MakeTrusted(sqltypes.Uint8, strconv.AppendUint(nil, cast.ToUint64(v), 10)), nil
	case sqltypes.Int16:
		return sqltypes.MakeTrusted(sqltypes.Int16, strconv.AppendInt(nil, cast.ToInt64(v), 10)), nil
	case sqltypes.Uint16:
		return sqltypes.MakeTrusted(sqltypes.Uint16, strconv.AppendUint(nil, cast.ToUint64(v), 10)), nil
	case sqltypes.Int24:
		return sqltypes.MakeTrusted(sqltypes.Int24, strconv.AppendUint(nil, cast.ToUint64(v), 10)), nil
	case sqltypes.Uint24:
		return sqltypes.MakeTrusted(sqltypes.Uint24, strconv.AppendUint(nil, cast.ToUint64(v), 10)), nil
	case sqltypes.Int32:
		return sqltypes.MakeTrusted(sqltypes.Int32, strconv.AppendInt(nil, cast.ToInt64(v), 10)), nil
	case sqltypes.Uint32:
		return sqltypes.MakeTrusted(sqltypes.Uint32, strconv.AppendUint(nil, cast.ToUint64(v), 10)), nil
	case sqltypes.Int64:
		return sqltypes.MakeTrusted(sqltypes.Int64, strconv.AppendInt(nil, cast.ToInt64(v), 10)), nil
	case sqltypes.Uint64:
		return sqltypes.MakeTrusted(sqltypes.Uint64, strconv.AppendUint(nil, cast.ToUint64(v), 10)), nil
	case sqltypes.Float32:
		return sqltypes.MakeTrusted(sqltypes.Float32, strconv.AppendFloat(nil, cast.ToFloat64(v), 'f', -1, 64)), nil
	case sqltypes.Float64:
		return sqltypes.MakeTrusted(sqltypes.Float64, strconv.AppendFloat(nil, cast.ToFloat64(v), 'f', -1, 64)), nil
	}

	return sqltypes.MakeTrusted(sqltypes.Int64, []byte{}), nil
}

// String implements Type interface.
func (t numberTypeImpl) String() string {
	switch t.baseType {
	case sqltypes.Int8:
		return "TINYINT"
	case sqltypes.Uint8:
		return "TINYINT UNSIGNED"
	case sqltypes.Int16:
		return "SMALLINT"
	case sqltypes.Uint16:
		return "SMALLINT UNSIGNED"
	case sqltypes.Int24:
		return "MEDIUMINT"
	case sqltypes.Uint24:
		return "MEDIUMINT UNSIGNED"
	case sqltypes.Int32:
		return "INT"
	case sqltypes.Uint32:
		return "INT UNSIGNED"
	case sqltypes.Int64:
		return "BIGINT"
	case sqltypes.Uint64:
		return "BIGINT UNSIGNED"
	case sqltypes.Float32:
		return "FLOAT"
	case sqltypes.Float64:
		return "DOUBLE"
	}
	panic(fmt.Sprintf("%v is not a valid number base type", t.baseType.String()))
}

// Type implements Type interface.
func (t numberTypeImpl) Type() query.Type {
	return t.baseType
}

// Zero implements Type interface.
func (t numberTypeImpl) Zero() interface{} {
	switch t.baseType {
	case sqltypes.Int8:
		return int8(0)
	case sqltypes.Uint8:
		return uint8(0)
	case sqltypes.Int16:
		return int16(0)
	case sqltypes.Uint16:
		return uint16(0)
	case sqltypes.Int24:
		return int32(0)
	case sqltypes.Uint24:
		return uint32(0)
	case sqltypes.Int32:
		return int32(0)
	case sqltypes.Uint32:
		return uint32(0)
	case sqltypes.Int64:
		return int64(0)
	case sqltypes.Uint64:
		return uint64(0)
	case sqltypes.Float32:
		return float32(0)
	case sqltypes.Float64:
		return float64(0)
	}
	panic(fmt.Sprintf("%v is not a valid number base type", t.baseType.String()))
}

// IsUnsigned implements NumberType interface.
func (t numberTypeImpl) IsUnsigned() bool {
	switch t.baseType {
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64:
		return true
	}
	return false
}

// IsSigned implements NumberType interface.
func (t numberTypeImpl) IsSigned() bool {
	switch t.baseType {
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int24, sqltypes.Int32, sqltypes.Int64, sqltypes.Float32, sqltypes.Float64:
		return true
	}
	return false
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