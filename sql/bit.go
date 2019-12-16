package sql

import (
	"fmt"
	"strconv"

	"github.com/spf13/cast"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

var promotedBitType = MustCreateBitType(64)

// Represents the BIT type.
// https://dev.mysql.com/doc/refman/8.0/en/bit-type.html
type BitType interface {
	Type
	NumberOfBits() uint8
}

type bitType struct{
	numOfBits uint8
}

// CreateBitType creates a BitType.
func CreateBitType(numOfBits uint8) (BitType, error) {
	if numOfBits == 0 || numOfBits > 64 {
		return nil, fmt.Errorf("%v is an invalid number of bits", numOfBits)
	}
	return bitType{
		numOfBits: numOfBits,
	}, nil
}

// MustCreateBitType is the same as CreateBitType except it panics on errors.
func MustCreateBitType(numOfBits uint8) BitType {
	bt, err := CreateBitType(numOfBits)
	if err != nil {
		panic(err)
	}
	return bt
}

// Compare implements Type interface.
func (t bitType) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	ac, err := t.Convert(a)
	if err != nil {
		return 0, err
	}
	bc, err := t.Convert(b)
	if err != nil {
		return 0, err
	}

	ai := ac.(uint64)
	bi := bc.(uint64)
	if ai < bi {
		return -1, nil
	} else if ai > bi {
		return 1, nil
	}
	return 0, nil
}

// Convert implements Type interface.
func (t bitType) Convert(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	if _, ok := v.(string); ok {
		return nil, ErrInvalidType.New(t)
	}

	value, err := cast.ToUint64E(v)
	if err != nil {
		return nil, err
	}
	if value > uint64(1 << t.numOfBits - 1) {
		return nil, fmt.Errorf("%v is beyond the maximum value that can be held by %v bits", value, t.numOfBits)
	}
	return value, nil
}

// MustConvert implements the Type interface.
func (t bitType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Promote implements the Type interface.
func (t bitType) Promote() Type {
	return promotedBitType
}

// SQL implements Type interface.
func (t bitType) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}
	value, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}
	return sqltypes.MakeTrusted(sqltypes.Bit, strconv.AppendUint(nil, value.(uint64), 10)), nil
}

// String implements Type interface.
func (t bitType) String() string {
	return fmt.Sprintf("BIT(%v)", t.numOfBits)
}

// Type implements Type interface.
func (t bitType) Type() query.Type {
	return sqltypes.Bit
}

// Zero implements Type interface. Returns a uint64 value.
func (t bitType) Zero() interface{} {
	return uint64(0)
}

// NumberOfBits returns the number of bits that this type may contain.
func (t bitType) NumberOfBits() uint8 {
	return t.numOfBits
}