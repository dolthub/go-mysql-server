package sql

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/shopspring/decimal"
	"gopkg.in/src-d/go-errors.v1"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

type DecimalType interface {
	Type
	ConvertToDecimal(v interface{}) (decimal.NullDecimal, error)
	Precision() uint8
	Scale() uint8
}

var (
	// decimalContainer is a big.Float that can hold any digit allowed by MySQL's DECIMAL.
	// Max DECIMAL value in MySQL is a 65 digit base 10 number.
	// 10^65 < 2^216, but 10^65 > 2^215, thus 216 bits is the minimum precision required to represent that integer.
	// According to the TestDecimalAccuracy test, 216 will cause off-by-1 for some fractions, so the precision set
	// is the smallest found to allow that test to pass with zero errors.
	decimalContainer = new(big.Float).SetPrec(217).SetMode(big.ToNearestAway)

	ErrConvertingToDecimal = errors.NewKind("value %v is not a valid Decimal")
	ErrConvertingToDecimalInternal = errors.NewKind("precision above decimal could not be computed for %v")
	ErrConvertingToDecimalFrac = errors.NewKind("internal error handling fractional portion of Decimal")
	ErrConvertToDecimalLimit = errors.NewKind("value of Decimal is too large for type")
)

type decimalType struct{
	precision uint8
	scale uint8
}

// CreateDecimalType creates a DecimalType.
func CreateDecimalType(precision uint8, scale uint8) (DecimalType, error) {
	if precision > 65 {
		return nil, fmt.Errorf("%v is beyond the max precision", precision)
	}
	if scale > precision {
		return nil, fmt.Errorf("%v cannot be larger than the precision %v", scale, precision)
	}
	if scale > 30 {
		return nil, fmt.Errorf("%v is beyond the max scale", scale)
	}
	if precision == 0 {
		precision = 10
	}
	return decimalType{
		precision: precision,
		scale: scale,
	}, nil
}

// MustCreateDecimalType is the same as CreateDecimalType except it panics on errors.
func MustCreateDecimalType(precision uint8, scale uint8) DecimalType {
	bt, err := CreateDecimalType(precision, scale)
	if err != nil {
		panic(err)
	}
	return bt
}

// Type implements Type interface.
func (t decimalType) Type() query.Type {
	return sqltypes.Decimal
}

// Compare implements Type interface.
func (t decimalType) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	af, err := t.ConvertToDecimal(a)
	if err != nil {
		return 0, err
	}
	bf, err := t.ConvertToDecimal(b)
	if err != nil {
		return 0, err
	}

	return af.Decimal.Cmp(bf.Decimal), nil
}

// Convert implements Type interface.
func (t decimalType) Convert(v interface{}) (interface{}, error) {
	dec, err := t.ConvertToDecimal(v)
	if err != nil {
		return nil, err
	}
	if !dec.Valid {
		return nil, nil
	}
	return dec.Decimal.StringFixed(int32(t.scale)), nil
}

// Precision returns the precision, or total number of digits, that may be held.
func (t decimalType) ConvertToDecimal(v interface{}) (decimal.NullDecimal, error) {
	if v == nil {
		return decimal.NullDecimal{}, nil
	}

	var res decimal.Decimal

	switch value := v.(type) {
	case int:
		return t.ConvertToDecimal(int64(value))
	case uint:
		return t.ConvertToDecimal(uint64(value))
	case int8:
		return t.ConvertToDecimal(int64(value))
	case uint8:
		return t.ConvertToDecimal(uint64(value))
	case int16:
		return t.ConvertToDecimal(int64(value))
	case uint16:
		return t.ConvertToDecimal(uint64(value))
	case int32:
		res = decimal.NewFromInt32(value)
	case uint32:
		return t.ConvertToDecimal(uint64(value))
	case int64:
		res = decimal.NewFromInt(value)
	case uint64:
		res = decimal.NewFromBigInt(new(big.Int).SetUint64(value), 0)
	case float32:
		res = decimal.NewFromFloat32(value)
	case float64:
		res = decimal.NewFromFloat(value)
	case string:
		var err error
		res, err = decimal.NewFromString(value)
		if err != nil {
			// The decimal library cannot handle all of the different formats
			bf, _, err := new(big.Float).SetPrec(217).Parse(value, 0)
			if err != nil {
				return decimal.NullDecimal{}, err
			}
			res, err = decimal.NewFromString(bf.Text('f', -1))
			if err != nil {
				return decimal.NullDecimal{}, err
			}
		}
	case *big.Float:
		return t.ConvertToDecimal(value.Text('f', -1))
	case *big.Int:
		return t.ConvertToDecimal(value.Text(10))
	case *big.Rat:
		return t.ConvertToDecimal(new(big.Float).SetRat(value))
	case decimal.Decimal:
		res = value
	case decimal.NullDecimal:
		// This is the equivalent of passing in a nil
		if !value.Valid {
			return decimal.NullDecimal{}, nil
		}
		res = value.Decimal
	default:
		return decimal.NullDecimal{}, ErrConvertingToDecimal.New(v)
	}

	res = res.Round(int32(t.scale))
	max, _ := decimal.NewFromString("1" + strings.Repeat("0", int(t.precision - t.scale)))
	if res.Abs().Cmp(max) != -1 {
		return decimal.NullDecimal{}, ErrConvertToDecimalLimit.New()
	}

	return decimal.NullDecimal{Decimal: res, Valid: true}, nil
}

// MustConvert implements the Type interface.
func (t decimalType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Promote implements the Type interface.
func (t decimalType) Promote() Type {
	return MustCreateDecimalType(64, t.scale)
}

// SQL implements Type interface.
func (t decimalType) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}
	value, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}
	return sqltypes.MakeTrusted(sqltypes.Decimal, []byte(value.(string))), nil
}

// String implements Type interface.
func (t decimalType) String() string {
	return fmt.Sprintf("DECIMAL(%v,%v)", t.precision, t.scale)
}

// Zero implements Type interface. Returns a uint64 value.
func (t decimalType) Zero() interface{} {
	return decimal.NewFromInt(0).StringFixed(int32(t.scale))
}

// Precision returns the precision, or total number of digits, that may be held.
func (t decimalType) Precision() uint8 {
	return t.precision
}

// Scale returns the scale, or number of digits after the decimal, that may be held.
func (t decimalType) Scale() uint8 {
	return t.scale
}
