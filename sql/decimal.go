package sql

import (
	"fmt"
	"math/big"
	"strings"

	"gopkg.in/src-d/go-errors.v1"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

type DecimalType interface {
	Type
	ConvertToBigFloat(v interface{}) (*big.Float, error)
	FormatBigFloat(bigFloat *big.Float) string
	FormatDecimalStringToMySQL(decStr string) string
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

	af, err := t.ConvertToBigFloat(a)
	if err != nil {
		return 0, err
	}
	bf, err := t.ConvertToBigFloat(b)
	if err != nil {
		return 0, err
	}

	return af.Cmp(bf), nil
}

// Convert implements Type interface.
func (t decimalType) Convert(v interface{}) (interface{}, error) {
	float, err := t.ConvertToBigFloat(v)
	if err != nil {
		return nil, err
	}
	if float == nil {
		return nil, nil
	}
	decStr := t.FormatBigFloat(float)
	return t.FormatDecimalStringToMySQL(decStr), nil
}

// Precision returns the precision, or total number of digits, that may be held.
func (t decimalType) ConvertToBigFloat(v interface{}) (*big.Float, error) {
	if v == nil {
		return nil, nil
	}

	res := new(big.Float).Copy(decimalContainer)

	switch value := v.(type) {
	case int:
		return t.ConvertToBigFloat(int64(value))
	case uint:
		return t.ConvertToBigFloat(uint64(value))
	case int8:
		return t.ConvertToBigFloat(int64(value))
	case uint8:
		return t.ConvertToBigFloat(uint64(value))
	case int16:
		return t.ConvertToBigFloat(int64(value))
	case uint16:
		return t.ConvertToBigFloat(uint64(value))
	case int32:
		return t.ConvertToBigFloat(int64(value))
	case uint32:
		return t.ConvertToBigFloat(uint64(value))
	case int64:
		res.SetInt64(value)
	case uint64:
		res.SetUint64(value)
	case float32:
		return t.ConvertToBigFloat(float64(value))
	case float64:
		res.SetFloat64(value)
	case string:
		_, _, err := res.Parse(value, 0)
		if err != nil {
			return nil, err
		}
	case *big.Float:
		res.Set(value)
	case *big.Int:
		res.SetInt(value)
	case *big.Rat:
		res.SetRat(value)
	default:
		return nil, ErrConvertingToDecimal.New(v)
	}

	// This does rounding on the fractional portion since we internally use a *big.Float which has issue with exact values
	if !res.IsInt() && !res.IsInf() {
		resStr := res.Text('f', -1)
		dotIndex := strings.Index(resStr, ".")
		if dotIndex != -1 {
			fracStr := resStr[dotIndex+1:]
			if len(fracStr) > int(t.scale) {
				if len(fracStr) > int(t.scale) + 1 {
					fracStr = fracStr[:int(t.scale)+1]
				}
				frac := new(big.Int)
				err := frac.UnmarshalText([]byte(fracStr))
				if err != nil {
					return nil, ErrConvertingToDecimalFrac.Wrap(err)
				}
				frac.Add(frac, big.NewInt(5))
				resAsInt, _ := res.Int(nil)
				upperBound := new(big.Int)
				_ = upperBound.UnmarshalText([]byte("1" + strings.Repeat("0", int(t.scale + 1))))
				if frac.Cmp(upperBound) == -1 {
					frac.Div(frac, big.NewInt(10))
					// When |res| < 1 then resAsInt becomes 0, which loses the sign when res < 0
					if res.Sign() != resAsInt.Sign() && res.Sign() == -1 {
						_, _, err = res.Parse("-0." + frac.Text(10), 10)
					} else {
						_, _, err = res.Parse(resAsInt.Text(10) + "." + frac.Text(10), 10)
					}
					if err != nil {
						return nil, ErrConvertingToDecimalFrac.Wrap(err)
					}
				} else {
					if res.Sign() == -1 {
						resAsInt.Sub(resAsInt, big.NewInt(1))
					} else {
						resAsInt.Add(resAsInt, big.NewInt(1))
					}
					res.SetInt(resAsInt)
				}
			}
		}
	}

	// Check if we're above the max value for this type
	max, _, err := new(big.Float).Copy(decimalContainer).Parse(fmt.Sprintf("1.0e%d", t.precision - t.scale), 0)
	if err != nil {
		return nil, ErrConvertingToDecimalInternal.New(t.precision - t.scale)
	}
	if new(big.Float).Abs(res).Cmp(max) != -1 {
		return nil, ErrConvertToDecimalLimit.New()
	}

	return res, nil
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
	return t.FormatDecimalStringToMySQL("0")
}

// FormatBigFloat returns a concise decimal representation of the given *big.Float.
func (t decimalType) FormatBigFloat(bigFloat *big.Float) string {
	return bigFloat.Text('f', -1)
}

// FormatDecimalString takes a decimal representation as returned by FormatBigFloat and returns a
// padded decimal string as returned by MySQL.
func (t decimalType) FormatDecimalStringToMySQL(decStr string) string {
	if decIndex := strings.Index(decStr, "."); decIndex != -1 {
		fracLength := len(decStr[decIndex+1:])
		if fracLength < int(t.scale) {
			decStr += strings.Repeat("0", int(t.scale) - fracLength)
		}
	} else if t.scale != 0 {
		decStr += "." + strings.Repeat("0", int(t.scale))
	}
	return decStr
}

// Precision returns the precision, or total number of digits, that may be held.
func (t decimalType) Precision() uint8 {
	return t.precision
}

// Scale returns the scale, or number of digits after the decimal, that may be held.
func (t decimalType) Scale() uint8 {
	return t.scale
}
