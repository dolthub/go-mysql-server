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
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/apd/v3"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/encodings"
	"github.com/dolthub/go-mysql-server/sql/values"
)

const (
	// DecimalTypeMaxPrecision returns the maximum precision allowed for the Decimal type.
	DecimalTypeMaxPrecision = 65
	// DecimalTypeMaxScale returns the maximum scale allowed for the Decimal type, assuming the
	// maximum precision is used. For a maximum scale that is relative to the precision of a given
	// decimal type, use its MaximumScale function.
	DecimalTypeMaxScale = 30

	// DecimalNaN is NaN special value represented in bytes.
	DecimalNaN = 0xc000
	// DecimalPosInf is positive Infinity special value represented in bytes.
	DecimalPosInf = 0xd000
	// DecimalNegInf is negative Infinity special value represented in bytes.
	DecimalNegInf = 0xf000
)

var (
	ErrConvertingToDecimal   = errors.NewKind("value %v is not a valid Decimal")
	ErrConvertToDecimalLimit = errors.NewKind("Out of range value for column of Decimal type ")
	ErrMarshalNullDecimal    = errors.NewKind("Decimal cannot marshal a null value")

	decimalValueType = reflect.TypeOf(&apd.Decimal{})
)

type DecimalType_ struct {
	exclusiveUpperBound *apd.Decimal
	definesColumn       bool
	precision           uint8
	scale               uint8
}

// InternalDecimalType is a special DecimalType that is used internally for Decimal comparisons. Not intended for usage
// from integrators.
var InternalDecimalType sql.DecimalType = DecimalType_{
	exclusiveUpperBound: apd.New(1, int32(65)),
	definesColumn:       false,
	precision:           65,
	scale:               30,
}

// CreateDecimalType creates a DecimalType for NON-TABLE-COLUMN.
func CreateDecimalType(precision uint8, scale uint8) (sql.DecimalType, error) {
	return createDecimalType(precision, scale, false)
}

// CreateColumnDecimalType creates a DecimalType for VALID-TABLE-COLUMN. Creating a decimal type for a column ensures that
// when operating on instances of this type, the result will be restricted to the defined precision and scale.
func CreateColumnDecimalType(precision uint8, scale uint8) (sql.DecimalType, error) {
	return createDecimalType(precision, scale, true)
}

// createDecimalType creates a DecimalType using given precision, scale
// and whether this type defines a valid table column.
func createDecimalType(precision uint8, scale uint8, definesColumn bool) (sql.DecimalType, error) {
	// check for limits for column defined types only
	if scale > DecimalTypeMaxScale {
		return nil, fmt.Errorf("Too big scale %v specified. Maximum is %v.", scale, DecimalTypeMaxScale)
	}
	if precision > DecimalTypeMaxPrecision {
		return nil, fmt.Errorf("Too big precision %v specified. Maximum is %v.", precision, DecimalTypeMaxPrecision)
	}
	if scale > precision {
		return nil, fmt.Errorf("Scale %v cannot be larger than the precision %v", scale, precision)
	}

	if precision == 0 {
		precision = 10
	}
	return DecimalType_{
		exclusiveUpperBound: apd.New(1, int32(precision-scale)),
		definesColumn:       definesColumn,
		precision:           precision,
		scale:               scale,
	}, nil
}

// CreateLiteralDecimalType is only used for literal decimal values that does not have
// the precision and scale limit. The limit only applies to column or cast types.
func CreateLiteralDecimalType(precision, scale uint8) sql.DecimalType {
	return DecimalType_{
		exclusiveUpperBound: apd.New(1, int32(precision-scale)),
		definesColumn:       false,
		precision:           precision,
		scale:               scale,
	}
}

// MustCreateDecimalType is the same as CreateDecimalType except it panics on errors and for NON-TABLE-COLUMN.
func MustCreateDecimalType(precision uint8, scale uint8) sql.DecimalType {
	dt, err := CreateDecimalType(precision, scale)
	if err != nil {
		panic(err)
	}
	return dt
}

// MustCreateColumnDecimalType is the same as CreateDecimalType except it panics on errors and for VALID-TABLE-COLUMN.
func MustCreateColumnDecimalType(precision uint8, scale uint8) sql.DecimalType {
	dt, err := CreateColumnDecimalType(precision, scale)
	if err != nil {
		panic(err)
	}
	return dt
}

// Type implements Type interface.
func (t DecimalType_) Type() query.Type {
	return sqltypes.Decimal
}

// Compare implements Type interface.
func (t DecimalType_) Compare(s context.Context, a interface{}, b interface{}) (int, error) {
	if hasNulls, res := CompareNulls(a, b); hasNulls {
		return res, nil
	}

	ad, err := t.ConvertToDecimal(a)
	if err != nil {
		return 0, err
	}
	bd, err := t.ConvertToDecimal(b)
	if err != nil {
		return 0, err
	}

	if (ad.Form == apd.NaN && bd.Form == apd.NaN) ||
		(ad.Form == apd.Infinite && bd.Form == apd.Infinite && ad.Negative == bd.Negative) {
		return 0, nil
	}
	if ad.Form == apd.NaN {
		return 1, nil
	}
	if bd.Form == apd.NaN {
		return -1, nil
	}
	return ad.Cmp(bd), nil
}

// CompareValue implements the ValueType interface
func (t DecimalType_) CompareValue(ctx *sql.Context, a, b sql.Value) (int, error) {
	if hasNulls, res := CompareNullValues(a, b); hasNulls {
		return res, nil
	}
	aDec, err := convertValueToDecimal(ctx, a)
	if err != nil {
		return 0, err
	}
	bDec, err := convertValueToDecimal(ctx, b)
	if err != nil {
		return 0, err
	}

	if (aDec.Form == apd.NaN && bDec.Form == apd.NaN) ||
		(aDec.Form == apd.Infinite && bDec.Form == apd.Infinite && aDec.Negative == bDec.Negative) {
		return 0, nil
	}
	if aDec.Form == apd.NaN {
		return 1, nil
	}
	if bDec.Form == apd.NaN {
		return -1, nil
	}

	return aDec.Cmp(bDec), nil
}

// Convert implements Type interface.
func (t DecimalType_) Convert(c context.Context, v interface{}) (interface{}, sql.ConvertInRange, error) {
	dec, err := t.ConvertToDecimal(v)
	if err != nil && !sql.ErrTruncatedIncorrect.Is(err) {
		return nil, sql.InRange, err
	}
	if dec == nil {
		return nil, sql.InRange, nil
	}
	res, inRange, cErr := t.BoundsCheck(dec)
	if cErr != nil {
		return nil, inRange, cErr
	}
	return res, inRange, err
}

func (t DecimalType_) ConvertToDecimal(v interface{}) (*apd.Decimal, error) {
	if v == nil {
		return nil, nil
	}

	switch value := v.(type) {
	case bool:
		if value {
			return t.ConvertToDecimal(DecimalFromInt64(1))
		} else {
			return t.ConvertToDecimal(apd.New(0, 0))
		}
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
		return t.ConvertToDecimal(DecimalFromInt64(int64(value)))
	case uint32:
		return t.ConvertToDecimal(uint64(value))
	case int64:
		return t.ConvertToDecimal(DecimalFromInt64(value))
	case uint64:
		return t.ConvertToDecimal(DecimalFromUint64(value))
	case float32:
		return t.ConvertToDecimal(DecimalFromFloat64(float64(value)))
	case float64:
		return t.ConvertToDecimal(DecimalFromFloat64(value))
	case string:
		truncStr := strings.Trim(value, sql.NumericCutSet)
		res, _, err := apd.NewFromString(truncStr)
		if err == nil {
			return t.ConvertToDecimal(res)
		}
		// The decimal library cannot handle all the different formats
		bf, _, err := new(big.Float).SetPrec(217).Parse(truncStr, 0)
		if err == nil {
			res, _, err = apd.NewFromString(bf.Text('f', -1))
			if err == nil {
				return t.ConvertToDecimal(res)
			}
		}
		truncStr, didTrunc := TruncateStringToDouble(value)
		if truncStr == "0" {
			nullDec, cErr := t.ConvertToDecimal(apd.New(0, 0))
			if cErr != nil {
				return nil, cErr
			}
			if didTrunc {
				return nullDec, sql.ErrTruncatedIncorrect.New(t, value)
			}
			return nullDec, nil
		}
		res, _, _ = apd.NewFromString(truncStr)
		nullDec, cErr := t.ConvertToDecimal(res)
		if cErr != nil {
			return nil, cErr
		}
		if didTrunc {
			err = sql.ErrTruncatedIncorrect.New(t, value)
		}
		return nullDec, err
	case *big.Float:
		return t.ConvertToDecimal(value.Text('f', -1))
	case *big.Int:
		return t.ConvertToDecimal(value.Text(10))
	case *big.Rat:
		return t.ConvertToDecimal(new(big.Float).SetRat(value))
	case *apd.Decimal:
		newVal := new(*value)
		if t.definesColumn && value.Exponent != int32(t.scale) {
			var err error
			newVal, err = sql.DecimalRound(value, int32(t.scale))
			if err != nil {
				return nil, err
			}
		}
		return newVal, nil
	case []uint8:
		return t.ConvertToDecimal(string(value))
	case JSONDocument:
		return t.ConvertToDecimal(value.Val)
	}

	return nil, ErrConvertingToDecimal.New(v)
}

func (t DecimalType_) BoundsCheck(v *apd.Decimal) (*apd.Decimal, sql.ConvertInRange, error) {
	newVal := new(*v)
	if -v.Exponent > int32(t.scale) {
		// TODO : add 'Data truncated' warning
		var err error
		newVal, err = sql.DecimalRound(v, int32(t.scale))
		if err != nil {
			return nil, sql.InRange, err
		}
	}
	// TODO add shortcut for common case
	// ex: certain num of bits fast tracks OK
	tmp := new(apd.Decimal)
	if tmp.Abs(newVal).Cmp(t.exclusiveUpperBound) >= 0 {
		return nil, sql.InRange, ErrConvertToDecimalLimit.New()
	}
	return newVal, sql.InRange, nil
}

// Equals implements the Type interface.
func (t DecimalType_) Equals(otherType sql.Type) bool {
	if ot, ok := otherType.(DecimalType_); ok {
		return t.precision == ot.precision && t.scale == ot.scale
	}
	return false
}

// MaxTextResponseByteLength implements the Type interface
func (t DecimalType_) MaxTextResponseByteLength(*sql.Context) uint32 {
	if t.scale == 0 {
		// if no digits are reserved for the right-hand side of the decimal point,
		// just return precision plus one byte for sign
		return uint32(t.precision + 1)
	} else {
		// otherwise return precision plus one byte for sign plus one byte for the decimal point
		return uint32(t.precision + 2)
	}
}

// Promote implements the Type interface.
func (t DecimalType_) Promote() sql.Type {
	if t.definesColumn {
		return MustCreateColumnDecimalType(DecimalTypeMaxPrecision, t.scale)
	}
	return MustCreateDecimalType(DecimalTypeMaxPrecision, t.scale)
}

// SQL implements Type interface.
func (t DecimalType_) SQL(ctx *sql.Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}
	value, err := t.ConvertToDecimal(v)
	if err != nil {
		return sqltypes.Value{}, err
	}
	val := encodings.StringToBytes(t.DecimalValueStringFixed(value))
	return sqltypes.MakeTrusted(sqltypes.Decimal, val), nil
}

func (t DecimalType_) SQLValue(ctx *sql.Context, v sql.Value, dest []byte) (sqltypes.Value, error) {
	if v.IsNull() {
		return sqltypes.NULL, nil
	}
	d := values.ReadDecimal(v.Val)
	return sqltypes.MakeTrusted(sqltypes.Decimal, encodings.StringToBytes(t.DecimalValueStringFixed(d))), nil
}

// String implements Type interface.
func (t DecimalType_) String() string {
	return fmt.Sprintf("decimal(%v,%v)", t.precision, t.scale)
}

// ValueType implements Type interface.
func (t DecimalType_) ValueType() reflect.Type {
	return decimalValueType
}

// Zero implements Type interface.
func (t DecimalType_) Zero() interface{} {
	// The zero value should have the same scale as the type
	return apd.New(0, -int32(t.scale))
}

// CollationCoercibility implements sql.CollationCoercible interface.
func (DecimalType_) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// ExclusiveUpperBound implements DecimalType interface.
func (t DecimalType_) ExclusiveUpperBound() *apd.Decimal {
	return t.exclusiveUpperBound
}

// MaximumScale implements DecimalType interface.
func (t DecimalType_) MaximumScale() uint8 {
	if t.precision >= DecimalTypeMaxScale {
		return DecimalTypeMaxScale
	}
	return t.precision
}

// Precision implements DecimalType interface.
func (t DecimalType_) Precision() uint8 {
	return t.precision
}

// Scale implements DecimalType interface.
func (t DecimalType_) Scale() uint8 {
	return t.scale
}

// DecimalValueStringFixed returns string value for the given decimal value. If decimal type value is for valid table column only,
// it should use scale defined by the column. Otherwise, the result value should use its own precision and scale.
func (t DecimalType_) DecimalValueStringFixed(v *apd.Decimal) string {
	if t.definesColumn {
		if int32(t.scale) != v.Exponent {
			v, _ = sql.DecimalRound(v, int32(t.scale))
		}
	}
	return v.Text('f')
}

func convertValueToDecimal(ctx *sql.Context, v sql.Value) (*apd.Decimal, error) {
	switch v.Typ {
	case sqltypes.Int8:
		x := values.ReadInt8(v.Val)
		return DecimalFromInt64(int64(x)), nil
	case sqltypes.Int16:
		x := values.ReadInt16(v.Val)
		return DecimalFromInt64(int64(x)), nil
	case sqltypes.Int32:
		x := values.ReadInt32(v.Val)
		return DecimalFromInt64(int64(x)), nil
	case sqltypes.Int64:
		x := values.ReadInt64(v.Val)
		return DecimalFromInt64(x), nil
	case sqltypes.Uint8:
		x := values.ReadUint8(v.Val)
		return DecimalFromInt64(int64(x)), nil
	case sqltypes.Uint16:
		x := values.ReadUint16(v.Val)
		return DecimalFromInt64(int64(x)), nil
	case sqltypes.Uint32:
		x := values.ReadUint32(v.Val)
		return DecimalFromInt64(int64(x)), nil
	case sqltypes.Uint64:
		x := values.ReadUint64(v.Val)
		return DecimalFromUint64(x), nil
	case sqltypes.Float32:
		x := values.ReadFloat32(v.Val)
		return DecimalFromFloat32(x), nil
	case sqltypes.Float64:
		x := values.ReadFloat64(v.Val)
		return DecimalFromFloat64(x), nil
	case sqltypes.Decimal:
		x := values.ReadDecimal(v.Val)
		return x, nil
	case sqltypes.Bit:
		x := values.ReadUint64(v.Val)
		return DecimalFromUint64(x), nil
	case sqltypes.Year:
		x := values.ReadUint16(v.Val)
		return DecimalFromInt64(int64(x)), nil
	case sqltypes.Date:
		x := values.ReadDate(v.Val)
		s := x.UTC().Unix()
		return DecimalFromInt64(s), nil
	case sqltypes.Time:
		x := values.ReadInt64(v.Val)
		return DecimalFromInt64(x), nil
	case sqltypes.Datetime, sqltypes.Timestamp:
		x := values.ReadDatetime(v.Val)
		return DecimalFromInt64(x.UTC().Unix()), nil
	case sqltypes.Text, sqltypes.Blob:
		var err error
		if v.Val == nil {
			v.Val, err = v.WrappedVal.Unwrap(ctx)
			if err != nil {
				return nil, err
			}
		}
		x := values.ReadString(v.Val)
		res, _, err := apd.NewFromString(x)
		if err != nil {
			return nil, err
		}
		return res, nil
	default:
		return nil, ErrConvertingToDecimal.New(v)
	}
}

// IsDecimalType implements the sql.DecimalType
func (t DecimalType_) IsDecimalType() bool {
	return true
}

// DecimalFromFloat32 returns *apd.Decimal set from given float32.
func DecimalFromFloat32(f float32) *apd.Decimal {
	dec := new(apd.Decimal)
	s := strconv.FormatFloat(float64(f), 'f', -1, 32)
	dec, _, err := dec.SetString(s)
	if err != nil {
		panic(err)
	}
	return dec
}

// DecimalFromFloat64 returns *apd.Decimal set from given float64.
func DecimalFromFloat64(f float64) *apd.Decimal {
	dec := new(apd.Decimal)
	dec, err := dec.SetFloat64(f)
	if err != nil {
		panic(err)
	}
	return dec
}

// DecimalFromInt64 returns *apd.Decimal set from given int64 with 0 scale.
func DecimalFromInt64(x int64) *apd.Decimal {
	return apd.New(x, 0)
}

// DecimalFromUint64 returns *apd.Decimal set from given uint64 value.
func DecimalFromUint64(x uint64) *apd.Decimal {
	dec := new(apd.Decimal)
	dec.Coeff.SetMathBigInt(new(big.Int).SetUint64(x))
	dec.Exponent = 0
	return dec
}

// DecimalIntPart rounds the decimal value to 0 scale and returns the integer part int64.
func DecimalIntPart(val *apd.Decimal) int64 {
	v, _ := sql.DecimalRound(val, 0)
	i, _ := v.Int64()
	return i
}

// DecimalIntPartUint64 rounds the decimal value to 0 scale and returns the integer part uint64.
func DecimalIntPartUint64(val *apd.Decimal) uint64 {
	v, _ := sql.DecimalRound(val, 0)
	return v.Coeff.Uint64()
}

// DecimalDivRound divides and rounds to a given scale.
func DecimalDivRound(a, b *apd.Decimal, scale int32) *apd.Decimal {
	res := new(apd.Decimal)
	// need to set precision for decimal context because it does division operation.
	// TODO: can find precision for context based on the given values instead of using MaxExponent
	_, err := sql.DecimalHighPrecisionCtx.Quo(res, a, b)
	if err != nil {
		panic(err)
	}
	_, err = sql.DecimalHighPrecisionCtx.Quantize(res, res, -scale)
	if err != nil {
		panic(err)
	}
	return res
}

// DecimalMod mods the given values.
func DecimalMod(a, b *apd.Decimal) (*apd.Decimal, error) {
	res := new(apd.Decimal)
	m := math.Max(float64(a.NumDigits()), float64(b.NumDigits()))
	// need to set precision for decimal context because it does division operation.
	_, err := sql.DecimalCtx.WithPrecision(uint32(m)).Rem(res, a, b)
	return res, err
}

// DecimalTruncate truncates the decimal to given scale. It rounds down.
// 5.45 truncated with scale of 1 = 5.40
// 545 truncated with scale of -1 = 540
func DecimalTruncate(val *apd.Decimal, scale int32) *apd.Decimal {
	if -scale > val.Exponent {
		ctx := *sql.DecimalHighPrecisionCtx
		ctx.Rounding = apd.RoundDown
		newVal := new(*val)
		_, err := ctx.Quantize(newVal, val, -scale)
		if err != nil {
			panic(err)
		}
		if newVal.IsZero() {
			newVal.Negative = false
			newVal.Exponent = 0
		}
		return newVal
	}
	return val
}
