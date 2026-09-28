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

package expression

import (
	"fmt"
	"strings"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ErrConvertExpression is returned when a conversion is not possible.
var ErrConvertExpression = errors.NewKind("expression '%v': couldn't convert to %v")

const (
	// ConvertToBinary is a conversion to binary.
	ConvertToBinary = "binary"
	// ConvertToChar is a conversion to char.
	ConvertToChar = "char"
	// ConvertToNChar is a conversion to nchar.
	ConvertToNChar = "nchar"
	// ConvertToDate is a conversion to date.
	ConvertToDate = "date"
	// ConvertToDatetime is a conversion to datetime.
	ConvertToDatetime = "datetime"
	// ConvertToDecimal is a conversion to decimal.
	ConvertToDecimal = "decimal"
	// ConvertToFloat is a conversion to float.
	ConvertToFloat = "float"
	// ConvertToDouble is a conversion to double.
	ConvertToDouble = "double"
	// ConvertToJSON is a conversion to json.
	ConvertToJSON = "json"
	// ConvertToReal is a conversion to double.
	ConvertToReal = "real"
	// ConvertToSigned is a conversion to signed.
	ConvertToSigned = "signed"
	// ConvertToTime is a conversion to time.
	ConvertToTime = "time"
	// ConvertToYear isa convert to year.
	ConvertToYear = "year"
	// ConvertToUnsigned is a conversion to unsigned.
	ConvertToUnsigned = "unsigned"
)

// Convert represent a CAST(x AS T) or CONVERT(x, T) operation that casts x expression to type T.
type Convert struct {
	UnaryExpressionStub
	convType sql.Type

	// cachedDecimalType is the cached Decimal type for this convert expression. Because new Decimal types
	// must be created with their specific scale and precision values, unlike other types, we cache the created
	// type to avoid re-creating it on every call to Type().
	cachedDecimalType sql.DecimalType

	// castToType is a string representation of the base type to which we are casting (e.g. "char", "float", "decimal")
	castToType string
	// typeLength is the optional length parameter for types that support it (e.g. "char(10)")
	typeLength int
	// typeScale is the optional scale parameter for types that support it (e.g. "decimal(10, 2)")
	typeScale int
}

var _ sql.Expression = (*Convert)(nil)
var _ sql.CollationCoercible = (*Convert)(nil)

// CreateConvertType maps the castToType string to the matching type.
// TODO: consider moving this to planbuilder
func CreateConvertType(castToType string, typeLength, typeScale int) (sql.Type, error) {
	var res sql.Type
	var err error
	switch strings.ToLower(castToType) {
	case ConvertToBinary:
		res = types.LongBlob
	case ConvertToChar, ConvertToNChar:
		res = types.LongText
	case ConvertToDate:
		res = types.Date
	case ConvertToDatetime:
		res, err = types.CreateDatetimeType(sqltypes.Datetime, typeLength)
	case ConvertToDecimal:
		res, err = types.CreateColumnDecimalType(uint8(typeLength), uint8(typeScale))
	case ConvertToFloat:
		res = types.Float32
	case ConvertToDouble, ConvertToReal:
		res = types.Float64
	case ConvertToJSON:
		res = types.JSON
	case ConvertToSigned:
		res = types.Int64
	case ConvertToTime:
		res, err = types.CreateTimespanType(typeLength)
	case ConvertToUnsigned:
		res = types.Uint64
	case ConvertToYear:
		res = types.Year
	default:
		res = types.Null
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

// NewConvert creates a new Convert expression that will attempt to convert the specified expression |expr| into the
// |castToType| type. All optional parameters (i.e. typeLength, typeScale, and charset) are omitted and initialized
// to their zero values.
func NewConvert(expr sql.Expression, convType sql.Type) *Convert {
	disableRounding(expr)
	return &Convert{
		UnaryExpressionStub: UnaryExpressionStub{Child: expr},
		convType:            convType,
	}
}

// GetConvertToType returns which type the both left and right values should be converted to.
// If neither sql.Type represent number, then converted to string. Otherwise, we try to get
// the appropriate type to avoid any precision loss.
func GetConvertToType(l, r sql.Type) sql.Type {
	if types.Null == l && types.Null == r {
		return types.LongText
	}
	if types.Null == l {
		return GetConvertToType(r, r)
	}
	if types.Null == r {
		return GetConvertToType(l, l)
	}

	if !types.IsNumber(l) || !types.IsNumber(r) {
		// Special handling for BLOB types - preserve binary data
		if types.IsBlobType(l) || types.IsBlobType(r) {
			return types.LongBlob
		}
		return types.LongText
	}

	if types.IsDecimal(l) || types.IsDecimal(r) {
		// TODO: find scale and precision
		return types.InternalDecimalType
	}
	if types.IsBit(l) || types.IsBit(r) {
		return types.Int64
	}
	if types.IsUnsigned(l) && types.IsUnsigned(r) {
		return types.Uint64
	}
	if types.IsSigned(l) && types.IsSigned(r) {
		return types.Int64
	}
	if types.IsInteger(l) && types.IsInteger(r) {
		return types.Int64
	}

	return types.LongText
}

// IsNullable implements the Expression interface.
func (c *Convert) IsNullable(ctx *sql.Context) bool {
	// TODO: investigate
	switch c.castToType {
	case ConvertToDate, ConvertToDatetime, ConvertToBinary, ConvertToChar, ConvertToNChar:
		return true
	default:
		return c.Child.IsNullable(ctx)
	}
}

// Type implements the Expression interface.
func (c *Convert) Type(_ *sql.Context) sql.Type {
	return c.convType
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (c *Convert) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return c.convType.CollationCoercibility(ctx)
}

// String implements the Stringer interface.
func (c *Convert) String() string {
	return c.convType.String()
}

// DebugString implements the Expression interface.
func (c *Convert) DebugString(ctx *sql.Context) string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("convert")
	_ = pr.WriteChildren(fmt.Sprintf("type: %v", c.convType))
	return pr.String()
}

// WithChildren implements the Expression interface.
func (c *Convert) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return NewConvert(children[0], c.convType), nil
}

// Eval implements the Expression interface.
func (c *Convert) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	val, err := c.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// TODO: handle special errors and trimming
	origType := c.Child.Type(ctx)
	val, inRange, err := types.TypeAwareConversion(ctx, val, origType, c.convType)
	if err != nil {
		if !sql.ErrTruncatedIncorrect.Is(err) {
			return c.convType.Zero(), nil
		}
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
	}
	if inRange != sql.InRange && types.IsUnsigned(c.convType) {
		ctx.Warn(1105, "Cast to unsigned converted negative integer to its positive complement")
	}

	return val, nil
}

// convertValue converts a value from its current type to the specified target type for CAST/CONVERT operations.
// It handles type-specific conversion logic and applies length/scale constraints where applicable.
// If |typeLength| and |typeScale| are 0, they are ignored, otherwise they are used as constraints on the
// converted type where applicable (e.g. Char conversion supports only |typeLength|, Decimal conversion supports
// |typeLength| and |typeScale|).
// Only returns an error if converting to JSON, Date, and Datetime; the zero value is returned for float types.
// Nil is returned in all other cases.
func convertValue(ctx *sql.Context, val any, castTo string, origType sql.Type, typeLength, typeScale int) (any, error) {
	if val == nil {
		return nil, nil
	}
	var convType sql.Type
	var err error
	castTo = strings.ToLower(castTo)
	switch castTo {
	case ConvertToBinary:
		val, _, err = types.TypeAwareConversion(ctx, val, origType, types.LongBlob)
		if err != nil {
			return nil, nil
		}

		if types.IsTextOnly(origType) {
			// For string types we need to re-encode the string as we want the binary representation of the character set
			encoder := origType.(sql.StringType).Collation().CharacterSet().Encoder()
			encodedBytes, ok := encoder.Encode(val.([]byte))
			if !ok {
				return nil, fmt.Errorf("unable to re-encode string to convert to binary")
			}
			val = encodedBytes
		}
		if bb, ok := val.([]byte); ok && len(bb) < typeLength {
			val = append(bb, make([]byte, typeLength-len(bb))...)
		}
		return truncateConvertedValue(val, typeLength)
	case ConvertToChar, ConvertToNChar:
		val, _, err = types.TypeAwareConversion(ctx, val, origType, types.LongText)
		if err != nil {
			return nil, nil
		}
		return truncateConvertedValue(val, typeLength)
	case ConvertToJSON:
		val, _, err = types.JSON.Convert(ctx, val)
		if err != nil {
			return nil, err
		}
		return val, nil
	case ConvertToDate:
		val, _, err = types.Date.Convert(ctx, val)
		if err != nil {
			if !sql.ErrTruncatedIncorrect.Is(err) {
				return nil, err
			}
			ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
		}
		return val, nil
	case ConvertToDatetime:
		var dtType sql.Type
		dtType, err = types.CreateDatetimeType(sqltypes.Datetime, typeLength)
		if err != nil {
			return nil, err
		}
		val, _, err = dtType.Convert(ctx, val)
		if err != nil {
			if !sql.ErrTruncatedIncorrect.Is(err) {
				return nil, err
			}
			ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
		}
		return val, nil
	case ConvertToTime:
		var timeType sql.Type
		timeType, err = types.CreateTimespanType(typeLength)
		if err != nil {
			return nil, err
		}
		val, _, err = timeType.Convert(ctx, val)
		if err != nil {
			if !sql.ErrTruncatedIncorrect.Is(err) {
				return nil, err
			}
			ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
		}
		return val, nil
	case ConvertToDecimal:
		convType = createConvertedDecimalType(typeLength, typeScale, false)
	case ConvertToFloat:
		convType = types.Float32
	case ConvertToDouble, ConvertToReal:
		convType = types.Float64
	case ConvertToSigned:
		convType = types.Int64
	case ConvertToUnsigned:
		convType = types.Uint64
	case ConvertToYear:
		convType = types.Uint64
	default:
		return nil, nil
	}

	var inRange sql.ConvertInRange
	val, inRange, err = types.TypeAwareConversion(ctx, val, origType, convType)
	if err != nil {
		if !sql.ErrTruncatedIncorrect.Is(err) {
			return convType.Zero(), nil
		}
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
	}
	if inRange != sql.InRange && castTo == ConvertToUnsigned {
		ctx.Warn(1105, "Cast to unsigned converted negative integer to its positive complement")
	}
	return val, nil
}

// truncateConvertedValue truncates |val| to the specified |typeLength| if |val|
// is a string or byte slice. If the typeLength is 0, or if it is greater than
// the length of |val|, then |val| is simply returned as is. If |val| is not a
// string or []byte, then an error is returned.
func truncateConvertedValue(val any, typeLength int) (any, error) {
	if typeLength <= 0 {
		return val, nil
	}

	switch v := val.(type) {
	case []byte:
		if len(v) <= typeLength {
			typeLength = len(v)
		}
		return v[:typeLength], nil
	case string:
		if len(v) <= typeLength {
			typeLength = len(v)
		}
		return v[:typeLength], nil
	default:
		return nil, fmt.Errorf("unsupported type for truncation: %T", val)
	}
}

// createConvertedDecimalType creates a new Decimal type with the specified |precision| and |scale|. If a Decimal
// type cannot be created from the values specified, the internal Decimal type is returned. If |logErrors| is true,
// an error will also logged to the standard logger. (Setting |logErrors| to false, allows the caller to prevent
// spurious error message from being logged multiple times for the same error.) This function is intended to be
// used in places where an error cannot be returned (e.g. Node.Type(ctx) implementations), hence why it logs an error
// instead of returning one.
func createConvertedDecimalType(length, scale int, logErrors bool) sql.DecimalType {
	if length > 0 && scale > 0 {
		dt, err := types.CreateColumnDecimalType(uint8(length), uint8(scale))
		if err != nil {
			if logErrors {
				logrus.StandardLogger().Errorf("unable to create decimal type with length %d and scale %d: %v", length, scale, err)
			}
			return types.InternalDecimalType
		}
		return dt
	}
	return types.InternalDecimalType
}
