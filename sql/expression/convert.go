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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

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
	// convType is the type to which we are casting
	convType sql.Type
	// typeLength is the optional length parameter for char and binary conversions (e.g. "char(10)"), which is used to
	// truncate (and pad, for binary) the converted value
	typeLength int
}

var _ sql.Expression = (*Convert)(nil)
var _ sql.CollationCoercible = (*Convert)(nil)

// CreateConvertType returns the type that a CAST(x AS T) or CONVERT(x, T) operation converts to, where |castToType|
// is the base name of the target type (e.g. "char", "float", "decimal"), and |typeLength| and |typeScale| are the
// optional length and scale parameters of the target type (e.g. "decimal(10, 2)"). An error is returned if the
// length or scale are out of range for the target type.
func CreateConvertType(castToType string, typeLength, typeScale int) (sql.Type, error) {
	switch strings.ToLower(castToType) {
	case ConvertToBinary:
		if int64(typeLength) > types.LongTextBlobMax {
			return nil, sql.ErrTooBigDisplayWidth.New("cast as binary", types.LongTextBlobMax)
		}
		return types.LongBlob, nil
	case ConvertToChar, ConvertToNChar:
		if int64(typeLength) > types.LongTextBlobMax {
			return nil, sql.ErrTooBigDisplayWidth.New("cast as char", types.LongTextBlobMax)
		}
		return types.LongText, nil
	case ConvertToDate:
		return types.Date, nil
	case ConvertToDatetime:
		return types.CreateDatetimeType(sqltypes.Datetime, typeLength)
	case ConvertToDecimal:
		if typeLength > types.DecimalTypeMaxPrecision {
			return nil, sql.ErrTooBigPrecision.New(typeLength, types.DecimalTypeMaxPrecision)
		}
		if typeScale > types.DecimalTypeMaxScale {
			return nil, sql.ErrTooBigScale.New(typeScale, types.DecimalTypeMaxScale)
		}
		return createConvertedDecimalType(typeLength, typeScale)
	case ConvertToFloat:
		return types.Float32, nil
	case ConvertToDouble, ConvertToReal:
		return types.Float64, nil
	case ConvertToJSON:
		return types.JSON, nil
	case ConvertToSigned:
		return types.Int64, nil
	case ConvertToTime:
		return types.CreateTimespanType(typeLength)
	case ConvertToUnsigned:
		return types.Uint64, nil
	case ConvertToYear:
		return types.Year, nil
	default:
		return types.Null, nil
	}
}

// NewConvert creates a new Convert expression that will attempt to convert the specified expression |expr| into the
// |convType| type.
func NewConvert(expr sql.Expression, convType sql.Type) *Convert {
	return NewConvertWithLength(expr, convType, 0)
}

// NewConvertWithLength creates a new Convert expression that will attempt to convert |expr| into the |convType| type,
// with |typeLength| specifying a length constraint for char and binary conversions. |typeLength| is ignored for all
// other types, whose constraints are defined by |convType| itself.
func NewConvertWithLength(expr sql.Expression, convType sql.Type, typeLength int) *Convert {
	disableRounding(expr)
	return &Convert{
		UnaryExpressionStub: UnaryExpressionStub{Child: expr},
		convType:            convType,
		typeLength:          typeLength,
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
	if types.IsTime(c.convType) || types.IsText(c.convType) {
		return true
	}
	return c.Child.IsNullable(ctx)
}

// Type implements the Expression interface.
func (c *Convert) Type(_ *sql.Context) sql.Type {
	return c.convType
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (c *Convert) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	switch {
	case types.IsJSON(c.convType):
		return ctx.GetCharacterSet().BinaryCollation(), 2
	case types.IsBinaryType(c.convType):
		return sql.Collation_binary, 2
	case types.IsText(c.convType):
		return ctx.GetCollation(), 2
	case types.IsNullType(c.convType):
		return sql.Collation_binary, 7
	default:
		return sql.Collation_binary, 5
	}
}

// String implements the Stringer interface.
func (c *Convert) String() string {
	return fmt.Sprintf("convert(%v, %s)", c.Child, c.convType.String())
}

// DebugString implements the Expression interface.
func (c *Convert) DebugString(ctx *sql.Context) string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("convert")
	children := []string{
		fmt.Sprintf("type: %v", c.convType),
	}

	if c.typeLength > 0 {
		children = append(children, fmt.Sprintf("typeLength: %v", c.typeLength))
	}
	children = append(children, sql.DebugString(ctx, c.Child))
	_ = pr.WriteChildren(children...)
	return pr.String()
}

// WithChildren implements the Expression interface.
func (c *Convert) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return NewConvertWithLength(children[0], c.convType, c.typeLength), nil
}

// Eval implements the Expression interface.
func (c *Convert) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	val, err := c.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	casted, err := convertValue(ctx, val, c.Child.Type(ctx), c.convType, c.typeLength)
	if err != nil {
		// Conversions to JSON return an error, all other conversions return nil and a warning instead
		if types.IsJSON(c.convType) {
			return nil, err
		}
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
		return nil, nil
	}
	return casted, nil
}

// convertValue converts a value from its current type |origType| to the target type |convType| for CAST/CONVERT
// operations. It handles type-specific conversion logic. If |typeLength| is greater than 0, it is used as a length
// constraint for char and binary conversions; it is ignored for all other types.
// Only returns an error if converting to JSON, Date, Datetime, and Time. The zero value is returned for numeric types,
// and nil is returned for string types.
func convertValue(ctx *sql.Context, val any, origType, convType sql.Type, typeLength int) (any, error) {
	if val == nil {
		return nil, nil
	}
	switch {
	case types.IsNullType(convType):
		return nil, nil
	case types.IsJSON(convType):
		val, _, err := types.TypeAwareConversion(ctx, val, origType, convType)
		if err != nil {
			return nil, err
		}
		return val, nil
	case types.IsTime(convType), types.IsTimespan(convType):
		val, _, err := types.TypeAwareConversion(ctx, val, origType, convType)
		if err != nil {
			if !sql.ErrTruncatedIncorrect.Is(err) {
				return nil, err
			}
			ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
		}
		return val, nil
	case types.IsBinaryType(convType):
		val, _, err := types.TypeAwareConversion(ctx, val, origType, convType)
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
	case types.IsText(convType):
		val, _, err := types.TypeAwareConversion(ctx, val, origType, convType)
		if err != nil {
			return nil, nil
		}
		return truncateConvertedValue(val, typeLength)
	default:
		res, inRange, err := types.TypeAwareConversion(ctx, val, origType, convType)
		if err != nil {
			if !sql.ErrTruncatedIncorrect.Is(err) {
				return convType.Zero(), nil
			}
			ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
		}
		if inRange != sql.InRange && types.IsUnsigned(convType) {
			ctx.Warn(1105, "Cast to unsigned converted negative integer to its positive complement")
		}
		return res, nil
	}
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

// createConvertedDecimalType creates the Decimal type for a conversion with the specified |precision| and |scale|.
// If neither are specified, the internal Decimal type is returned so that no precision is lost.
func createConvertedDecimalType(precision, scale int) (sql.Type, error) {
	if precision <= 0 && scale <= 0 {
		return types.InternalDecimalType, nil
	}
	return types.CreateColumnDecimalType(uint8(precision), uint8(scale))
}
