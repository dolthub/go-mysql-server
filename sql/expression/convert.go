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
	"strconv"
	"strings"
	"time"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
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
	// ConvertToUnsigned is a conversion to unsigned.
	ConvertToUnsigned = "unsigned"
)

// Convert represent a CAST(x AS T) or CONVERT(x, T) operation that casts x expression to type T.
type Convert struct {
	UnaryExpression
	// Type to cast
	castToType string
}

// NewConvert creates a new Convert expression.
func NewConvert(expr sql.Expression, castToType string) *Convert {
	return &Convert{
		UnaryExpression: UnaryExpression{Child: expr},
		castToType:      strings.ToLower(castToType),
	}
}

// IsNullable implements the Expression interface.
func (c *Convert) IsNullable() bool {
	switch c.castToType {
	case ConvertToDate, ConvertToDatetime:
		return true
	default:
		return c.Child.IsNullable()
	}
}

// Type implements the Expression interface.
func (c *Convert) Type() sql.Type {
	switch c.castToType {
	case ConvertToBinary:
		return sql.LongBlob
	case ConvertToChar, ConvertToNChar:
		return sql.LongText
	case ConvertToDate:
		return sql.Date
	case ConvertToDatetime:
		return sql.Datetime
	case ConvertToDecimal:
		//TODO: these values are completely arbitrary, we need to get the given precision/scale and store it
		return sql.MustCreateDecimalType(65, 10)
	case ConvertToDouble, ConvertToReal:
		return sql.Float64
	case ConvertToJSON:
		return sql.JSON
	case ConvertToSigned:
		return sql.Int64
	case ConvertToTime:
		return sql.Time
	case ConvertToUnsigned:
		return sql.Uint64
	default:
		return sql.Null
	}
}

// Name implements the Expression interface.
func (c *Convert) String() string {
	return fmt.Sprintf("convert(%v, %v)", c.Child, c.castToType)
}

// WithChildren implements the Expression interface.
func (c *Convert) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return NewConvert(children[0], c.castToType), nil
}

// Eval implements the Expression interface.
func (c *Convert) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := c.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	casted, err := convertValue(val, c.castToType)
	if err != nil {
		return nil, ErrConvertExpression.Wrap(err, c.String(), c.castToType)
	}

	return casted, nil
}

// convertValue only returns an error if converting to JSON, and returns the zero value for float types.
// Nil is returned in all other cases.
func convertValue(val interface{}, castTo string) (interface{}, error) {
	switch strings.ToLower(castTo) {
	case ConvertToBinary:
		b, err := sql.LongBlob.Convert(val)
		if err != nil {
			return nil, nil
		}
		return b, nil
	case ConvertToChar, ConvertToNChar:
		s, err := sql.LongText.Convert(val)
		if err != nil {
			return nil, nil
		}
		return s, nil
	case ConvertToDate:
		_, isTime := val.(time.Time)
		_, isString := val.(string)
		if !(isTime || isString) {
			return nil, nil
		}
		d, err := sql.Date.Convert(val)
		if err != nil {
			return nil, nil
		}
		return d, nil
	case ConvertToDatetime:
		_, isTime := val.(time.Time)
		_, isString := val.(string)
		if !(isTime || isString) {
			return nil, nil
		}
		d, err := sql.Datetime.Convert(val)
		if err != nil {
			return nil, nil
		}
		return d, nil
	case ConvertToDecimal:
		//TODO: these values are completely arbitrary, we need to get the given precision/scale and store it
		typ := sql.MustCreateDecimalType(65, 10)
		d, err := typ.Convert(val)
		if err != nil {
			return typ.Zero(), nil
		}
		return d, nil
	case ConvertToDouble, ConvertToReal:
		d, err := sql.Float64.Convert(val)
		if err != nil {
			return sql.Float64.Zero(), nil
		}
		return d, nil
	case ConvertToJSON:
		js, err := sql.JSON.Convert(val)
		if err != nil {
			return nil, err
		}
		return js, nil
	case ConvertToSigned:
		num, err := sql.Int64.Convert(val)
		if err != nil {
			return sql.Int64.Zero(), nil
		}

		return num, nil
	case ConvertToTime:
		t, err := sql.Time.Convert(val)
		if err != nil {
			return nil, nil
		}
		return t, nil
	case ConvertToUnsigned:
		num, err := sql.Uint64.Convert(val)
		if err != nil {
			num = handleUnsignedErrors(err, val)
		}

		return num, nil
	default:
		return nil, nil
	}
}

func handleUnsignedErrors(err error, val interface{}) uint64 {
	if err.Error() == "unable to cast negative value" {
		return castSignedToUnsigned(val)
	}

	if strings.Contains(err.Error(), "strconv.ParseUint") {
		signedNum, err := strconv.ParseInt(val.(string), 0, 64)
		if err != nil {
			return uint64(0)
		}

		return castSignedToUnsigned(signedNum)
	}

	return uint64(0)
}

func castSignedToUnsigned(val interface{}) uint64 {
	var unsigned uint64
	switch num := val.(type) {
	case int:
		unsigned = uint64(num)
	case int8:
		unsigned = uint64(num)
	case int16:
		unsigned = uint64(num)
	case int32:
		unsigned = uint64(num)
	case int64:
		unsigned = uint64(num)
	}

	return unsigned
}
