package expression

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cast"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
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
	// ConvertToDatetime is a conversion to datetune.
	ConvertToDatetime = "datetime"
	// ConvertToDecimal is a conversion to decimal.
	ConvertToDecimal = "decimal"
	// ConvertToJSON is a conversion to json.
	ConvertToJSON = "json"
	// ConvertToSigned is a conversion to signed.
	ConvertToSigned = "signed"
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
		castToType:      castToType,
	}
}

// Type implements the Expression interface.
func (c *Convert) Type() sql.Type {
	switch c.castToType {
	case ConvertToBinary:
		return sql.Blob
	case ConvertToChar, ConvertToNChar:
		return sql.Text
	case ConvertToDate, ConvertToDatetime:
		return sql.Date
	case ConvertToDecimal:
		return sql.Float64
	case ConvertToJSON:
		return sql.JSON
	case ConvertToSigned:
		return sql.Int64
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

// TransformUp implements the Expression interface.
func (c *Convert) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := c.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewConvert(child, c.castToType))
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

func convertValue(val interface{}, castTo string) (interface{}, error) {
	switch castTo {
	case ConvertToBinary:
		s, err := sql.Text.Convert(val)
		if err != nil {
			return nil, err
		}

		b, err := sql.Blob.Convert(s)
		if err != nil {
			return nil, err
		}

		return b, nil
	case ConvertToChar, ConvertToNChar:
		s, err := sql.Text.Convert(val)
		if err != nil {
			return nil, err
		}

		return s, nil
	case ConvertToDate, ConvertToDatetime:
		_, isTime := val.(time.Time)
		_, isString := val.(string)
		if !(isTime || isString) {
			return nil, nil
		}

		d, err := sql.Timestamp.Convert(val)
		if err != nil {
			d, err = sql.Date.Convert(val)
			if err != nil {
				return nil, nil
			}
		}

		return d, nil
	case ConvertToDecimal:
		d, err := cast.ToFloat64E(val)
		if err != nil {
			return float64(0), nil
		}

		return d, nil
	case ConvertToJSON:
		s, err := cast.ToStringE(val)
		if err != nil {
			return nil, err
		}

		var jsn interface{}
		err = json.Unmarshal([]byte(s), &jsn)
		if err != nil {
			return nil, err
		}

		return []byte(s), nil
	case ConvertToSigned:
		num, err := sql.Int64.Convert(val)
		if err != nil {
			return int64(0), nil
		}

		return num, nil
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
