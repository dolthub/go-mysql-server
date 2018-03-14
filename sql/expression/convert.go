package expression

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cast"
	errors "gopkg.in/src-d/go-errors.v0"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// ErrConvertExpression is returned when a conversion is not possible.
var ErrConvertExpression = errors.NewKind("expression '%v': couldn't convert to %v")

const (
	// ConvertTo represents those conversion types Convert accepts.
	ConvertToBinary   = "binary"
	ConvertToChar     = "char"
	ConvertToNChar    = "nchar"
	ConvertToDate     = "date"
	ConvertToDatetime = "datetime"
	ConvertToDecimal  = "decimal"
	ConvertToJSON     = "json"
	ConvertToSigned   = "signed"
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
func (c *Convert) Name() string {
	return fmt.Sprintf("convert(%v, %v)", c.Child.Name(), c.castToType)
}

// TransformUp implements the Expression interface.
func (c *Convert) TransformUp(f func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
	child, err := c.UnaryExpression.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewConvert(child, c.castToType))
}

// Eval implements the Expression interface.
func (c *Convert) Eval(session sql.Session, row sql.Row) (interface{}, error) {
	val, err := c.UnaryExpression.Child.Eval(session, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	switch c.castToType {
	case ConvertToBinary:
		s, err := cast.ToStringE(val)
		if err != nil {
			return nil, ErrConvertExpression.Wrap(err, c.Name(), c.castToType)
		}

		return []byte(s), nil
	case ConvertToChar, ConvertToNChar:
		s, err := cast.ToStringE(val)
		if err != nil {
			return nil, ErrConvertExpression.Wrap(err, c.Name(), c.castToType)
		}

		return s, err
	case ConvertToDate, ConvertToDatetime:
		switch date := val.(type) {
		case string:
			t, err := time.Parse(sql.TimestampLayout, date)
			if err != nil {
				t, err = time.Parse(sql.DateLayout, date)
				if err != nil {
					return nil, nil
				}
			}

			return t.UTC(), nil
		default:
			return nil, nil
		}
	case ConvertToDecimal:
		d, err := cast.ToFloat64E(val)
		if err != nil {
			return float64(0), nil
		}

		return d, nil
	case ConvertToJSON:
		s, err := cast.ToStringE(val)
		if err != nil {
			return nil, ErrConvertExpression.Wrap(err, c.Name(), c.castToType)
		}

		var jsn interface{}
		err = json.Unmarshal([]byte(s), &jsn)
		if err != nil {
			return nil, ErrConvertExpression.Wrap(err, c.Name(), c.castToType)
		}

		return []byte(s), nil
	case ConvertToSigned:
		num, err := cast.ToInt64E(val)
		if err != nil {
			return int64(0), nil
		}
		return num, nil
	case ConvertToUnsigned:
		num, err := cast.ToUint64E(val)
		if err != nil {
			num = handleUnsignedErrors(err, val)
		}

		return num, nil
	default:
		return nil, ErrConvertExpression.New(c.Name(), c.castToType)
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
