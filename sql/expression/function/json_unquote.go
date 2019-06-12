package function

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"reflect"
	"unicode/utf8"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

// JSONUnquote unquotes JSON value and returns the result as a utf8mb4 string.
// Returns NULL if the argument is NULL.
// An error occurs if the value starts and ends with double quotes but is not a valid JSON string literal.
type JSONUnquote struct {
	expression.UnaryExpression
}

// NewJSONUnquote creates a new JSONUnquote UDF.
func NewJSONUnquote(json sql.Expression) sql.Expression {
	return &JSONUnquote{expression.UnaryExpression{Child: json}}
}

func (js *JSONUnquote) String() string {
	return fmt.Sprintf("JSON_UNQUOTE(%s)", js.Child)
}

// Type implements the Expression interface.
func (*JSONUnquote) Type() sql.Type {
	return sql.Text
}

// TransformUp implements the Expression interface.
func (js *JSONUnquote) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	json, err := js.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewJSONUnquote(json))
}

// Eval implements the Expression interface.
func (js *JSONUnquote) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	json, err := js.Child.Eval(ctx, row)
	if json == nil || err != nil {
		return json, err
	}

	ex, err := sql.Text.Convert(json)
	if err != nil {
		return nil, err
	}
	str, ok := ex.(string)
	if !ok {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(ex).String())
	}

	return unquote(str)
}

// The implementation is taken from TiDB
// https://github.com/pingcap/tidb/blob/a594287e9f402037b06930026906547000006bb6/types/json/binary_functions.go#L89
func unquote(s string) (string, error) {
	ret := new(bytes.Buffer)
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' {
			i++
			if i == len(s) {
				return "", fmt.Errorf("Missing a closing quotation mark in string")
			}
			switch s[i] {
			case '"':
				ret.WriteByte('"')
			case 'b':
				ret.WriteByte('\b')
			case 'f':
				ret.WriteByte('\f')
			case 'n':
				ret.WriteByte('\n')
			case 'r':
				ret.WriteByte('\r')
			case 't':
				ret.WriteByte('\t')
			case '\\':
				ret.WriteByte('\\')
			case 'u':
				if i+4 > len(s) {
					return "", fmt.Errorf("Invalid unicode: %s", s[i+1:])
				}
				char, size, err := decodeEscapedUnicode([]byte(s[i+1 : i+5]))
				if err != nil {
					return "", err
				}
				ret.Write(char[0:size])
				i += 4
			default:
				// For all other escape sequences, backslash is ignored.
				ret.WriteByte(s[i])
			}
		} else {
			ret.WriteByte(s[i])
		}
	}

	str := ret.String()
	strlen := len(str)
	// Remove prefix and suffix '"'.
	if strlen > 1 {
		head, tail := str[0], str[strlen-1]
		if head == '"' && tail == '"' {
			return str[1 : strlen-1], nil
		}
	}
	return str, nil
}

// decodeEscapedUnicode decodes unicode into utf8 bytes specified in RFC 3629.
// According RFC 3629, the max length of utf8 characters is 4 bytes.
// And MySQL use 4 bytes to represent the unicode which must be in [0, 65536).
// The implementation is taken from TiDB:
// https://github.com/pingcap/tidb/blob/a594287e9f402037b06930026906547000006bb6/types/json/binary_functions.go#L136
func decodeEscapedUnicode(s []byte) (char [4]byte, size int, err error) {
	size, err = hex.Decode(char[0:2], s)
	if err != nil || size != 2 {
		// The unicode must can be represented in 2 bytes.
		return char, 0, err
	}
	var unicode uint16
	err = binary.Read(bytes.NewReader(char[0:2]), binary.BigEndian, &unicode)
	if err != nil {
		return char, 0, err
	}
	size = utf8.RuneLen(rune(unicode))
	utf8.EncodeRune(char[0:size], rune(unicode))
	return
}
