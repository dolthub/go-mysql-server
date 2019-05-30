package function

import (
	"encoding/base64"
	"fmt"
	"reflect"
	"strings"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

// ToBase64 is a function to encode a string to the Base64 format
// using the same dialect that MySQL's TO_BASE64 uses
type ToBase64 struct {
	expression.UnaryExpression
}

// NewToBase64 creates a new ToBase64 expression.
func NewToBase64(e sql.Expression) sql.Expression {
	return &ToBase64{expression.UnaryExpression{Child: e}}
}

// Eval implements the Expression interface.
func (t *ToBase64) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	str, err := t.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if str == nil {
		return nil, nil
	}

	str, err = sql.Text.Convert(str)
	if err != nil {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(str))
	}

	encoded := base64.StdEncoding.EncodeToString([]byte(str.(string)))

	lenEncoded := len(encoded)
	if lenEncoded <= 76 {
		return encoded, nil
	}

	// Split into max 76 chars lines
	var out strings.Builder
	start := 0
	end := 76
	for {
		out.WriteString(encoded[start:end] + "\n")
		start += 76
		end += 76
		if end >= lenEncoded {
			out.WriteString(encoded[start:lenEncoded])
			break
		}
	}

	return out.String(), nil
}

// String implements the Stringer interface.
func (t *ToBase64) String() string {
	return fmt.Sprintf("TO_BASE64(%s)", t.Child)
}

// IsNullable implements the Expression interface.
func (t *ToBase64) IsNullable() bool {
	return t.Child.IsNullable()
}

// TransformUp implements the Expression interface.
func (t *ToBase64) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := t.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewToBase64(child))
}

// Type implements the Expression interface.
func (t *ToBase64) Type() sql.Type {
	return sql.Text
}


// FromBase64 is a function to decode a Base64-formatted string
// using the same dialect that MySQL's FROM_BASE64 uses
type FromBase64 struct {
	expression.UnaryExpression
}

// NewFromBase64 creates a new FromBase64 expression.
func NewFromBase64(e sql.Expression) sql.Expression {
	return &FromBase64{expression.UnaryExpression{Child: e}}
}

// Eval implements the Expression interface.
func (t *FromBase64) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	str, err := t.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if str == nil {
		return nil, nil
	}

	str, err = sql.Text.Convert(str)
	if err != nil {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(str))
	}

	decoded, err := base64.StdEncoding.DecodeString(str.(string))
	if err != nil {
		return nil, err
	}

	return string(decoded), nil
}

// String implements the Stringer interface.
func (t *FromBase64) String() string {
	return fmt.Sprintf("FROM_BASE64(%s)", t.Child)
}

// IsNullable implements the Expression interface.
func (t *FromBase64) IsNullable() bool {
	return t.Child.IsNullable()
}

// TransformUp implements the Expression interface.
func (t *FromBase64) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := t.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewFromBase64(child))
}

// Type implements the Expression interface.
func (t *FromBase64) Type() sql.Type {
	return sql.Text
}
