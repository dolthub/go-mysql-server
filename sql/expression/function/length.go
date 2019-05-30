package function

import (
	"fmt"
	"unicode/utf8"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

// Length returns the length of a string or binary content, either in bytes
// or characters.
type Length struct {
	expression.UnaryExpression
	CountType CountType
}

// CountType is the kind of length count.
type CountType bool

const (
	// NumBytes counts the number of bytes in a string or binary content.
	NumBytes = CountType(false)
	// NumChars counts the number of characters in a string or binary content.
	NumChars = CountType(true)
)

// NewLength returns a new LENGTH function.
func NewLength(e sql.Expression) sql.Expression {
	return &Length{expression.UnaryExpression{Child: e}, NumBytes}
}

// NewCharLength returns a new CHAR_LENGTH function.
func NewCharLength(e sql.Expression) sql.Expression {
	return &Length{expression.UnaryExpression{Child: e}, NumChars}
}

// TransformUp implements the sql.Expression interface.
func (l *Length) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := l.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	nl := *l
	nl.Child = child
	return &nl, nil
}

// Type implements the sql.Expression interface.
func (l *Length) Type() sql.Type { return sql.Int32 }

func (l *Length) String() string {
	if l.CountType == NumBytes {
		return fmt.Sprintf("LENGTH(%s)", l.Child)
	}
	return fmt.Sprintf("CHAR_LENGTH(%s)", l.Child)
}

// Eval implements the sql.Expression interface.
func (l *Length) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := l.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	var content string
	switch l.Child.Type() {
	case sql.Blob:
		val, err = sql.Blob.Convert(val)
		if err != nil {
			return nil, err
		}

		content = string(val.([]byte))
	default:
		val, err = sql.Text.Convert(val)
		if err != nil {
			return nil, err
		}

		content = string(val.(string))
	}

	if l.CountType == NumBytes {
		return int32(len(content)), nil
	}

	return int32(utf8.RuneCountInString(content)), nil
}
