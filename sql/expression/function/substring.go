package function

import (
	"fmt"
	"reflect"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Substring is a function to return a part of a string.
// This function behaves as the homonym MySQL function.
// Since Go strings are UTF8, this function does not return a direct sub
// string str[start:start+length], instead returns the substring of rune
// s. That is, "á"[0:1] does not return a partial unicode glyph, but "á"
// itself.
type Substring struct {
	str   sql.Expression
	start sql.Expression
	len   sql.Expression
}

// NewSubstring creates a new substring UDF.
func NewSubstring(args ...sql.Expression) (sql.Expression, error) {
	var str, start, ln sql.Expression
	switch len(args) {
	case 2:
		str = args[0]
		start = args[1]
		ln = nil
	case 3:
		str = args[0]
		start = args[1]
		ln = args[2]
	default:
		return nil, sql.ErrInvalidArgumentNumber.New("2 or 3", len(args))
	}
	return &Substring{str, start, ln}, nil
}

// Children implements the Expression interface.
func (s *Substring) Children() []sql.Expression {
	if s.len == nil {
		return []sql.Expression{s.str, s.start}
	}
	return []sql.Expression{s.str, s.start, s.len}
}

// Eval implements the Expression interface.
func (s *Substring) Eval(
	ctx *sql.Context,
	row sql.Row,
) (interface{}, error) {
	str, err := s.str.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	var text []rune
	switch str := str.(type) {
	case string:
		text = []rune(str)
	case []byte:
		text = []rune(string(str))
	case nil:
		return nil, nil
	default:
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(str).String())
	}

	start, err := s.start.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if start == nil {
		return nil, nil
	}

	start, err = sql.Int64.Convert(start)
	if err != nil {
		return nil, err
	}

	var length int64
	runeCount := int64(len(text))
	if s.len != nil {
		len, err := s.len.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		if len == nil {
			return nil, nil
		}

		len, err = sql.Int64.Convert(len)
		if err != nil {
			return nil, err
		}

		length = len.(int64)
	} else {
		length = runeCount
	}

	var startIdx int64
	if start := start.(int64); start < 0 {
		startIdx = runeCount + start
	} else {
		startIdx = start - 1
	}

	if startIdx < 0 || startIdx >= runeCount || length <= 0 {
		return "", nil
	}

	if startIdx+length > runeCount {
		length = int64(runeCount) - startIdx
	}

	return string(text[startIdx : startIdx+length]), nil
}

// IsNullable implements the Expression interface.
func (s *Substring) IsNullable() bool {
	return s.str.IsNullable() || s.start.IsNullable() || (s.len != nil && s.len.IsNullable())
}

func (s *Substring) String() string {
	if s.len == nil {
		return fmt.Sprintf("SUBSTRING(%s, %s)", s.str, s.start)
	}
	return fmt.Sprintf("SUBSTRING(%s, %s, %s)", s.str, s.start, s.len)
}

// Resolved implements the Expression interface.
func (s *Substring) Resolved() bool {
	return s.start.Resolved() && s.str.Resolved() && (s.len == nil || s.len.Resolved())
}

// Type implements the Expression interface.
func (*Substring) Type() sql.Type { return sql.Text }

// TransformUp implements the Expression interface.
func (s *Substring) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	str, err := s.str.TransformUp(f)
	if err != nil {
		return nil, err
	}

	start, err := s.start.TransformUp(f)
	if err != nil {
		return nil, err
	}

	// It is safe to omit the errors of NewSubstring here because to be able to call
	// this method, you need a valid instance of Substring, so the arity must be correct
	// and that's the only error NewSubstring can return.
	var sub sql.Expression
	if s.len != nil {
		len, err := s.len.TransformUp(f)
		if err != nil {
			return nil, err
		}
		sub, _ = NewSubstring(str, start, len)
	} else {
		sub, _ = NewSubstring(str, start)
	}
	return f(sub)
}
