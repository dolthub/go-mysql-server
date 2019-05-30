package function

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/src-d/go-mysql-server/sql"
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
		return nil, sql.ErrInvalidArgumentNumber.New("SUBSTRING", "2 or 3", len(args))
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

// SubstringIndex returns the substring from string str before count occurrences of the delimiter delim.
// If count is positive, everything to the left of the final delimiter (counting from the left) is returned.
// If count is negative, everything to the right of the final delimiter (counting from the right) is returned.
// SUBSTRING_INDEX() performs a case-sensitive match when searching for delim.
type SubstringIndex struct {
	str   sql.Expression
	delim sql.Expression
	count sql.Expression
}

// NewSubstringIndex creates a new SubstringIndex UDF.
func NewSubstringIndex(str, delim, count sql.Expression) sql.Expression {
	return &SubstringIndex{str, delim, count}
}

// Children implements the Expression interface.
func (s *SubstringIndex) Children() []sql.Expression {
	return []sql.Expression{s.str, s.delim, s.count}
}

// Eval implements the Expression interface.
func (s *SubstringIndex) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	ex, err := s.str.Eval(ctx, row)
	if ex == nil || err != nil {
		return nil, err
	}
	ex, err = sql.Text.Convert(ex)
	if err != nil {
		return nil, err
	}
	str, ok := ex.(string)
	if !ok {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(ex).String())
	}

	ex, err = s.delim.Eval(ctx, row)
	if ex == nil || err != nil {
		return nil, err
	}
	ex, err = sql.Text.Convert(ex)
	if err != nil {
		return nil, err
	}
	delim, ok := ex.(string)
	if !ok {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(ex).String())
	}

	ex, err = s.count.Eval(ctx, row)
	if ex == nil || err != nil {
		return nil, err
	}
	ex, err = sql.Int64.Convert(ex)
	if err != nil {
		return nil, err
	}
	count, ok := ex.(int64)
	if !ok {
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(ex).String())
	}

	// Implementation taken from pingcap/tidb
	// https://github.com/pingcap/tidb/blob/37c128b64f3ad2f08d52bc767b6e3320ecf429d8/expression/builtin_string.go#L1229
	strs := strings.Split(str, delim)
	start, end := int64(0), int64(len(strs))
	if count > 0 {
		// If count is positive, everything to the left of the final delimiter (counting from the left) is returned.
		if count < end {
			end = count
		}
	} else {
		// If count is negative, everything to the right of the final delimiter (counting from the right) is returned.
		count = -count
		if count < 0 {
			// -count overflows max int64, returns an empty string.
			return "", nil
		}

		if count < end {
			start = end - count
		}
	}

	return strings.Join(strs[start:end], delim), nil
}

// IsNullable implements the Expression interface.
func (s *SubstringIndex) IsNullable() bool {
	return s.str.IsNullable() || s.delim.IsNullable() || s.count.IsNullable()
}

func (s *SubstringIndex) String() string {
	return fmt.Sprintf("SUBSTRING_INDEX(%s, %s, %d)", s.str, s.delim, s.count)
}

// Resolved implements the Expression interface.
func (s *SubstringIndex) Resolved() bool {
	return s.str.Resolved() && s.delim.Resolved() && s.count.Resolved()
}

// Type implements the Expression interface.
func (*SubstringIndex) Type() sql.Type { return sql.Text }

// TransformUp implements the Expression interface.
func (s *SubstringIndex) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	str, err := s.str.TransformUp(f)
	if err != nil {
		return nil, err
	}

	delim, err := s.delim.TransformUp(f)
	if err != nil {
		return nil, err
	}

	count, err := s.count.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewSubstringIndex(str, delim, count))
}
