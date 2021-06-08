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

package function

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
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

var _ sql.FunctionExpression = (*Substring)(nil)

// NewSubstring creates a new substring UDF.
func NewSubstring(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
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

// FunctionName implements sql.FunctionExpression
func (s *Substring) FunctionName() string {
	return "substring"
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
func (*Substring) Type() sql.Type { return sql.LongText }

// WithChildren implements the Expression interface.
func (*Substring) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewSubstring(ctx, children...)
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

var _ sql.FunctionExpression = (*SubstringIndex)(nil)

// NewSubstringIndex creates a new SubstringIndex UDF.
func NewSubstringIndex(ctx *sql.Context, str, delim, count sql.Expression) sql.Expression {
	return &SubstringIndex{str, delim, count}
}

// FunctionName implements sql.FunctionExpression
func (s *SubstringIndex) FunctionName() string {
	return "substring_index"
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
	ex, err = sql.LongText.Convert(ex)
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
	ex, err = sql.LongText.Convert(ex)
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
func (*SubstringIndex) Type() sql.Type { return sql.LongText }

// WithChildren implements the Expression interface.
func (s *SubstringIndex) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 3)
	}
	return NewSubstringIndex(ctx, children[0], children[1], children[2]), nil
}

// Left is a function that returns the first N characters of a string expression.
type Left struct {
	str sql.Expression
	len sql.Expression
}

var _ sql.FunctionExpression = Left{}

// NewLeft creates a new LEFT function.
func NewLeft(ctx *sql.Context, str, len sql.Expression) sql.Expression {
	return Left{str, len}
}

// FunctionName implements sql.FunctionExpression
func (l Left) FunctionName() string {
	return "left"
}

// Children implements the Expression interface.
func (l Left) Children() []sql.Expression {
	return []sql.Expression{l.str, l.len}
}

// Eval implements the Expression interface.
func (l Left) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	str, err := l.str.Eval(ctx, row)
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

	var length int64
	runeCount := int64(len(text))
	len, err := l.len.Eval(ctx, row)
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

	if length > runeCount {
		length = runeCount
	}
	if length <= 0 {
		return "", nil
	}

	return string(text[:length]), nil
}

// IsNullable implements the Expression interface.
func (l Left) IsNullable() bool {
	return l.str.IsNullable() || l.len.IsNullable()
}

func (l Left) String() string {
	return fmt.Sprintf("LEFT(%s, %s)", l.str, l.len)
}

// Resolved implements the Expression interface.
func (l Left) Resolved() bool {
	return l.str.Resolved() && l.len.Resolved()
}

// Type implements the Expression interface.
func (Left) Type() sql.Type { return sql.LongText }

// WithChildren implements the Expression interface.
func (l Left) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 2)
	}
	return NewLeft(ctx, children[0], children[1]), nil
}

type Instr struct {
	str    sql.Expression
	substr sql.Expression
}

var _ sql.FunctionExpression = Instr{}

// NewInstr creates a new instr UDF.
func NewInstr(ctx *sql.Context, str, substr sql.Expression) sql.Expression {
	return Instr{str, substr}
}

// FunctionName implements sql.FunctionExpression
func (i Instr) FunctionName() string {
	return "instr"
}

// Children implements the Expression interface.
func (i Instr) Children() []sql.Expression {
	return []sql.Expression{i.str, i.substr}
}

// Eval implements the Expression interface.
func (i Instr) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	str, err := i.str.Eval(ctx, row)
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

	substr, err := i.substr.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	var subtext []rune
	switch substr := substr.(type) {
	case string:
		subtext = []rune(substr)
	case []byte:
		subtext = []rune(string(subtext))
	case nil:
		return nil, nil
	default:
		return nil, sql.ErrInvalidType.New(reflect.TypeOf(str).String())
	}

	return findSubsequence(text, subtext) + 1, nil
}

func findSubsequence(text []rune, subtext []rune) int64 {
	for i := 0; i <= len(text)-len(subtext); i++ {
		var j int
		for j = 0; j < len(subtext); j++ {
			if text[i+j] != subtext[j] {
				break
			}
		}
		if j == len(subtext) {
			return int64(i)
		}
	}
	return -1
}

// IsNullable implements the Expression interface.
func (i Instr) IsNullable() bool {
	return i.str.IsNullable() || i.substr.IsNullable()
}

func (i Instr) String() string {
	return fmt.Sprintf("INSTR(%s, %s)", i.str, i.substr)
}

// Resolved implements the Expression interface.
func (i Instr) Resolved() bool {
	return i.str.Resolved() && i.substr.Resolved()
}

// Type implements the Expression interface.
func (Instr) Type() sql.Type { return sql.Int64 }

// WithChildren implements the Expression interface.
func (i Instr) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 2)
	}
	return NewInstr(ctx, children[0], children[1]), nil
}
