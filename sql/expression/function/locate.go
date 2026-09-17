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
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Locate returns the 1-based character position of the first occurrence of a
// substring in a string, or 0 if not found. Positions are character-based.
type Locate struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*Locate)(nil)
var _ sql.CollationCoercible = (*Locate)(nil)

// NewLocate returns a new Locate function.
func NewLocate(ctx *sql.Context, exprs ...sql.Expression) (sql.Expression, error) {
	if len(exprs) < 2 || len(exprs) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("LOCATE", "2 or 3", len(exprs))
	}

	return &Locate{expression.NaryExpression{ChildExpressions: exprs}}, nil
}

// FunctionName implements sql.FunctionExpression
func (l *Locate) FunctionName() string {
	return "locate"
}

// Description implements sql.FunctionExpression
func (l *Locate) Description() string {
	return "returns the position of the first occurrence of a substring in a string."
}

// WithChildren implements the Expression interface.
func (l *Locate) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) < 2 || len(children) > 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 2)
	}

	return &Locate{expression.NaryExpression{ChildExpressions: children}}, nil
}

// Type implements the sql.Expression interface.
func (l *Locate) Type(ctx *sql.Context) sql.Type { return types.Int32 }

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Locate) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (l *Locate) String() string {
	switch len(l.ChildExpressions) {
	case 2:
		return fmt.Sprintf("%s(%s,%s)", l.FunctionName(), l.ChildExpressions[0], l.ChildExpressions[1])
	case 3:
		return fmt.Sprintf("%s(%s,%s,%s)", l.FunctionName(), l.ChildExpressions[0], l.ChildExpressions[1], l.ChildExpressions[2])
	}
	return ""
}

func (l *Locate) DebugString(ctx *sql.Context) string {
	switch len(l.ChildExpressions) {
	case 2:
		return fmt.Sprintf("%s(%s,%s)", l.FunctionName(), sql.DebugString(ctx, l.ChildExpressions[0]), sql.DebugString(ctx, l.ChildExpressions[1]))
	case 3:
		return fmt.Sprintf("%s(%s,%s,%s)", l.FunctionName(), sql.DebugString(ctx, l.ChildExpressions[0]), sql.DebugString(ctx, l.ChildExpressions[1]), sql.DebugString(ctx, l.ChildExpressions[2]))
	}
	return ""
}

// isBinaryStringType is true for BINARY, VARBINARY, and BLOB only.
// types.IsBinaryType also matches JSON/GEOMETRY/VECTOR, which LOCATE must fold.
func isBinaryStringType(t sql.Type) bool {
	if t == nil {
		return false
	}
	switch t.Type() {
	case sqltypes.Binary, sqltypes.VarBinary, sqltypes.Blob:
		return true
	default:
		return false
	}
}

func isAllASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// Eval implements the sql.Expression interface.
func (l *Locate) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if len(l.ChildExpressions) < 2 || len(l.ChildExpressions) > 3 {
		return nil, nil
	}

	substrVal, err := l.ChildExpressions[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if substrVal == nil {
		return nil, nil
	}

	substrVal, _, err = types.LongText.Convert(ctx, substrVal)
	if err != nil {
		return nil, err
	}

	// Handle Dolt's TextStorage wrapper that doesn't convert to plain string
	substrVal, err = sql.UnwrapAny(ctx, substrVal)
	if err != nil {
		return nil, err
	}

	substr, ok := substrVal.(string)
	if !ok {
		return nil, sql.ErrInvalidArgumentDetails.New("locate", "substring must be a string")
	}

	strVal, err := l.ChildExpressions[1].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if strVal == nil {
		return nil, nil
	}

	strVal, _, err = types.LongText.Convert(ctx, strVal)
	if err != nil {
		return nil, err
	}

	// Handle Dolt's TextStorage wrapper that doesn't convert to plain string
	strVal, err = sql.UnwrapAny(ctx, strVal)
	if err != nil {
		return nil, err
	}

	str, ok := strVal.(string)
	if !ok {
		return nil, sql.ErrInvalidArgumentDetails.New("locate", "string must be a string")
	}

	position := 1

	if len(l.ChildExpressions) == 3 {
		posVal, err := l.ChildExpressions[2].Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		if posVal != nil {
			posInt, _, err := types.Int32.Convert(ctx, posVal)
			if err != nil {
				return nil, sql.ErrInvalidArgumentDetails.New("locate", "start must be an integer")
			}
			position = int(posInt.(int32))
		}
	}

	caseSensitive := isBinaryStringType(l.ChildExpressions[0].Type(ctx)) ||
		isBinaryStringType(l.ChildExpressions[1].Type(ctx))

	// Fast path when every byte is a single character.
	if isAllASCII(str) && isAllASCII(substr) {
		switch {
		case position <= 0 || position > len(str)+1:
			return int32(0), nil
		case len(substr) == 0:
			return int32(position), nil
		}

		haystack := str[position-1:]
		needle := substr
		if !caseSensitive {
			haystack = strings.ToLower(haystack)
			needle = strings.ToLower(needle)
		}
		res := strings.Index(haystack, needle)
		if res == -1 {
			return int32(0), nil
		}
		return int32(res + position), nil
	}

	strRunes := []rune(str)
	substrRunes := []rune(substr)

	// Out of range, or empty needle at a valid start (including len+1).
	switch {
	case position <= 0 || position > len(strRunes)+1:
		return int32(0), nil
	case len(substrRunes) == 0:
		return int32(position), nil
	}

	haystack := strRunes[position-1:]
	needle := substrRunes
	if !caseSensitive {
		haystack = runesToLower(haystack)
		needle = runesToLower(needle)
	}

	res := findSubsequence(haystack, needle)
	if res == -1 {
		return int32(0), nil
	}
	return int32(res + int64(position)), nil
}

func runesToLower(rs []rune) []rune {
	out := make([]rune, len(rs))
	for i, r := range rs {
		out[i] = unicode.ToLower(r)
	}
	return out
}
