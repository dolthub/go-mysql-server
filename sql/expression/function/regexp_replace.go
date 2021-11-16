// Copyright 2021 Dolthub, Inc.
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
	"gopkg.in/src-d/go-errors.v1"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dolthub/go-mysql-server/sql"
)

// RegexpReplace implements the REGEXP_REPLACE function.
// https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-replace
type RegexpReplace struct {
	Text 		sql.Expression
	Pattern 	sql.Expression
	ReplaceStr 	sql.Expression
	Position 	sql.Expression
	Occurrence 	sql.Expression
	Flags 		sql.Expression

	cachedVal   atomic.Value
	re          *regexp.Regexp
	compileOnce sync.Once
	compileErr  error
}

var _ sql.FunctionExpression = (*RegexpReplace)(nil)

// NewRegexpReplace creates a new RegexpLike expression.
func NewRegexpReplace(args ...sql.Expression) (sql.Expression, error) {
	var r *RegexpReplace
	switch len(args) {
	case 6:
		r = &RegexpReplace{
			Text:    args[0],
			Pattern: args[1],
			ReplaceStr: args[2],
			Position: args[3],
			Occurrence: args[4],
			Flags:   args[5],
		}
	case 5:
		r = &RegexpReplace{
			Text:    args[0],
			Pattern: args[1],
			ReplaceStr: args[2],
			Position: args[3],
			Occurrence: args[4],
		}
	case 4:
		r = &RegexpReplace{
			Text:    args[0],
			Pattern: args[1],
			ReplaceStr: args[2],
			Position: args[3],
		}
	case 3:
		r = &RegexpReplace{
			Text:    args[0],
			Pattern: args[1],
			ReplaceStr: args[2],
		}
	default:
		return nil, sql.ErrInvalidArgumentNumber.New("regexp_replace", "3,4,5 or 6", len(args))
	}
	return r, nil
}

// FunctionName implements sql.FunctionExpression
func (r *RegexpReplace) FunctionName() string {
	return "regexp_replace"
}

// Type implements the sql.Expression interface.
func (r *RegexpReplace) Type() sql.Type { return sql.LongText }

// IsNullable implements the sql.Expression interface.
func (r *RegexpReplace) IsNullable() bool { return true }

// Children implements the sql.Expression interface.
func (r *RegexpReplace) Children() []sql.Expression {
	var result = []sql.Expression{r.Text, r.Pattern, r.ReplaceStr}
	if r.Position != nil {
		result = append(result, r.Position)
	}
	if r.Occurrence != nil {
		result = append(result, r.Occurrence)
	}
	if r.Flags != nil {
		result = append(result, r.Flags)
	}
	return result
}

// Resolved implements the sql.Expression interface.
func (r *RegexpReplace) Resolved() bool {
	return r.Text.Resolved() &&
		r.Pattern.Resolved() &&
		r.ReplaceStr.Resolved() &&
		(r.Position == nil || r.Position.Resolved()) &&
		(r.Occurrence == nil || r.Occurrence.Resolved()) &&
		(r.Flags == nil || r.Flags.Resolved())
}

// WithChildren implements the sql.Expression interface.
func (r *RegexpReplace) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	required := 3
	if r.Position != nil {
		required = 4
	}
	if r.Occurrence != nil {
		required = 5
	}
	if r.Flags != nil {
		required = 6
	}
	if len(children) != required {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), required)
	}
	return NewRegexpLike(children...)
}

func (r *RegexpReplace) String() string {
	var args []string
	for _, e := range r.Children() {
		args = append(args, e.String())
	}
	return fmt.Sprintf("regexp_replace(%s)", strings.Join(args, ", "))
}


func (r *RegexpReplace) compile(ctx *sql.Context) {
	r.compileOnce.Do(func() {
		r.re, r.compileErr = compileRegex(ctx, r.Pattern, r.Flags, r.FunctionName(), nil)
	})
}

// Eval implements the sql.Expression interface.
func (r *RegexpReplace) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("function.RegexpLike")
	defer span.Finish()

	cached := r.cachedVal.Load()
	if cached != nil {
		return cached, nil
	}

	// TODO: if null is passed in anywhere, return null, so need to check for argument lengths
	// Create regex
	r.compile(ctx)
	if r.compileErr != nil {
		return nil, r.compileErr
	}
	if r.re == nil {
		return nil, nil
	}

	// Evaluate text
	text, err := r.Text.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if text == nil {
		return nil, nil
	}
	text, err = sql.LongText.Convert(text)
	if err != nil {
		return nil, err
	}

	// Convert to string
	_text := text.(string)

	// Evaluate ReplaceStr
	replaceStr, err := r.ReplaceStr.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if replaceStr == nil {
		return nil, nil
	}
	replaceStr, err = sql.LongText.Convert(text)
	if err != nil {
		return nil, err
	}

	// Evaluate Position
	pos, err := r.Position.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Handle type for position
	_pos := 1
	switch pos.(type) {
	case nil:
		_pos = 1 // TODO: use constant
	case int:
		_pos = pos.(int)
	case int8:
		_pos = int(pos.(int8))
	case int16:
		_pos = int(pos.(int16))
	case int32:
		_pos = int(pos.(int32))
	case int64:
		_pos = int(pos.(int64))
	default:
		return nil, nil // TODO: incorrect type
	}

	// Non-positive position throws incorrect parameter
	if _pos <= 0 {
		return nil, ErrInvalidArgument.New(r.FunctionName())
	}

	// Handle out of bounds
	if _pos > len(_text) {
		return nil, errors.NewKind("Index out of bounds for regular expression search.").New()
	}

	// Evaluate Occurrence
	occ, err := r.Occurrence.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Handle types for occurrence
	_occ := 0
	switch pos.(type) {
	case nil:
		_occ = 0 // TODO: use constant
	case int:
		_occ = occ.(int)
	case int8:
		_occ = int(occ.(int8))
	case int16:
		_occ = int(occ.(int16))
	case int32:
		_occ = int(occ.(int32))
	case int64:
		_occ = int(occ.(int64))
	default:
		return nil, nil // TODO: incorrect type
	}


	// MySQL interprets negative occurrences as first for some reason
	if _occ < 0 {
		_occ = 1
	} else if _occ == 0 {
		// Replace everything
		return _text[:_pos-1] + r.re.ReplaceAllString(_text[_pos-1:], replaceStr.(string)), nil
	}

	// Extract all matches
	matches := r.re.FindAllString(_text[_pos-1:], -1)
	indexes := r.re.FindAllStringIndex(_text[_pos-1:], -1)

	// No matches, return original string
	if len(matches) == 0 {
		return _text, nil
	}

	// TODO: Might be a way to combine these two cases
	// If there aren't enough occurrences
	if _occ > len(matches) {
		return _text, nil
	}

	if _occ == 0 {
		// Replace all occurrences


	} else {
		// Replace only the nth occurrence
		matches[_occ - 1] = replaceStr.(string)
	}

	// Recombine matches
	res := prefix
	res += _text[:indexes[0][0]]
	res += matches[0]



	return matches, nil
}
