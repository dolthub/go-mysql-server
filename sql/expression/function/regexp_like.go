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
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// RegexpLike implements the REGEXP_LIKE function.
// https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-like
type RegexpLike struct {
	Text    sql.Expression
	Pattern sql.Expression
	Flags   sql.Expression

	cachedVal   atomic.Value
	re          *regexp.Regexp
	compileOnce sync.Once
	compileErr  error
}

var _ sql.FunctionExpression = (*RegexpLike)(nil)

// NewRegexpLike creates a new RegexpLike expression.
func NewRegexpLike(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	var r *RegexpLike
	switch len(args) {
	case 3:
		r = &RegexpLike{
			Text:    args[0],
			Pattern: args[1],
			Flags:   args[2],
		}
	case 2:
		r = &RegexpLike{
			Text:    args[0],
			Pattern: args[1],
		}
	default:
		return nil, sql.ErrInvalidArgumentNumber.New("regexp_like", "2 or 3", len(args))
	}
	return r, nil
}

// FunctionName implements sql.FunctionExpression
func (r *RegexpLike) FunctionName() string {
	return "regexp_like"
}

// Type implements the sql.Expression interface.
func (r *RegexpLike) Type() sql.Type { return sql.Int8 }

// IsNullable implements the sql.Expression interface.
func (r *RegexpLike) IsNullable() bool { return true }

// Children implements the sql.Expression interface.
func (r *RegexpLike) Children() []sql.Expression {
	var result = []sql.Expression{r.Text, r.Pattern}
	if r.Flags != nil {
		result = append(result, r.Flags)
	}
	return result
}

// Resolved implements the sql.Expression interface.
func (r *RegexpLike) Resolved() bool {
	return r.Text.Resolved() && r.Pattern.Resolved() && (r.Flags == nil || r.Flags.Resolved())
}

// WithChildren implements the sql.Expression interface.
func (r *RegexpLike) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	required := 2
	if r.Flags != nil {
		required = 3
	}
	if len(children) != required {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), required)
	}
	return NewRegexpLike(ctx, children...)
}

func (r *RegexpLike) String() string {
	var args []string
	for _, e := range r.Children() {
		args = append(args, e.String())
	}
	return fmt.Sprintf("regexp_like(%s)", strings.Join(args, ", "))
}

func (r *RegexpLike) compile(ctx *sql.Context) {
	r.compileOnce.Do(func() {
		r.re, r.compileErr = compileRegex(ctx, r.Pattern, r.Flags, r.FunctionName(), nil)
	})
}

// Eval implements the sql.Expression interface.
func (r *RegexpLike) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("function.RegexpLike")
	defer span.Finish()

	cached := r.cachedVal.Load()
	if cached != nil {
		return cached, nil
	}

	r.compile(ctx)
	if r.compileErr != nil {
		return nil, r.compileErr
	}
	if r.re == nil {
		return nil, nil
	}

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

	var outVal int8
	if r.re.MatchString(text.(string)) {
		outVal = int8(1)
	} else {
		outVal = int8(0)
	}

	if canBeCached(r.Text) {
		r.cachedVal.Store(outVal)
	}
	return outVal, nil
}

func compileRegex(ctx *sql.Context, pattern, flags sql.Expression, funcName string, row sql.Row) (*regexp.Regexp, error) {
	patternVal, err := pattern.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if patternVal == nil {
		return nil, nil
	}
	patternVal, err = sql.LongText.Convert(patternVal)
	if err != nil {
		return nil, err
	}

	flagsStr := "(?i)"
	if flags != nil {
		f, err := flags.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if f == nil {
			return nil, nil
		}
		f, err = sql.LongText.Convert(f)
		if err != nil {
			return nil, err
		}

		flagsStr = f.(string)
		flagsStr, err = consolidateRegexpFlags(flagsStr, funcName)
		if err != nil {
			return nil, err
		}
		flagsStr = fmt.Sprintf("(?%s)", flagsStr)
		flagsStr = strings.Replace(flagsStr, "c", `\c`, -1)
	}
	return regexp.Compile(flagsStr + patternVal.(string))
}

// consolidateRegexpFlags consolidates regexp flags by removing duplicates, resolving order of conflicting flags, and
// verifying that all flags are valid.
func consolidateRegexpFlags(flags, funcName string) (string, error) {
	flagSet := make(map[string]struct{})
	// The flag 'u' is unsupported for now, as there isn't an equivalent flag in golang's regexp library
	for _, flag := range flags {
		switch flag {
		case 'c':
			if _, ok := flagSet["i"]; ok {
				delete(flagSet, "i")
			}
		case 'i':
			flagSet["i"] = struct{}{}
		case 'm':
			flagSet["m"] = struct{}{}
		case 'n':
			flagSet["s"] = struct{}{}
		default:
			return "", sql.ErrInvalidArgument.New(funcName)
		}
	}
	flags = ""
	for flag := range flagSet {
		flags += flag
	}
	return flags, nil
}

func canBeCached(e sql.Expression) bool {
	hasCols := false
	sql.Inspect(e, func(e sql.Expression) bool {
		switch e.(type) {
		case *expression.GetField, *expression.UserVar, *expression.SystemVar, *expression.ProcedureParam:
			hasCols = true
		}
		return true
	})
	return !hasCols
}
