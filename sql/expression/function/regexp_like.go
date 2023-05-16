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
	"strings"
	"sync"
	"sync/atomic"

	regex "github.com/dolthub/go-icu-regex"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// RegexpLike implements the REGEXP_LIKE function.
// https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-like
type RegexpLike struct {
	Text    sql.Expression
	Pattern sql.Expression
	Flags   sql.Expression

	cachedVal   atomic.Value
	re          regex.Regex
	compileOnce sync.Once
	compileErr  error
}

var _ sql.FunctionExpression = (*RegexpLike)(nil)
var _ sql.CollationCoercible = (*RegexpLike)(nil)
var _ sql.Closer = (*RegexpLike)(nil)

// NewRegexpLike creates a new RegexpLike expression.
func NewRegexpLike(args ...sql.Expression) (sql.Expression, error) {
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

// Description implements sql.FunctionExpression
func (r *RegexpLike) Description() string {
	return "returns whether string matches regular expression."
}

// Type implements the sql.Expression interface.
func (r *RegexpLike) Type() sql.Type { return types.Int8 }

// CollationCoercibility implements the interface sql.CollationCoercible.
func (r *RegexpLike) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	leftCollation, leftCoercibility := sql.GetCoercibility(ctx, r.Text)
	rightCollation, rightCoercibility := sql.GetCoercibility(ctx, r.Pattern)
	return sql.ResolveCoercibility(leftCollation, leftCoercibility, rightCollation, rightCoercibility)
}

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
func (r *RegexpLike) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	required := 2
	if r.Flags != nil {
		required = 3
	}
	if len(children) != required {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), required)
	}
	return NewRegexpLike(children...)
}

func (r *RegexpLike) String() string {
	var args []string
	for _, e := range r.Children() {
		args = append(args, e.String())
	}
	return fmt.Sprintf("%s(%s)", r.FunctionName(), strings.Join(args, ","))
}

func (r *RegexpLike) compile(ctx *sql.Context) {
	r.compileOnce.Do(func() {
		r.re, r.compileErr = compileRegex(ctx, r.Pattern, r.Text, r.Flags, r.FunctionName(), nil)
	})
}

// Eval implements the sql.Expression interface.
func (r *RegexpLike) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("function.RegexpLike")
	defer span.End()

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
	text, _, err = types.LongText.Convert(text)
	if err != nil {
		return nil, err
	}

	err = r.re.SetMatchString(ctx, text.(string))
	if err != nil {
		return nil, err
	}
	ok, err := r.re.Matches(ctx, 0, 0)
	if err != nil {
		return nil, err
	}
	var outVal int8
	if ok {
		outVal = int8(1)
	} else {
		outVal = int8(0)
	}

	if canBeCached(r.Text) {
		r.cachedVal.Store(outVal)
	}
	return outVal, nil
}

// Close implements the sql.Closer interface.
func (r *RegexpLike) Close(ctx *sql.Context) error {
	if r.re != nil {
		return r.re.Close()
	}
	return nil
}

func compileRegex(ctx *sql.Context, pattern, text, flags sql.Expression, funcName string, row sql.Row) (regex.Regex, error) {
	patternVal, err := pattern.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if patternVal == nil {
		return nil, nil
	}
	patternVal, _, err = types.LongText.Convert(patternVal)
	if err != nil {
		return nil, err
	}

	// Empty regex, throw illegal argument
	if len(patternVal.(string)) == 0 {
		return nil, errors.NewKind("Illegal argument to regular expression.").New()
	}

	// It appears that MySQL ONLY uses the collation to determine case-sensitivity and character set. We don't need to
	// worry about the character set since we convert all strings to UTF-8 for internal consistency. At the time of
	// writing this comment, all case-insensitive collations end with "_ci", so we can just check for that.
	leftCollation, leftCoercibility := sql.GetCoercibility(ctx, text)
	rightCollation, rightCoercibility := sql.GetCoercibility(ctx, pattern)
	resolvedCollation, _ := sql.ResolveCoercibility(leftCollation, leftCoercibility, rightCollation, rightCoercibility)
	flagsStr := ""
	if strings.HasSuffix(resolvedCollation.String(), "_ci") {
		flagsStr = "i"
	}

	if flags != nil {
		f, err := flags.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if f == nil {
			return nil, nil
		}
		f, _, err = types.LongText.Convert(f)
		if err != nil {
			return nil, err
		}

		flagsStr = f.(string)
		flagsStr, err = consolidateRegexpFlags(flagsStr, funcName)
		if err != nil {
			return nil, err
		}
	}
	regexFlags := regex.RegexFlags_None
	for _, flag := range flagsStr {
		// The 'c' flag is the default behavior, so we don't need to set anything in that case.
		// Any illegal flags will have been caught by consolidateRegexpFlags.
		switch flag {
		case 'i':
			regexFlags |= regex.RegexFlags_Case_Insensitive
		case 'm':
			regexFlags |= regex.RegexFlags_Multiline
		case 'n':
			regexFlags |= regex.RegexFlags_Dot_All
		case 'u':
			regexFlags |= regex.RegexFlags_Unix_Lines
		}
	}

	re := regex.CreateRegex()
	if err = re.SetRegexString(ctx, patternVal.(string), regexFlags); err != nil {
		_ = re.Close()
		return nil, err
	}
	return re, nil
}

// consolidateRegexpFlags consolidates regexp flags by removing duplicates, resolving order of conflicting flags, and
// verifying that all flags are valid.
func consolidateRegexpFlags(flags, funcName string) (string, error) {
	flagSet := make(map[string]struct{})
	for _, flag := range flags {
		switch flag {
		case 'c':
			delete(flagSet, "i")
		case 'i':
			flagSet["i"] = struct{}{}
		case 'm':
			flagSet["m"] = struct{}{}
		case 'n':
			flagSet["n"] = struct{}{}
		case 'u':
			flagSet["u"] = struct{}{}
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
