package function

import (
	"fmt"
	"regexp"
	"strings"

	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

// RegexpMatches returns the matches of a regular expression.
type RegexpMatches struct {
	Text    sql.Expression
	Pattern sql.Expression
	Flags   sql.Expression

	cacheable bool
	re        *regexp.Regexp
}

var _ sql.FunctionExpression = (*RegexpMatches)(nil)

// NewRegexpMatches creates a new RegexpMatches expression.
func NewRegexpMatches(args ...sql.Expression) (sql.Expression, error) {
	var r RegexpMatches
	switch len(args) {
	case 3:
		r.Flags = args[2]
		fallthrough
	case 2:
		r.Text = args[0]
		r.Pattern = args[1]
	default:
		return nil, sql.ErrInvalidArgumentNumber.New("regexp_matches", "2 or 3", len(args))
	}

	if canBeCached(r.Pattern) && (r.Flags == nil || canBeCached(r.Flags)) {
		r.cacheable = true
	}

	return &r, nil
}

// FunctionName implements sql.FunctionExpression
func (r *RegexpMatches) FunctionName() string {
	return "regexp_matches"
}

// Type implements the sql.Expression interface.
func (r *RegexpMatches) Type() sql.Type { return sql.CreateArray(sql.LongText) }

// IsNullable implements the sql.Expression interface.
func (r *RegexpMatches) IsNullable() bool { return true }

// Children implements the sql.Expression interface.
func (r *RegexpMatches) Children() []sql.Expression {
	var result = []sql.Expression{r.Text, r.Pattern}
	if r.Flags != nil {
		result = append(result, r.Flags)
	}
	return result
}

// Resolved implements the sql.Expression interface.
func (r *RegexpMatches) Resolved() bool {
	return r.Text.Resolved() && r.Pattern.Resolved() && (r.Flags == nil || r.Flags.Resolved())
}

// WithChildren implements the sql.Expression interface.
func (r *RegexpMatches) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	required := 2
	if r.Flags != nil {
		required = 3
	}

	if len(children) != required {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), required)
	}

	return NewRegexpMatches(children...)
}

func (r *RegexpMatches) String() string {
	var args []string
	for _, e := range r.Children() {
		args = append(args, e.String())
	}
	return fmt.Sprintf("regexp_matches(%s)", strings.Join(args, ", "))
}

// Eval implements the sql.Expression interface.
func (r *RegexpMatches) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("function.RegexpMatches")
	defer span.Finish()

	var re *regexp.Regexp
	var err error
	if r.cacheable {
		if r.re == nil {
			r.re, err = r.compileRegex(ctx, nil)
			if err != nil {
				return nil, err
			}

			if r.re == nil {
				return nil, nil
			}
		}
		re = r.re
	} else {
		re, err = r.compileRegex(ctx, row)
		if err != nil {
			return nil, err
		}

		if re == nil {
			return nil, nil
		}
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

	matches := re.FindAllStringSubmatch(text.(string), -1)
	if len(matches) == 0 {
		return nil, nil
	}

	var result []interface{}
	for _, m := range matches {
		for _, sm := range m {
			result = append(result, sm)
		}
	}

	return result, nil
}

func (r *RegexpMatches) compileRegex(ctx *sql.Context, row sql.Row) (*regexp.Regexp, error) {
	pattern, err := r.Pattern.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if pattern == nil {
		return nil, nil
	}

	pattern, err = sql.LongText.Convert(pattern)
	if err != nil {
		return nil, err
	}

	var flags string
	if r.Flags != nil {
		f, err := r.Flags.Eval(ctx, row)
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

		flags = f.(string)
		for _, f := range flags {
			if !validRegexpFlags[f] {
				return nil, errInvalidRegexpFlag.New(f)
			}
		}

		flags = fmt.Sprintf("(?%s)", flags)
	}

	return regexp.Compile(flags + pattern.(string))
}

var errInvalidRegexpFlag = errors.NewKind("invalid regexp flag: %v")

var validRegexpFlags = map[rune]bool{
	'i': true,
}

func canBeCached(e sql.Expression) bool {
	var hasCols bool
	sql.Inspect(e, func(e sql.Expression) bool {
		if _, ok := e.(*expression.GetField); ok {
			hasCols = true
		}
		return true
	})
	return !hasCols
}
