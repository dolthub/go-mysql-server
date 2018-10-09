package expression

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/src-d/go-mysql-server.v0/internal/regex"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Like performs pattern matching against two strings.
type Like struct {
	BinaryExpression
	canCacheRegex bool
	regex         regex.Matcher
}

// NewLike creates a new LIKE expression.
func NewLike(left, right sql.Expression) sql.Expression {
	var canCacheRegex = true
	Inspect(right, func(e sql.Expression) bool {
		if _, ok := e.(*GetField); ok {
			canCacheRegex = false
		}
		return true
	})

	return &Like{BinaryExpression{left, right}, canCacheRegex, nil}
}

// Type implements the sql.Expression interface.
func (l *Like) Type() sql.Type { return sql.Boolean }

// Eval implements the sql.Expression interface.
func (l *Like) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("expression.Like")
	defer span.Finish()

	var re regex.Matcher
	if l.regex == nil {
		v, err := l.Right.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		v, err = sql.Text.Convert(v)
		if err != nil {
			return nil, err
		}

		re, err = regex.New(regex.Default(), patternToRegex(v.(string)))
		if err != nil {
			return nil, err
		}

		if l.canCacheRegex {
			l.regex = re
		}
	} else {
		re = l.regex
	}

	value, err := l.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	value, err = sql.Text.Convert(value)
	if err != nil {
		return nil, err
	}

	return re.Match(value.(string)), nil
}

func (l *Like) String() string {
	return fmt.Sprintf("%s LIKE %s", l.Left, l.Right)
}

// TransformUp implements the sql.Expression interface.
func (l *Like) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	left, err := l.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := l.Right.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewLike(left, right))
}

func patternToRegex(pattern string) string {
	var buf bytes.Buffer
	buf.WriteRune('^')
	var escaped bool
	for _, r := range strings.Replace(regexp.QuoteMeta(pattern), `\\`, `\`, -1) {
		switch r {
		case '_':
			if escaped {
				buf.WriteRune(r)
			} else {
				buf.WriteRune('.')
			}
		case '%':
			if !escaped {
				buf.WriteString(".*")
			} else {
				buf.WriteRune(r)
			}
		case '\\':
			if escaped {
				buf.WriteString(`\\`)
			} else {
				escaped = true
				continue
			}
		default:
			if escaped {
				buf.WriteString(`\`)
			}
			buf.WriteRune(r)
		}

		if escaped {
			escaped = false
		}
	}

	buf.WriteRune('$')
	return buf.String()
}
