package expression

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/internal/regex"
	"github.com/dolthub/go-mysql-server/sql"
)

// Like performs pattern matching against two strings.
type Like struct {
	BinaryExpression
	pool   *sync.Pool
	cached bool
}

// NewLike creates a new LIKE expression.
func NewLike(left, right sql.Expression) sql.Expression {
	var cached = true
	sql.Inspect(right, func(e sql.Expression) bool {
		if _, ok := e.(*GetField); ok {
			cached = false
		}
		return true
	})

	return &Like{
		BinaryExpression: BinaryExpression{left, right},
		pool:             nil,
		cached:           cached,
	}
}

// Type implements the sql.Expression interface.
func (l *Like) Type() sql.Type { return sql.Boolean }

// Eval implements the sql.Expression interface.
func (l *Like) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("expression.Like")
	defer span.Finish()

	left, err := l.Left.Eval(ctx, row)
	if err != nil || left == nil {
		return nil, err
	}
	left, err = sql.LongText.Convert(left)
	if err != nil {
		return nil, err
	}

	var (
		matcher  regex.Matcher
		disposer regex.Disposer
		right    string
	)
	// eval right and convert to text
	if !l.cached || l.pool == nil {
		var v interface{}
		v, err = l.Right.Eval(ctx, row)
		if err != nil || v == nil {
			return nil, err
		}
		v, err = sql.LongText.Convert(v)
		if err != nil {
			return nil, err
		}
		right = patternToGoRegex(v.(string))
	}
	// for non-cached regex every time create a new matcher
	if !l.cached {
		matcher, disposer, err = regex.New("go", right)
	} else {
		if l.pool == nil {
			l.pool = &sync.Pool{
				New: func() interface{} {
					r, _, e := regex.New(regex.Default(), right)
					if e != nil {
						err = e
						return nil
					}
					return r
				},
			}
		}
		matcher = l.pool.Get().(regex.Matcher)
	}
	if matcher == nil {
		return nil, err
	}

	ok := matcher.Match(left.(string))
	if !l.cached {
		disposer.Dispose()
	} else if l.pool != nil {
		l.pool.Put(matcher)

	}

	return ok, nil
}

func (l *Like) String() string {
	return fmt.Sprintf("%s LIKE %s", l.Left, l.Right)
}

// WithChildren implements the Expression interface.
func (l *Like) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 2)
	}
	return NewLike(children[0], children[1]), nil
}

func patternToGoRegex(pattern string) string {
	var buf bytes.Buffer
	buf.WriteString("(?s)")
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
