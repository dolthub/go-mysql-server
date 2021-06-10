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

func newDefaultLikeMatcher(likeStr string) (regex.DisposableMatcher, error) {
	return regex.NewDisposableMatcher("go", likeStr)
}

// Like performs pattern matching against two strings.
type Like struct {
	BinaryExpression
	pool   *sync.Pool
	once   sync.Once
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
		once:             sync.Once{},
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

	createMatcher := newDefaultLikeMatcher
	lType := l.Left.Type()
	lm, likeOK := lType.(sql.LikeMatcher)
	if likeOK {
		createMatcher = lm.CreateMatcher
	}

	var likeMatcher regex.DisposableMatcher
	if !l.cached {
		// for non-cached regex every time create a new matcher
		right, rerr := l.evalRight(ctx, row)
		if rerr != nil {
			return nil, rerr
		}
		likeMatcher, err = createMatcher(*right)
	} else {
		l.once.Do(func() {
			right, err := l.evalRight(ctx, row)
			l.pool = &sync.Pool{
				New: func() interface{} {
					if err != nil || right == nil {
						return matcherErrTuple{nil, err}
					}
					m, e := createMatcher(*right)
					return matcherErrTuple{m, e}
				},
			}
		})
		rwe := l.pool.Get().(matcherErrTuple)
		likeMatcher, err = rwe.matcher, rwe.err
	}

	if err != nil {
		return nil, err
	}

	ok := likeMatcher.Match(left.(string))
	if !l.cached {
		likeMatcher.Dispose()
	} else {
		l.pool.Put(matcherErrTuple{likeMatcher, nil})
	}

	return ok, nil
}

func (l *Like) evalRight(ctx *sql.Context, row sql.Row) (*string, error) {
	v, err := l.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	v, err = sql.LongText.Convert(v)
	if err != nil {
		return nil, err
	}
	s := patternToGoRegex(v.(string))
	return &s, nil
}

func (l *Like) String() string {
	return fmt.Sprintf("%s LIKE %s", l.Left, l.Right)
}

// WithChildren implements the Expression interface.
func (l *Like) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
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
