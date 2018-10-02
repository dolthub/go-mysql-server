package plan

import (
	"fmt"
	"strings"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-vitess.v1/vt/sqlparser"
)

// Set configuration variables. Right now, only session variables are supported.
type Set struct {
	Variables []SetVariable
}

// SetVariable is a key-value pair to represent the value that will be set on
// a variable.
type SetVariable struct {
	Name  string
	Value sql.Expression
}

// NewSet creates a new Set node.
func NewSet(vars ...SetVariable) *Set {
	return &Set{vars}
}

// Resolved implements the sql.Node interface.
func (s *Set) Resolved() bool {
	for _, v := range s.Variables {
		if !v.Value.Resolved() {
			return false
		}
	}
	return true
}

// Children implements the sql.Node interface.
func (s *Set) Children() []sql.Node { return nil }

// TransformUp implements the sql.Node interface.
func (s *Set) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(s)
}

// TransformExpressions implements sql.Expressioner interface.
func (s *Set) TransformExpressions(f sql.TransformExprFunc) (sql.Node, error) {
	return s.TransformExpressionsUp(f)
}

// Expressions implements the sql.Expressioner interface.
func (s *Set) Expressions() []sql.Expression {
	var exprs = make([]sql.Expression, len(s.Variables))
	for i, v := range s.Variables {
		exprs[i] = v.Value
	}
	return exprs
}

// TransformExpressionsUp implements the sql.Node interface.
func (s *Set) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	var vars = make([]SetVariable, len(s.Variables))
	for i, v := range s.Variables {
		val, err := v.Value.TransformUp(f)
		if err != nil {
			return nil, err
		}

		vars[i] = v
		vars[i].Value = val
	}

	return NewSet(vars...), nil
}

// RowIter implements the sql.Node interface.
func (s *Set) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Set")
	defer span.Finish()

	const (
		sessionPrefix = sqlparser.SessionStr + "."
		globalPrefix  = sqlparser.GlobalStr + "."
	)
	for _, v := range s.Variables {
		value, err := v.Value.Eval(ctx, nil)
		if err != nil {
			return nil, err
		}

		name := strings.TrimLeft(v.Name, "@")
		if strings.HasPrefix(name, sessionPrefix) {
			name = name[len(sessionPrefix):]
		} else if strings.HasPrefix(name, globalPrefix) {
			name = name[len(globalPrefix):]
		}

		ctx.Set(name, v.Value.Type(), value)
	}

	return sql.RowsToRowIter(), nil
}

// Schema implements the sql.Node interface.
func (s *Set) Schema() sql.Schema { return nil }

func (s *Set) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Set")
	var children = make([]string, len(s.Variables))
	for i, v := range s.Variables {
		children[i] = fmt.Sprintf("%s = %s", v.Name, v.Value)
	}
	_ = p.WriteChildren(children...)
	return p.String()
}
