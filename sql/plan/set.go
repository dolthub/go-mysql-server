package plan

import (
	"fmt"
	"strings"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"vitess.io/vitess/go/vt/sqlparser"
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
		if _, ok := v.Value.(*expression.DefaultColumn); ok {
			continue
		}
		if !v.Value.Resolved() {
			return false
		}
	}
	return true
}

// Children implements the sql.Node interface.
func (s *Set) Children() []sql.Node { return nil }

// WithChildren implements the Node interface.
func (s *Set) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 0)
	}

	return s, nil
}

// WithExpressions implements the Expressioner interface.
func (s *Set) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(s.Variables) {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(exprs), len(s.Variables))
	}

	var vars = make([]SetVariable, len(s.Variables))
	for i, v := range s.Variables {
		vars[i] = SetVariable{
			Name:  v.Name,
			Value: exprs[i],
		}
	}

	return NewSet(vars...), nil
}

// Expressions implements the sql.Expressioner interface.
func (s *Set) Expressions() []sql.Expression {
	var exprs = make([]sql.Expression, len(s.Variables))
	for i, v := range s.Variables {
		exprs[i] = v.Value
	}
	return exprs
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
		var (
			value interface{}
			typ   sql.Type
			err   error
		)

		name := strings.TrimPrefix(
			strings.TrimPrefix(strings.TrimLeft(v.Name, "@"), sessionPrefix),
			globalPrefix,
		)

		switch v.Value.(type) {
		case *expression.DefaultColumn:
			valtyp, ok := sql.DefaultSessionConfig()[name]
			if !ok {
				continue
			}
			value, typ = valtyp.Value, valtyp.Typ
		default:
			// TODO: value checking for system variables. Each one has specific lists of acceptable values.
			value, err = v.Value.Eval(ctx, nil)
			if err != nil {
				return nil, err
			}
			typ = v.Value.Type()
		}

		ctx.Set(name, typ, value)
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
