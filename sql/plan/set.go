package plan

import (
	"fmt"
	"strings"

	"github.com/liquidata-inc/vitess/go/vt/sqlparser"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

// Set configuration variables. Right now, only session variables are supported.
type Set struct {
	Exprs []sql.Expression
}

// NewSet creates a new Set node.
func NewSet(vars ...sql.Expression) *Set {
	return &Set{vars}
}

// Resolved implements the sql.Node interface.
func (s *Set) Resolved() bool {
	for _, v := range s.Exprs {
		// TODO (maybe)?
		// if _, ok := v.Right.(*expression.DefaultColumn); ok {
		// 	continue
		// }
		if !v.Resolved() {
			return false
		}
	}
	return true
}

// Children implements the sql.Node interface.
func (s *Set) Children() []sql.Node { return nil }

// WithChildren implements the sql.Node interface.
func (s *Set) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 0)
	}

	return s, nil
}

// WithExpressions implements the sql.Expressioner interface.
func (s *Set) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(s.Exprs) {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(exprs), len(s.Exprs))
	}

	return NewSet(exprs...), nil
}

// Expressions implements the sql.Expressioner interface.
func (s *Set) Expressions() []sql.Expression {
	return s.Exprs
}

// RowIter implements the sql.Node interface.
func (s *Set) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Set")
	defer span.Finish()

	const (
		sessionPrefix = sqlparser.SessionStr + "."
		globalPrefix  = sqlparser.GlobalStr + "."
	)

	for _, v := range s.Exprs {
		switch v.(type) {
		case *expression.SetField:
			// OK, continue
		default:
			panic(fmt.Sprintf("unrecognized type %T", v))
		}

		setField := v.(*expression.SetField)

		var (
			value interface{}
			typ   sql.Type
			err   error
		)

		varName := strings.TrimPrefix(
			strings.TrimPrefix(strings.TrimLeft(setField.Left.String(), "@"), sessionPrefix),
			globalPrefix,
		)

		// TODO: value checking for system variables. Each one has specific lists of acceptable values.
		value, err = setField.Right.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		typ = setField.Left.Type()

		// TODO: differentiate between system and user vars here
		err = ctx.Set(ctx, varName, typ, value)
		if err != nil {
			return nil, err
		}
	}

	return sql.RowsToRowIter(), nil
}

// Schema implements the sql.Node interface.
func (s *Set) Schema() sql.Schema { return nil }

func (s *Set) String() string {
	var children = make([]string, len(s.Exprs))
	for i, v := range s.Exprs {
		children[i] = fmt.Sprintf(v.String())
	}
	return fmt.Sprintf("SET %s", strings.Join(children, ", "))
}

func (s *Set) DebugString() string {
	var children = make([]string, len(s.Exprs))
	for i, v := range s.Exprs {
		children[i] = fmt.Sprintf(sql.DebugString(v))
	}
	return fmt.Sprintf("SET %s", strings.Join(children, ", "))
}
