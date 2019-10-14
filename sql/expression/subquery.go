package expression

import (
	"fmt"

	"github.com/src-d/go-mysql-server/sql"
	errors "gopkg.in/src-d/go-errors.v1"
)

var errExpectedSingleRow = errors.NewKind("the subquery returned more than 1 row")

// Subquery that is executed as an expression.
type Subquery struct {
	Query sql.Node
	value interface{}
}

// NewSubquery returns a new subquery node.
func NewSubquery(node sql.Node) *Subquery {
	return &Subquery{node, nil}
}

// Eval implements the Expression interface.
func (s *Subquery) Eval(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	if s.value != nil {
		if elems, ok := s.value.([]interface{}); ok {
			if len(elems) > 1 {
				return nil, errExpectedSingleRow.New()
			}
			return elems[0], nil
		}
		return s.value, nil
	}

	iter, err := s.Query.RowIter(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := sql.RowIterToRows(iter)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		s.value = nil
		return nil, nil
	}

	if len(rows) > 1 {
		return nil, errExpectedSingleRow.New()
	}

	s.value = rows[0][0]
	return s.value, nil
}

// EvalMultiple returns all rows returned by a subquery.
func (s *Subquery) EvalMultiple(ctx *sql.Context) ([]interface{}, error) {
	if s.value != nil {
		return s.value.([]interface{}), nil
	}

	iter, err := s.Query.RowIter(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := sql.RowIterToRows(iter)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		s.value = []interface{}{}
		return nil, nil
	}

	var result = make([]interface{}, len(rows))
	for i, row := range rows {
		result[i] = row[0]
	}
	s.value = result

	return result, nil
}

// IsNullable implements the Expression interface.
func (s *Subquery) IsNullable() bool {
	return s.Query.Schema()[0].Nullable
}

func (s *Subquery) String() string {
	return fmt.Sprintf("(%s)", s.Query)
}

// Resolved implements the Expression interface.
func (s *Subquery) Resolved() bool {
	return s.Query.Resolved()
}

// Type implements the Expression interface.
func (s *Subquery) Type() sql.Type {
	return s.Query.Schema()[0].Type
}

// WithChildren implements the Expression interface.
func (s *Subquery) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 0)
	}
	return s, nil
}

// Children implements the Expression interface.
func (s *Subquery) Children() []sql.Expression {
	return nil
}

// WithQuery returns the subquery with the query node changed.
func (s *Subquery) WithQuery(node sql.Node) *Subquery {
	ns := *s
	ns.Query = node
	return &ns
}
