package expression

import (
	"fmt"

	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

var errExpectedSingleRow = errors.NewKind("the subquery returned more than 1 row")

// Subquery is as an expression whose value is derived by executing a subquery. It must be executed for every row in
// the outer result set.
type Subquery struct {
	Query sql.Node
	// OuterScopeSchema contains the schema of rows from the outer scope, which will be used in the evaluation of the
	// query for each outer scope row.
	OuterScopeSchema sql.Schema
	value interface{}
}

// NewSubquery returns a new subquery expression.
func NewSubquery(node sql.Node) *Subquery {
	return &Subquery{Query: node}
}

// Eval implements the Expression interface.
func (s *Subquery) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
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

func (s *Subquery) DebugString() string {
	return fmt.Sprintf("(%s)", sql.DebugString(s.Query))
}

// Resolved implements the Expression interface.
func (s *Subquery) Resolved() bool {
	return s.Query.Resolved()
}

// Type implements the Expression interface.
func (s *Subquery) Type() sql.Type {
	// TODO: handle row results (more than one column)
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

// WithOuterScopeSchema returns the subquery with the outer scope schema changed
func (s *Subquery) WithOuterScopeSchema(schema sql.Schema) *Subquery {
	ns := *s
	ns.OuterScopeSchema = schema
	return &ns
}
