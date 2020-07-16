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
}

// NewSubquery returns a new subquery expression.
func NewSubquery(node sql.Node) *Subquery {
	return &Subquery{Query: node}
}

// Eval implements the Expression interface.
func (s *Subquery) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	iter, err := s.Query.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	rows, err := sql.RowIterToRows(iter)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	if len(rows) > 1 {
		return nil, errExpectedSingleRow.New()
	}

	// TODO: fix this
	col := 0
	if len(row) <= len(rows[0]) {
		col = len(row)
	}
	return rows[0][col], nil
}

// EvalMultiple returns all rows returned by a subquery.
// TODO: give row context
func (s *Subquery) EvalMultiple(ctx *sql.Context) ([]interface{}, error) {
	iter, err := s.Query.RowIter(ctx, nil)
	if err != nil {
		return nil, err
	}

	rows, err := sql.RowIterToRows(iter)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	var result = make([]interface{}, len(rows))
	for i, row := range rows {
		result[i] = row[0]
	}

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