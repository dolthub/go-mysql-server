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
	// The subquery to execute for each row in the outer result set
	Query sql.Node
	// The number of columns of outer scope schema expected before inner-query result row columns
	ScopeLen int
}

// NewSubquery returns a new subquery expression.
func NewSubquery(node sql.Node) *Subquery {
	return &Subquery{Query: node}
}

// Eval implements the Expression interface.
func (s *Subquery) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// TODO: the row being evaluated here might be shorter than the schema of the wrapping scope node. This is typically a
	//  problem for Project nodes, where there isn't a 1:1 correspondence between child row results and projections (more
	//  projections than unique columns in the underlying result set). This creates a problem for the evaluation of
	//  subquery rows -- all the indexes will be off, since they expect to be given a row the same length as the schema of
	//  their scope node. In this case, we fix the indexes by filling in zero values for the missing elements. This should
	//  probably be dealt with by adjustments to field indexes in the analyzer instead.
	scopeRow := row
	if len(scopeRow) < s.ScopeLen {
		scopeRow = make(sql.Row, s.ScopeLen)
		copy(scopeRow, row)
	}

	iter, err := s.Query.RowIter(ctx, scopeRow)
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

	// TODO: fix this. This should always be true, but isn't, because we don't consistently pass the scope row in all
	//  parts of the engine.
	col := 0
	if len(scopeRow) < len(rows[0]) {
		col = len(scopeRow)
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

// WithScopeLen returns the subquery with the scope length changed.
func (s *Subquery) WithScopeLen(length int) *Subquery {
	ns := *s
	ns.ScopeLen = length
	return &ns
}