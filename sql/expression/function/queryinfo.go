package function

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// RowCount implements the ROW_COUNT() function
type RowCount struct{}

func (r RowCount) IsNonDeterministic() bool {
	return true
}

func NewRowCount() sql.Expression {
	return RowCount{}
}

var _ sql.FunctionExpression = RowCount{}
var _ sql.CollationCoercible = RowCount{}

// Description implements sql.FunctionExpression
func (r RowCount) Description() string {
	return "returns the number of rows updated."
}

// Resolved implements sql.Expression
func (r RowCount) Resolved() bool {
	return true
}

// String implements sql.Expression
func (r RowCount) String() string {
	return fmt.Sprintf("%s()", r.FunctionName())
}

// Type implements sql.Expression
func (r RowCount) Type() sql.Type {
	return types.Int64
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (RowCount) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// IsNullable implements sql.Expression
func (r RowCount) IsNullable() bool {
	return false
}

// Eval implements sql.Expression
func (r RowCount) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return ctx.GetLastQueryInfo(sql.RowCount), nil
}

// Children implements sql.Expression
func (r RowCount) Children() []sql.Expression {
	return nil
}

// WithChildren implements sql.Expression
func (r RowCount) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return sql.NillaryWithChildren(r, children...)
}

// FunctionName implements sql.FunctionExpression
func (r RowCount) FunctionName() string {
	return "row_count"
}

// LastInsertId implements the LAST_INSERT_ID() function
type LastInsertId struct {
	expression.UnaryExpression
}

func (r LastInsertId) IsNonDeterministic() bool {
	return true
}

func NewLastInsertId(children ...sql.Expression) (sql.Expression, error) {
	switch len(children) {
	case 0:
		return LastInsertId{}, nil
	case 1:
		return LastInsertId{UnaryExpression: expression.UnaryExpression{Child: children[0]}}, nil
	default:
		return nil, sql.ErrInvalidArgumentNumber.New("LastInsertId", len(children), 1)
	}
}

var _ sql.FunctionExpression = LastInsertId{}
var _ sql.CollationCoercible = LastInsertId{}

// Description implements sql.FunctionExpression
func (r LastInsertId) Description() string {
	return "returns value of the AUTOINCREMENT column for the last INSERT."
}

// Resolved implements sql.Expression
func (r LastInsertId) Resolved() bool {
	return true
}

// String implements sql.Expression
func (r LastInsertId) String() string {
	return fmt.Sprintf("%s(%s)", r.FunctionName(), r.Child)
}

// Type implements sql.Expression
func (r LastInsertId) Type() sql.Type {
	return types.Uint64
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (LastInsertId) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// IsNullable implements sql.Expression
func (r LastInsertId) IsNullable() bool {
	return false
}

// Eval implements sql.Expression
func (r LastInsertId) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// With no arguments, just return the last insert id for this session
	if len(r.Children()) == 0 {
		lastInsertId := ctx.GetLastQueryInfo(sql.LastInsertId)
		unsigned, _, err := types.Uint64.Convert(lastInsertId)
		if err != nil {
			return nil, err
		}
		return unsigned, nil
	}

	// If an expression is provided, we set the next insert id for this session as well as returning it
	res, err := r.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	id, _, err := types.Int64.Convert(res)
	if err != nil {
		return nil, err
	}

	ctx.SetLastQueryInfo(sql.LastInsertId, id.(int64))
	return id, nil
}

// Children implements sql.Expression
func (r LastInsertId) Children() []sql.Expression {
	if r.Child == nil {
		return nil
	}
	return []sql.Expression{r.Child}
}

// WithChildren implements sql.Expression
func (r LastInsertId) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewLastInsertId(children...)
}

// FunctionName implements sql.FunctionExpression
func (r LastInsertId) FunctionName() string {
	return "last_insert_id"
}

// FoundRows implements the FOUND_ROWS() function
type FoundRows struct{}

func (r FoundRows) IsNonDeterministic() bool {
	return true
}

func NewFoundRows() sql.Expression {
	return FoundRows{}
}

var _ sql.FunctionExpression = FoundRows{}
var _ sql.CollationCoercible = FoundRows{}

// FunctionName implements sql.FunctionExpression
func (r FoundRows) FunctionName() string {
	return "found_rows"
}

// Description implements sql.Expression
func (r FoundRows) Description() string {
	return "for a SELECT with a LIMIT clause, returns the number of rows that would be returned were there no LIMIT clause."
}

// Resolved implements sql.Expression
func (r FoundRows) Resolved() bool {
	return true
}

// String implements sql.Expression
func (r FoundRows) String() string {
	return fmt.Sprintf("%s()", r.FunctionName())
}

// Type implements sql.Expression
func (r FoundRows) Type() sql.Type {
	return types.Int64
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (FoundRows) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// IsNullable implements sql.Expression
func (r FoundRows) IsNullable() bool {
	return false
}

// Eval implements sql.Expression
func (r FoundRows) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return ctx.GetLastQueryInfo(sql.FoundRows), nil
}

// Children implements sql.Expression
func (r FoundRows) Children() []sql.Expression {
	return nil
}

// WithChildren implements sql.Expression
func (r FoundRows) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return sql.NillaryWithChildren(r, children...)
}
