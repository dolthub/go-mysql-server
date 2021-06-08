package function

import "github.com/dolthub/go-mysql-server/sql"

// RowCount implements the ROW_COUNT() function
type RowCount struct{}

func NewRowCount(ctx *sql.Context) sql.Expression {
	return RowCount{}
}

var _ sql.FunctionExpression = RowCount{}

// Resolved implements sql.Expression
func (r RowCount) Resolved() bool {
	return true
}

// String implements sql.Expression
func (r RowCount) String() string {
	return "ROW_COUNT()"
}

// Type implements sql.Expression
func (r RowCount) Type() sql.Type {
	return sql.Uint64
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
func (r RowCount) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return sql.NillaryWithChildren(r, children...)
}

// FunctionName implements sql.FunctionExpression
func (r RowCount) FunctionName() string {
	return "row_count"
}

// LastInsertId implements the LAST_INSERT_ID() function
type LastInsertId struct{}

func NewLastInsertId(ctx *sql.Context) sql.Expression {
	return LastInsertId{}
}

var _ sql.FunctionExpression = LastInsertId{}

// Resolved implements sql.Expression
func (r LastInsertId) Resolved() bool {
	return true
}

// String implements sql.Expression
func (r LastInsertId) String() string {
	return "LAST_INSERT_ID()"
}

// Type implements sql.Expression
func (r LastInsertId) Type() sql.Type {
	return sql.Uint64
}

// IsNullable implements sql.Expression
func (r LastInsertId) IsNullable() bool {
	return false
}

// Eval implements sql.Expression
func (r LastInsertId) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return ctx.GetLastQueryInfo(sql.LastInsertId), nil
}

// Children implements sql.Expression
func (r LastInsertId) Children() []sql.Expression {
	return nil
}

// WithChildren implements sql.Expression
func (r LastInsertId) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return sql.NillaryWithChildren(r, children...)
}

// FunctionName implements sql.FunctionExpression
func (r LastInsertId) FunctionName() string {
	return "last_insert_id"
}

// FoundRows implements the FOUND_ROWS() function
type FoundRows struct{}

func NewFoundRows(ctx *sql.Context) sql.Expression {
	return FoundRows{}
}

var _ sql.FunctionExpression = FoundRows{}

// Resolved implements sql.Expression
func (r FoundRows) Resolved() bool {
	return true
}

// String implements sql.Expression
func (r FoundRows) String() string {
	return "FOUND_ROWS()"
}

// Type implements sql.Expression
func (r FoundRows) Type() sql.Type {
	return sql.Uint64
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
func (r FoundRows) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return sql.NillaryWithChildren(r, children...)
}

// FunctionName implements sql.FunctionExpression
func (r FoundRows) FunctionName() string {
	return "found_rows"
}
