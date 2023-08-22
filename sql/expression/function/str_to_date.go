package function

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql/planbuilder/dateparse"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// NewStrToDate constructs a new function expression from the given child expressions.
func NewStrToDate(args ...sql.Expression) (sql.Expression, error) {
	if len(args) != 2 {
		return nil, sql.ErrInvalidArgumentNumber.New("STR_TO_DATE", 2, len(args))
	}
	return &StrToDate{
		Date:   args[0],
		Format: args[1],
	}, nil
}

// StrToDate defines the built-in function STR_TO_DATE(str, format)
type StrToDate struct {
	Date   sql.Expression
	Format sql.Expression
}

var _ sql.FunctionExpression = (*StrToDate)(nil)
var _ sql.CollationCoercible = (*StrToDate)(nil)

// Description implements sql.FunctionExpression
func (s StrToDate) Description() string {
	return "parses the date/datetime/timestamp expression according to the format specifier."
}

// Resolved returns whether the node is resolved.
func (s StrToDate) Resolved() bool {
	dateResolved := s.Date == nil || s.Date.Resolved()
	formatResolved := s.Format == nil || s.Format.Resolved()
	return dateResolved && formatResolved
}

func (s StrToDate) String() string {
	return fmt.Sprintf("%s(%s,%s)", s.FunctionName(), s.Date, s.Format)
}

// Type returns the expression type.
func (s StrToDate) Type() sql.Type {
	return types.DatetimeMaxPrecision
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (StrToDate) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// IsNullable returns whether the expression can be null.
func (s StrToDate) IsNullable() bool {
	return true
}

// Eval evaluates the given row and returns a result.
func (s StrToDate) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	date, err := s.Date.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	format, err := s.Format.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	dateStr, ok := date.(string)
	if !ok {
		// TODO: improve this error
		return nil, sql.ErrInvalidType.New(fmt.Sprintf("%T", date))
	}
	formatStr, ok := format.(string)
	if !ok {
		// TODO: improve this error
		return nil, sql.ErrInvalidType.New(fmt.Sprintf("%T", formatStr))
	}

	goTime, err := dateparse.ParseDateWithFormat(dateStr, formatStr)
	if err != nil {
		ctx.Warn(1411, fmt.Sprintf("Incorrect value: '%s' for function %s", dateStr, s.FunctionName()))
		return nil, nil
	}

	// zero dates '0000-00-00' and '2010-00-13' are allowed,
	// but depends on strict sql_mode with NO_ZERO_DATE or NO_ZERO_IN_DATE modes enabled.
	return goTime, nil
}

// Children returns the children expressions of this expression.
func (s StrToDate) Children() []sql.Expression {
	children := make([]sql.Expression, 0, 2)
	if s.Date != nil {
		children = append(children, s.Date)
	}
	if s.Format != nil {
		children = append(children, s.Format)
	}
	return children
}

func (s StrToDate) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewStrToDate(children...)
}

func (s StrToDate) FunctionName() string {
	return "str_to_date"
}
