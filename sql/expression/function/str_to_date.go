package function

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse/dateparse"
)

// NewStrToDate constructs a new function expression from the given child expressions.
func NewStrToDate(args ...sql.Expression) (sql.Expression, error) {
	if len(args) != 2 {
		return nil, sql.ErrInvalidArgumentNumber.New("STR_TO_DATE", 2, len(args))
	}
	return &StringToDatetime{
		Date:   args[0],
		Format: args[1],
	}, nil
}

// StringToDatetime defines the built-in function STR_TO_DATE(str, format)
type StringToDatetime struct {
	Date   sql.Expression
	Format sql.Expression
}

var _ sql.FunctionExpression = (*StringToDatetime)(nil)

// Resolved returns whether the node is resolved.
func (s StringToDatetime) Resolved() bool {
	dateResolved := s.Date == nil || s.Date.Resolved()
	formatResolved := s.Format == nil || s.Format.Resolved()
	return dateResolved && formatResolved
}

func (s StringToDatetime) String() string {
	return fmt.Sprintf("STR_TO_DATE(%s, %s)", s.Date, s.Format)
}

// Type returns the expression type.
func (s StringToDatetime) Type() sql.Type {
	return sql.Datetime
}

// IsNullable returns whether the expression can be null.
func (s StringToDatetime) IsNullable() bool {
	return true
}

// Eval evaluates the given row and returns a result.
func (s StringToDatetime) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
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
		return sql.Null, nil
	}
	return goTime, nil
}

// Children returns the children expressions of this expression.
func (s StringToDatetime) Children() []sql.Expression {
	children := make([]sql.Expression, 0, 2)
	if s.Date != nil {
		children = append(children, s.Date)
	}
	if s.Format != nil {
		children = append(children, s.Format)
	}
	return children
}

func (s StringToDatetime) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewStrToDate(children...)
}

func (s StringToDatetime) FunctionName() string {
	return "str_to_date"
}
