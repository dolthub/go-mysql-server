package function

import (
	"fmt"
	"strings"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

type UnaryFuncLogic func(*sql.Context, interface{}) (interface{}, error)

type UnaryFunc struct {
	expression.UnaryExpression
	// Name is the name of the function
	Name string
	// The type returned by the function
	RetType sql.Type
	// Logic contains the logic being executed when the function is called
	Logic UnaryFuncLogic
}

var _ sql.FunctionExpression = (*UnaryFunc)(nil)

// NewUnaryFunc returns a function which is called to create a sql.Expression representing the function and its
// argemunts
func NewUnaryFunc(name string, retType sql.Type, logic UnaryFuncLogic) sql.Function1 {
	fn := func(e sql.Expression) sql.Expression {
		return &UnaryFunc{expression.UnaryExpression{Child: e}, name, retType, logic}
	}

	return sql.Function1{Name: name, Fn: fn}
}

// FunctionName implements sql.FunctionExpression
func (uf *UnaryFunc) FunctionName() string {
	return uf.Name
}

// Eval implements the Expression interface.
func (uf *UnaryFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if uf.Child == nil {
		return nil, nil
	}

	val, err := uf.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	return uf.Logic(ctx, val)
}

// String implements the fmt.Stringer interface.
func (uf *UnaryFunc) String() string {
	return fmt.Sprintf("%s(%s)", strings.ToUpper(uf.Name), uf.Child.String())
}

// IsNullable implements the Expression interface.
func (uf *UnaryFunc) IsNullable() bool {
	return uf.Child.IsNullable()
}

// WithChildren implements the Expression interface.
func (uf *UnaryFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(uf, len(children), 1)
	}

	return &UnaryFunc{expression.UnaryExpression{Child: children[0]}, uf.Name, uf.RetType, uf.Logic}, nil
}

// Type implements the Expression interface.
func (uf *UnaryFunc) Type() sql.Type {
	return uf.RetType
}
