package function

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type UnaryFunc struct {
	expression.UnaryExpression
	// Name is the name of the function
	Name string
	// The type returned by the function
	RetType sql.Type
}

func NewUnaryFunc(arg sql.Expression, name string, returnType sql.Type) *UnaryFunc {
	return &UnaryFunc{
		UnaryExpression: expression.UnaryExpression{Child: arg},
		Name:            name,
		RetType:         returnType,
	}
}

// FunctionName implements sql.FunctionExpression
func (uf *UnaryFunc) FunctionName() string {
	return uf.Name
}

// EvalChild is a convenience function for safely evaluating a child expression
func (uf *UnaryFunc) EvalChild(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if uf.Child == nil {
		return nil, nil
	}

	return uf.Child.Eval(ctx, row)
}

// String implements the fmt.Stringer interface.
func (uf *UnaryFunc) String() string {
	return fmt.Sprintf("%s(%s)", strings.ToUpper(uf.Name), uf.Child.String())
}

// Type implements the Expression interface.
func (uf *UnaryFunc) Type() sql.Type {
	return uf.RetType
}
