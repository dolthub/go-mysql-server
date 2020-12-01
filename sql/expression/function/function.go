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

// FunctionName implements sql.FunctionExpression
func (uf *UnaryFunc) FunctionName() string {
	return uf.Name
}

// Eval implements the Expression interface.
func (uf *UnaryFunc) EvalChild(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if uf.Child == nil {
		return nil, nil
	}

	val, err := uf.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	return val, nil
}

// String implements the fmt.Stringer interface.
func (uf *UnaryFunc) String() string {
	return fmt.Sprintf("%s(%s)", strings.ToUpper(uf.Name), uf.Child.String())
}

// Type implements the Expression interface.
func (uf *UnaryFunc) Type() sql.Type {
	return uf.RetType
}
