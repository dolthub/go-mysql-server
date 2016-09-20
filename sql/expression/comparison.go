package expression

import (
	"fmt"

	"github.com/mvader/gitql/sql"
)

type Comparsion struct {
	BinaryExpression
	ChildType sql.Type
}

func (*Comparsion) Type() sql.Type {
	return sql.Boolean
}

type Equals struct {
	Comparsion
}

func NewEquals(left sql.Expression, right sql.Expression) *Equals {
	checkEqualTypes(left, right)
	return &Equals{Comparsion{BinaryExpression{left, right}, left.Type()}}
}

func (e Equals) Eval(row sql.Row) interface{} {
	a := e.Left.Eval(row)
	b := e.Right.Eval(row)
	return e.ChildType.Compare(a, b) == 0
}

type GreaterThan struct {
	Comparsion
}

func NewGreaterThan(left sql.Expression, right sql.Expression) *GreaterThan {
	checkEqualTypes(left, right)
	return &GreaterThan{Comparsion{BinaryExpression{left, right}, left.Type()}}
}

func (e GreaterThan) Eval(row sql.Row) interface{} {
	a := e.Left.Eval(row)
	b := e.Right.Eval(row)
	return e.ChildType.Compare(a, b) == 1
}

type LessThan struct {
	Comparsion
}

func NewLessThan(left sql.Expression, right sql.Expression) *LessThan {
	checkEqualTypes(left, right)
	return &LessThan{Comparsion{BinaryExpression{left, right}, left.Type()}}
}

func (e LessThan) Eval(row sql.Row) interface{} {
	a := e.Left.Eval(row)
	b := e.Right.Eval(row)
	return e.ChildType.Compare(a, b) == -1
}

type GreaterThanOrEqual struct {
	Comparsion
}

func NewGreaterThanOrEqual(left sql.Expression, right sql.Expression) *GreaterThanOrEqual {
	checkEqualTypes(left, right)
	return &GreaterThanOrEqual{Comparsion{BinaryExpression{left, right}, left.Type()}}
}

func (e GreaterThanOrEqual) Eval(row sql.Row) interface{} {
	a := e.Left.Eval(row)
	b := e.Right.Eval(row)
	return e.ChildType.Compare(a, b) > -1
}

type LessThanOrEqual struct {
	Comparsion
}

func NewLessThanOrEqual(left sql.Expression, right sql.Expression) *LessThanOrEqual {
	checkEqualTypes(left, right)
	return &LessThanOrEqual{Comparsion{BinaryExpression{left, right}, left.Type()}}
}

func (e LessThanOrEqual) Eval(row sql.Row) interface{} {
	a := e.Left.Eval(row)
	b := e.Right.Eval(row)
	return e.ChildType.Compare(a, b) < 1
}

func checkEqualTypes(a sql.Expression, b sql.Expression) {
	if a.Type() != b.Type() {
		panic(fmt.Errorf("both types should be equal: %v and %v\n", a, b))
	}
}

func (e Equals) Name() string {
	return e.Left.Name() + "==" + e.Right.Name()
}
