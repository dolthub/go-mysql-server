package expression

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

type BindVar struct {
	Name string
}

func NewBindVar(name string) sql.Expression {
	return &BindVar{name}
}

func (bv *BindVar) Resolved() bool {
	return true
}

func (bv *BindVar) String() string {
	return "BindVar(" + bv.Name + ")"
}

func (bv *BindVar) Type() sql.Type {
	return sql.LongText
}

func (bv *BindVar) IsNullable() bool {
	return true
}

func (bv *BindVar) Eval(*sql.Context, sql.Row) (interface{}, error) {
	return nil, fmt.Errorf("attempt to evaluate unbound placeholder variable %s", bv.Name)
}

func (bv *BindVar) Children() []sql.Expression {
	return nil
}

func (bv *BindVar) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
                return nil, sql.ErrInvalidChildrenNumber.New(bv, len(children), 0)

	}
	return bv, nil
}
