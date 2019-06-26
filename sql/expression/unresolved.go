package expression

import (
	"fmt"
	"strings"

	"github.com/src-d/go-mysql-server/sql"
)

// UnresolvedColumn is an expression of a column that is not yet resolved.
// This is a placeholder node, so its methods Type, IsNullable and Eval are not
// supposed to be called.
type UnresolvedColumn struct {
	name  string
	table string
}

// NewUnresolvedColumn creates a new UnresolvedColumn expression.
func NewUnresolvedColumn(name string) *UnresolvedColumn {
	return &UnresolvedColumn{name: name}
}

// NewUnresolvedQualifiedColumn creates a new UnresolvedColumn expression
// with a table qualifier.
func NewUnresolvedQualifiedColumn(table, name string) *UnresolvedColumn {
	return &UnresolvedColumn{name: name, table: table}
}

// Children implements the Expression interface.
func (*UnresolvedColumn) Children() []sql.Expression {
	return nil
}

// Resolved implements the Expression interface.
func (*UnresolvedColumn) Resolved() bool {
	return false
}

// IsNullable implements the Expression interface.
func (*UnresolvedColumn) IsNullable() bool {
	panic("unresolved column is a placeholder node, but IsNullable was called")
}

// Type implements the Expression interface.
func (*UnresolvedColumn) Type() sql.Type {
	panic("unresolved column is a placeholder node, but Type was called")
}

// Name implements the Nameable interface.
func (uc *UnresolvedColumn) Name() string { return uc.name }

// Table returns the table name.
func (uc *UnresolvedColumn) Table() string { return uc.table }

func (uc *UnresolvedColumn) String() string {
	if uc.table == "" {
		return uc.name
	}
	return fmt.Sprintf("%s.%s", uc.table, uc.name)
}

// Eval implements the Expression interface.
func (*UnresolvedColumn) Eval(ctx *sql.Context, r sql.Row) (interface{}, error) {
	panic("unresolved column is a placeholder node, but Eval was called")
}

// WithChildren implements the Expression interface.
func (uc *UnresolvedColumn) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(uc, len(children), 0)
	}
	return uc, nil
}

// UnresolvedFunction represents a function that is not yet resolved.
// This is a placeholder node, so its methods Type, IsNullable and Eval are not
// supposed to be called.
type UnresolvedFunction struct {
	name string
	// IsAggregate or not.
	IsAggregate bool
	// Children of the expression.
	Arguments []sql.Expression
}

// NewUnresolvedFunction creates a new UnresolvedFunction expression.
func NewUnresolvedFunction(
	name string,
	agg bool,
	arguments ...sql.Expression,
) *UnresolvedFunction {
	return &UnresolvedFunction{name, agg, arguments}
}

// Children implements the Expression interface.
func (uf *UnresolvedFunction) Children() []sql.Expression {
	return uf.Arguments
}

// Resolved implements the Expression interface.
func (*UnresolvedFunction) Resolved() bool {
	return false
}

// IsNullable implements the Expression interface.
func (*UnresolvedFunction) IsNullable() bool {
	panic("unresolved function is a placeholder node, but IsNullable was called")
}

// Type implements the Expression interface.
func (*UnresolvedFunction) Type() sql.Type {
	panic("unresolved function is a placeholder node, but Type was called")
}

// Name implements the Nameable interface.
func (uf *UnresolvedFunction) Name() string { return uf.name }

func (uf *UnresolvedFunction) String() string {
	var exprs = make([]string, len(uf.Arguments))
	for i, e := range uf.Arguments {
		exprs[i] = e.String()
	}
	return fmt.Sprintf("%s(%s)", uf.name, strings.Join(exprs, ", "))
}

// Eval implements the Expression interface.
func (*UnresolvedFunction) Eval(ctx *sql.Context, r sql.Row) (interface{}, error) {
	panic("unresolved function is a placeholder node, but Eval was called")
}

// WithChildren implements the Expression interface.
func (uf *UnresolvedFunction) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != len(uf.Arguments) {
		return nil, sql.ErrInvalidChildrenNumber.New(uf, len(children), len(uf.Arguments))
	}
	return NewUnresolvedFunction(uf.name, uf.IsAggregate, children...), nil
}
