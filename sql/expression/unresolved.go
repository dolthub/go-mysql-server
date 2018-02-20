package expression

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// UnresolvedColumn is an expression of a column that is not yet resolved.
// This is a placeholder node, so its methods Type, IsNullable and Eval are not
// supposed to be called.
type UnresolvedColumn struct {
	name string
}

// NewUnresolvedColumn creates a new UnresolvedColumn expression.
func NewUnresolvedColumn(name string) *UnresolvedColumn {
	return &UnresolvedColumn{name}
}

// Resolved implements the Resolvable interface.
func (UnresolvedColumn) Resolved() bool {
	return false
}

// IsNullable implements the Expression interface.
func (UnresolvedColumn) IsNullable() bool {
	panic("unresolved column is a placeholder node, but IsNullable was called")
}

// Type implements the Expression interface.
func (UnresolvedColumn) Type() sql.Type {
	panic("unresolved column is a placeholder node, but Type was called")
}

// Name implements the Expression interface.
func (uc UnresolvedColumn) Name() string {
	return uc.name
}

// Eval implements the Expression interface.
func (UnresolvedColumn) Eval(r sql.Row) interface{} {
	panic("unresolved column is a placeholder node, but Eval was called")
}

// TransformUp implements the Transformable interface.
func (uc *UnresolvedColumn) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	n := *uc
	return f(&n)
}

// UnresolvedFunction represents a function that is not yet resolved.
// This is a placeholder node, so its methods Type, IsNullable and Eval are not
// supposed to be called.
type UnresolvedFunction struct {
	name string
	// IsAggregate or not.
	IsAggregate bool
	// Children of the expression.
	Children []sql.Expression
}

// NewUnresolvedFunction creates a new UnresolvedFunction expression.
func NewUnresolvedFunction(
	name string,
	agg bool,
	children ...sql.Expression,
) *UnresolvedFunction {
	return &UnresolvedFunction{name, agg, children}
}

// Resolved implements the Resolvable interface.
func (UnresolvedFunction) Resolved() bool {
	return false
}

// IsNullable implements the Expression interface.
func (UnresolvedFunction) IsNullable() bool {
	panic("unresolved function is a placeholder node, but IsNullable was called")
}

// Type implements the Expression interface.
func (UnresolvedFunction) Type() sql.Type {
	panic("unresolved function is a placeholder node, but Type was called")
}

// Name implements the Expression interface.
func (uf UnresolvedFunction) Name() string {
	return uf.name
}

// Eval implements the Expression interface.
func (UnresolvedFunction) Eval(r sql.Row) interface{} {
	panic("unresolved function is a placeholder node, but Eval was called")
}

// TransformUp implements the Transformable interface.
func (uf *UnresolvedFunction) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	var rc []sql.Expression
	for _, c := range uf.Children {
		rc = append(rc, f(c))
	}

	return f(NewUnresolvedFunction(uf.name, uf.IsAggregate, rc...))
}
