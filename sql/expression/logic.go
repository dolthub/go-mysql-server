package expression

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// And checks whether two expressions are true.
type And struct {
	BinaryExpression
}

// NewAnd creates a new And expression.
func NewAnd(left, right sql.Expression) sql.Expression {
	return &And{BinaryExpression{Left: left, Right: right}}
}

// Name implements the Expression interface.
func (And) Name() string {
	return "AND"
}

// Type implements the Expression interface.
func (And) Type() sql.Type {
	return sql.Boolean
}

// Eval implements the Expression interface.
func (a *And) Eval(row sql.Row) (interface{}, error) {
	lval, err := a.Left.Eval(row)
	if err != nil {
		return nil, err
	}

	if lval == false {
		return false, nil
	}

	rval, err := a.Right.Eval(row)
	if err != nil {
		return nil, err
	}

	if rval == false {
		return false, nil
	}

	if lval == nil || rval == nil {
		return nil, nil
	}

	return true, nil
}

// TransformUp implements the Expression interface.
func (a *And) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return f(NewAnd(
		a.Left.TransformUp(f),
		a.Right.TransformUp(f),
	))
}

// Or checks whether one of the two given expressions is true.
type Or struct {
	BinaryExpression
}

// NewOr creates a new Or expression.
func NewOr(left, right sql.Expression) sql.Expression {
	return &Or{BinaryExpression{Left: left, Right: right}}
}

// Name implements the Expression interface.
func (Or) Name() string {
	return "OR"
}

// Type implements the Expression interface.
func (Or) Type() sql.Type {
	return sql.Boolean
}

// Eval implements the Expression interface.
func (o *Or) Eval(row sql.Row) (interface{}, error) {
	lval, err := o.Left.Eval(row)
	if err != nil {
		return nil, err
	}

	if lval == true {
		return true, nil
	}

	rval, err := o.Right.Eval(row)
	if err != nil {
		return nil, err
	}

	if lval == nil && rval == nil {
		return nil, nil
	}

	return rval == true, nil
}

// TransformUp implements the Expression interface.
func (o *Or) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return f(NewOr(
		o.Left.TransformUp(f),
		o.Right.TransformUp(f),
	))
}
