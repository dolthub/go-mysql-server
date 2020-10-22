package expression

import (
	"github.com/dolthub/go-mysql-server/sql"
	"sync"
)

// AutoIncrement represents a literal expression (string, number, bool, ...).
type AutoIncrement struct {
	UnaryExpression
	delta *Literal
	base  *Literal
	sync.Once
}

// NewAutoIncrement creates a new AutoIncrement expression.
func NewAutoIncrement(base sql.Expression) *AutoIncrement {
	return &AutoIncrement{
		UnaryExpression{Child: base},
		NewLiteral(0, base.Type()),
		nil,
		sync.Once{},
	}
}

// Resolved implements the Expression interface.
func (i *AutoIncrement) Resolved() bool {
	return i.UnaryExpression.Resolved()
}

// IsNullable implements the Expression interface.
func (i *AutoIncrement) IsNullable() bool {
	return i.UnaryExpression.IsNullable()
}

// Type implements the Expression interface.
func (i *AutoIncrement) Type() sql.Type {
	return i.UnaryExpression.Child.Type()
}

// Eval implements the Expression interface.
func (i *AutoIncrement) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	var err error
	i.Once.Do(func() {
		var val interface{}
		val, err = i.Child.Eval(ctx, nil)
		if err != nil {
			return
		}
		i.base = NewLiteral(val, i.Child.Type())
	})

	if err != nil {
		return nil, err
	}

	one := NewLiteral(1, i.Type())

	d, err := NewPlus(i.delta, one).Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	i.delta = NewLiteral(d, i.Type())

	return NewPlus(i.base, i.delta).Eval(ctx, nil)
}

func (i *AutoIncrement) String() string {
	return "" // todo
}

func (i *AutoIncrement) DebugString() string {
	return "" // todo
}

// WithChildren implements the Expression interface.
func (i *AutoIncrement) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	return &AutoIncrement{
		UnaryExpression{Child: children[0]},
		i.delta,
		i.base,
		sync.Once{},
	}, nil
}

// Children implements the Expression interface.
func (i *AutoIncrement) Children() []sql.Expression {
	return []sql.Expression{i.Child}
}
