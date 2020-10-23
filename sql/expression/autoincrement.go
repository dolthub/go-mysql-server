package expression

import (
	"errors"
	"fmt"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

// AutoIncrement represents a literal expression (string, number, bool, ...).
type AutoIncrement struct {
	BinaryExpression
	lastInsertId *Literal
	sync.Once
}

// NewAutoIncrement creates a new AutoIncrement expression.
func NewAutoIncrement(lastInsertId, given sql.Expression) (*AutoIncrement, error) {
	_, ok := lastInsertId.Type().(sql.NumberType)
	if !ok {
		return nil, errors.New("AutoIncrement must be given a number type expression")
	}

	return &AutoIncrement{
		BinaryExpression{Left: lastInsertId, Right: given},
		nil,
		sync.Once{},
	}, nil
}

// Resolved implements the Expression interface.
func (i *AutoIncrement) Resolved() bool {
	return i.BinaryExpression.Resolved()
}

// IsNullable implements the Expression interface.
func (i *AutoIncrement) IsNullable() bool {
	return false
}

// Type implements the Expression interface.
func (i *AutoIncrement) Type() sql.Type {
	return i.Left.Type()
}

func (i *AutoIncrement) init(ctx *sql.Context) error {
	var err error
	i.Once.Do(func() {
		var base interface{}
		base, err = i.Left.Eval(ctx, nil)
		if err != nil {
			return
		}

		i.lastInsertId = NewLiteral(base, i.Left.Type())
	})
	return err
}

// Eval implements the Expression interface.
func (i *AutoIncrement) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	err := i.init(ctx)
	if err != nil {
		return nil, err
	}

	// get value provided by INSERT
	val, err := i.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		// provide AUTO_INCREMENT value
		one := NewLiteral(sql.NumericUnaryValue(i.Type()), i.Type())
		id, err := NewPlus(i.lastInsertId, one).Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		i.lastInsertId = NewLiteral(id, i.Type())
	} else {
		// last_insert_id = max(given, last_insert_id)
		cmp, err := i.Type().Compare(val, i.lastInsertId.value)
		if err != nil {
			return nil, err
		}
		if cmp <= 0 {
			return val, nil
		}
		i.lastInsertId = NewLiteral(val, i.Type())
	}

	return i.lastInsertId.Eval(ctx, row)
}

func (i *AutoIncrement) String() string {
	return fmt.Sprintf("AutoIncrement(%s)", i.Left.String())
}

// WithChildren implements the Expression interface.
func (i *AutoIncrement) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	return &AutoIncrement{
		BinaryExpression{Left: children[0], Right: children[1]},
		i.lastInsertId,
		sync.Once{},
	}, nil
}

// Children implements the Expression interface.
func (i *AutoIncrement) Children() []sql.Expression {
	return []sql.Expression{i.Left, i.Right}
}
