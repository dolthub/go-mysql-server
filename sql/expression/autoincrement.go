package expression

import (
	"errors"
	"fmt"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

// AutoIncrement implements AUTO_INCREMENT
type AutoIncrement struct {
	UnaryExpression
	lastInsertId *Literal
	autoTbl      sql.AutoIncrementTable
	autoCol      *sql.Column
	sync.Once
}

// NewAutoIncrement creates a new AutoIncrement expression.
func NewAutoIncrement(ctx *sql.Context, table sql.Table, given sql.Expression) (*AutoIncrement, error) {
	autoTbl, ok := table.(sql.AutoIncrementTable)
	if !ok {
		return nil, errors.New("this table does not support AUTO_INCREMENT columns")
	}

	last, err := autoTbl.GetAutoIncrementValue(ctx)
	if err != nil {
		return nil, err
	}

	var autoCol *sql.Column
	for _, c := range autoTbl.Schema() {
		if c.AutoIncrement {
			autoCol = c
			break
		}
	}

	return &AutoIncrement{
		UnaryExpression{Child: given},
		&Literal{last, given.Type()},
		autoTbl,
		autoCol,
		sync.Once{},
	}, nil
}

// IsNullable implements the Expression interface.
func (i *AutoIncrement) IsNullable() bool {
	return false
}

// Type implements the Expression interface.
func (i *AutoIncrement) Type() sql.Type {
	return i.autoCol.Type
}

// Eval implements the Expression interface.
func (i *AutoIncrement) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// get value provided by INSERT
	given, err := i.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// todo: |given| is int8 while |i.Right.Zero()| is int64
	cmp, err := i.Type().Compare(given, i.Type().Zero())
	if err != nil {
		return nil, err
	}

	if given == nil || cmp == 0 {
		// provide AUTO_INCREMENT value
		one := NewLiteral(sql.NumericUnaryValue(i.Type()), i.Type())
		id, err := NewPlus(i.lastInsertId, one).Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		i.lastInsertId = NewLiteral(id, i.Type())
	} else {
		// last_insert_id = max(given, last_insert_id)
		cmp, err := i.Type().Compare(given, i.lastInsertId.value)
		if err != nil {
			return nil, err
		}
		if cmp <= 0 {
			return given, nil
		}
		i.lastInsertId = NewLiteral(given, i.Type())
	}

	return i.lastInsertId.Eval(ctx, row)
}

func (i *AutoIncrement) String() string {
	return fmt.Sprintf("AutoIncrement(%s)", i.Child.String())
}

// WithChildren implements the Expression interface.
func (i *AutoIncrement) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	return &AutoIncrement{
		UnaryExpression{Child: children[0]},
		i.lastInsertId,
		i.autoTbl,
		i.autoCol,
		sync.Once{},
	}, nil
}

// Children implements the Expression interface.
func (i *AutoIncrement) Children() []sql.Expression {
	return []sql.Expression{i.Child}
}
