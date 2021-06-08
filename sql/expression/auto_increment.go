// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

var (
	// ErrAutoIncrementUnsupported is returned when table does not support AUTO_INCREMENT.
	ErrAutoIncrementUnsupported = errors.NewKind("table %s does not support AUTO_INCREMENT columns")
	// ErrNoAutoIncrementCols is returned when table has no AUTO_INCREMENT columns.
	ErrNoAutoIncrementCols = errors.NewKind("table %s has no AUTO_INCREMENT columns")
)

// AutoIncrement implements AUTO_INCREMENT
type AutoIncrement struct {
	UnaryExpression
	autoIncVal *Literal
	autoTbl    sql.AutoIncrementTable
	autoCol    *sql.Column
}

// NewAutoIncrement creates a new AutoIncrement expression.
func NewAutoIncrement(ctx *sql.Context, table sql.Table, given sql.Expression) (*AutoIncrement, error) {
	autoTbl, ok := table.(sql.AutoIncrementTable)
	if !ok {
		return nil, ErrAutoIncrementUnsupported.New(table.Name())
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
	if autoCol == nil {
		return nil, ErrNoAutoIncrementCols.New(table.Name())
	}

	return &AutoIncrement{
		UnaryExpression{Child: given},
		&Literal{last, autoCol.Type},
		autoTbl,
		autoCol,
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

	if given != nil && cmp != 0 {
		// check if the given value is greater than autoIncVal
		cmp, err := i.Type().Compare(given, i.autoIncVal.value)
		if err != nil {
			return nil, err
		}
		if cmp <= 0 {
			// if it's less, return it and don't increment
			return given, nil
		}
		i.autoIncVal = NewLiteral(given, i.Type())
	}

	val, err := i.autoIncVal.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	nextVal, err := NewIncrement(i.autoIncVal).Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	i.autoIncVal = NewLiteral(nextVal, i.Type())

	return val, nil
}

func (i *AutoIncrement) String() string {
	return fmt.Sprintf("AutoIncrement(%s)", i.Child.String())
}

// WithChildren implements the Expression interface.
func (i *AutoIncrement) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	return &AutoIncrement{
		UnaryExpression{Child: children[0]},
		i.autoIncVal,
		i.autoTbl,
		i.autoCol,
	}, nil
}

// Children implements the Expression interface.
func (i *AutoIncrement) Children() []sql.Expression {
	return []sql.Expression{i.Child}
}
