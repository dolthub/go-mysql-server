// Copyright 2021 Dolthub, Inc.
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

package aggregation

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"strings"
)

type GroupConcat struct {
	distinct sql.Expression
	orderBy []sql.Expression
	separator sql.Expression
	selectExprs []sql.Expression
}

var _ sql.FunctionExpression = &GroupConcat{}
var _ sql.Aggregation = &GroupConcat{}

func NewGroupConcat(distinct sql.Expression, orderBy []sql.Expression, separator sql.Expression, selectExprs []sql.Expression) (sql.Expression, error) {
	return &GroupConcat{distinct: distinct, orderBy: orderBy, separator: separator, selectExprs: selectExprs}, nil
}

// NewBuffer creates a new buffer for the aggregation.
func (g *GroupConcat) NewBuffer() sql.Row {
	var values []string
	var distinctSet = make(map[string]bool)
	const nulls = false

	return sql.NewRow(values, distinctSet, nulls)
}

// Update implements the Aggregation interface.
func (g *GroupConcat) Update(ctx *sql.Context, buffer, row sql.Row) error {
	row, err := evalExprs(ctx, g.selectExprs, row)

	// Skip if this is a null row
	if buffer[2].(bool) {
		return nil
	}

	// The length of the row should exceed 1.
	if len(row) > 1 {
		// TODO: Switch to mysql.EROperandColumns
		return fmt.Errorf("Operand should contain 1 column")
	}

	// Get the distinct keyword
	dv, err := g.distinct.Eval(ctx, row)
	if err != nil {
		return err
	}
	distinct := dv.(string)

	// Get the current value as a string
	v, err := sql.LongText.Convert(row[0])
	if err != nil {
		return err
	}

	vs := v.(string)

	// Get the current array of values and the map
	values := buffer[0].([]string)
	distinctSet := buffer[1].(map[string]bool)

	// Check if distinct is active if so look at and update our map
	if distinct != "" {
		// If this value exists go ahead and return nil
		if _, ok := distinctSet[vs]; ok {
			return nil
		} else {
			distinctSet[vs] = true
		}
	}

	values = append(values, vs)

	buffer[0] = values
	buffer[1] = distinctSet

	return nil
}

// Merge implements the Aggregation interface.
func (g *GroupConcat) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	return g.Update(ctx, buffer, partial)
}

// TODO: Reevaluate what's going with the return types
func (g *GroupConcat) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	rs := row[0].([]string)

	ret := fmt.Sprintf(strings.Join(rs[:], ","))

	return ret, nil
}

func evalExprs(ctx *sql.Context, exprs []sql.Expression, row sql.Row) (sql.Row, error) {
	result := make(sql.Row, len(exprs))
	for i, expr := range exprs {
		var err error
		result[i], err = expr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (g *GroupConcat) Resolved() bool {
	for _, se := range g.selectExprs {
		if !se.Resolved() {
			return false
		}
	}

	return true
}

func (g *GroupConcat) String() string {
	return "GROUP_CONCAT()" // TODO: Make this complete
}

func (g *GroupConcat) Type() sql.Type {
	return sql.LongText
}

func (g *GroupConcat) IsNullable() bool {
	return false
}

func (g *GroupConcat) Children() []sql.Expression {
	arr := make([]sql.Expression, 1)
	arr[0] = g.distinct
	arr = append(arr, g.orderBy...)
	arr = append(arr, g.separator)
	return append(arr, g.selectExprs...)
}

func (g *GroupConcat) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return GroupConcatToChildren(children...)
}

func GroupConcatToChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) == 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(GroupConcat{}, len(children), 2)
	}

	distinct := children[0]
	var orderByExpr []sql.Expression
	var separator sql.Expression

	var counter int
Loop:
	for i := 1; i < len(children); i++ {
		expr := children[i]
		switch expr.(type) {
		case *expression.Literal:
			// hit the separator case
			separator = expr
			counter = i
			break Loop
		default:
			orderByExpr = append(orderByExpr, expr)
		}
	}

	var selectExprs []sql.Expression
	if counter < len(children) {
		selectExprs = children[counter+1:]
	}

	return NewGroupConcat(distinct, orderByExpr, separator, selectExprs)
}

func (g *GroupConcat) FunctionName() string {
	return "group_concat"
}

