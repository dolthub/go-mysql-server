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
	"strings"
)

type GroupConcat struct {
	// distinct sql.Expression
	selectExprs []sql.Expression
	// TODO: Evaluate ORDER BY
	separator sql.Expression
}

var _ sql.FunctionExpression = &GroupConcat{}
var _ sql.Aggregation = &GroupConcat{}

func NewGroupConcat(separator sql.Expression, selectExprs ...sql.Expression) (sql.Expression, error) {
	return &GroupConcat{selectExprs: selectExprs, separator: separator}, nil
}

// NewBuffer creates a new buffer for the aggregation.
func (g *GroupConcat) NewBuffer() sql.Row {
	var values []string = nil
	const nulls = false

	return sql.NewRow(values, nulls)
}

// Update implements the Aggregation interface.
func (g *GroupConcat) Update(ctx *sql.Context, buffer, row sql.Row) error {
	// row, err := evalExprs(ctx, g.selectExprs, row)
	if buffer[1].(bool) {
		return nil
	}

	v, err := sql.LongText.Convert(row[0])
	if err != nil {
		return err
	}

	// Get the value as string and append
	vs := v.(string)
	ba := buffer[0].([]string)

	ba = append(ba, vs)

	buffer[0] = ba

	return nil
}

// Merge implements the Aggregation interface.
func (g *GroupConcat) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	return g.Update(ctx, buffer, partial)
}

func (g *GroupConcat) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	rs := row[0].([]string)
	ret := fmt.Sprintf(strings.Join(rs[:], ","))

	return ret, nil
}

// TODO: Can this return more than one row.
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
	return g.selectExprs
}

// TODO: Reevaluate this when order by arises
func (g *GroupConcat) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) == 0 || len(children) > 4 {
		return nil, sql.ErrInvalidChildrenNumber.New(g, len(children), 3)
	}

	return NewGroupConcat(children[0], children[1:]...)
}

func (g *GroupConcat) FunctionName() string {
	return "group_concat"
}

