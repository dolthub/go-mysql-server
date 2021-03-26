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
	"github.com/dolthub/go-mysql-server/sql"
)

type GroupConcat struct {
	// distinct sql.Expression
	selectExprs []sql.Expression
	// TODO: Evaluate ORDER BY
	separator sql.Expression
}

var _ sql.FunctionExpression = &GroupConcat{}

func NewGroupConcat (separator sql.Expression, selectExprs ...sql.Expression) (sql.Expression, error) {
	return &GroupConcat{selectExprs: selectExprs, separator: separator}, nil
}

// NewBuffer creates a new buffer for the aggregation.
func (g *GroupConcat) NewBuffer() sql.Row {
	return sql.NewRow(int64(0))
}

// Update implements the Aggregation interface.
func (g *GroupConcat) Update(ctx *sql.Context, buffer, row sql.Row) error {
	return nil
}

// Merge implements the Aggregation interface.
func (g *GroupConcat) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	buffer[0] = buffer[0].(int64) + partial[0].(int64)
	return nil
}

func (g *GroupConcat) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {

	return nil, nil
}

func (g *GroupConcat) Resolved() bool {
	//for _, expr := range g.selectExprs {
	//	if !expr.Resolved() {
	//		return false
	//	}
	//}

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
	return nil
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

