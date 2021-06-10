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
	"sort"
	"strings"

	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type GroupConcat struct {
	distinct    string
	sf          sql.SortFields
	separator   string
	selectExprs []sql.Expression
	maxLen      int
	returnType  sql.Type
}

var _ sql.FunctionExpression = &GroupConcat{}
var _ sql.Aggregation = &GroupConcat{}

func NewEmptyGroupConcat(ctx *sql.Context) sql.Expression {
	return &GroupConcat{}
}

func NewGroupConcat(ctx *sql.Context, distinct string, orderBy sql.SortFields, separator string, selectExprs []sql.Expression, maxLen int) (*GroupConcat, error) {
	return &GroupConcat{distinct: distinct, sf: orderBy, separator: separator, selectExprs: selectExprs, maxLen: maxLen}, nil
}

// NewBuffer creates a new buffer for the aggregation.
func (g *GroupConcat) NewBuffer() sql.Row {
	var rows []sql.Row
	var distinctSet = make(map[string]bool)

	return sql.NewRow(rows, distinctSet)
}

// Update implements the Aggregation interface.
func (g *GroupConcat) Update(ctx *sql.Context, buffer, originalRow sql.Row) error {
	evalRow, retType, err := evalExprs(ctx, g.selectExprs, originalRow)
	if err != nil {
		return err
	}

	g.returnType = retType

	// Skip if this is a null row
	if evalRow == nil {
		return nil
	}

	var v interface{}
	if retType == sql.Blob {
		v, err = sql.Blob.Convert(evalRow[0])
	} else {
		v, err = sql.LongText.Convert(evalRow[0])
	}

	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}

	vs := v.(string)

	// Get the current array of rows and the map
	rows := buffer[0].([]sql.Row)
	distinctSet := buffer[1].(map[string]bool)

	// Check if distinct is active if so look at and update our map
	if g.distinct != "" {
		// If this value exists go ahead and return nil
		if _, ok := distinctSet[vs]; ok {
			return nil
		} else {
			distinctSet[vs] = true
		}
	}

	// Append the current value to the end of the row. We want to preserve the row's original structure for
	// for sort ordering in the final step.
	rows = append(rows, append(originalRow, nil, vs))

	buffer[0] = rows
	buffer[1] = distinctSet

	return nil
}

// Merge implements the Aggregation interface.
func (g *GroupConcat) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	return g.Update(ctx, buffer, partial)
}

// cc: https://dev.mysql.com/doc/refman/8.0/en/aggregate-functions.html#function_group-concat
func (g *GroupConcat) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	rows := row[0].([]sql.Row)

	if len(rows) == 0 {
		return nil, nil
	}

	// Execute the order operation if it exists.
	if g.sf != nil {
		sorter := &expression.Sorter{
			SortFields: g.sf,
			Rows:       rows,
			Ctx:        ctx,
		}

		sort.Stable(sorter)
		if sorter.LastError != nil {
			return nil, sorter.LastError
		}
	}

	sb := strings.Builder{}
	for i, row := range rows {
		lastIdx := len(row) - 1
		if i == 0 {
			sb.WriteString(row[lastIdx].(string))
		} else {
			sb.WriteString(g.separator)
			sb.WriteString(row[lastIdx].(string))
		}

		// Don't allow the string to cross maxlen
		if sb.Len() >= g.maxLen {
			break
		}
	}

	ret := sb.String()

	// There might be a couple of character differences even if we broke early in the loop
	if len(ret) > g.maxLen {
		ret = ret[:g.maxLen]
	}

	// Add this to handle any one off errors.
	return ret, nil
}

func evalExprs(ctx *sql.Context, exprs []sql.Expression, row sql.Row) (sql.Row, sql.Type, error) {
	result := make(sql.Row, len(exprs))
	retType := sql.Blob
	for i, expr := range exprs {
		var err error
		result[i], err = expr.Eval(ctx, row)
		if err != nil {
			return nil, nil, err
		}

		// If every expression returns Blob type return Blob otherwise return Text.
		if expr.Type() != sql.Blob {
			retType = sql.Text
		}
	}

	return result, retType, nil
}

func (g *GroupConcat) Resolved() bool {
	for _, se := range g.selectExprs {
		if !se.Resolved() {
			return false
		}
	}

	sfs := g.sf.ToExpressions()

	for _, sf := range sfs {
		if !sf.Resolved() {
			return false
		}
	}

	return true
}

func (g *GroupConcat) String() string {
	sb := strings.Builder{}
	sb.WriteString("group_concat(")
	if g.distinct != "" {
		sb.WriteString(fmt.Sprintf("distinct %s", g.distinct))
	}

	if g.selectExprs != nil {
		var exprs = make([]string, len(g.selectExprs))
		for i, expr := range g.selectExprs {
			exprs[i] = expr.String()
		}

		sb.WriteString(strings.Join(exprs, ", "))
	}

	if len(g.sf) > 0 {
		sb.WriteString(" order by ")
		for i, ob := range g.sf {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(ob.String())
		}
	}

	if g.separator != "," {
		sb.WriteString(" separator ")
		sb.WriteString(fmt.Sprintf("'%s'", g.separator))
	}

	sb.WriteString(")")

	return sb.String()
}

// cc: https://dev.mysql.com/doc/refman/8.0/en/aggregate-functions.html#function_group-concat for explanations
// on return type.
func (g *GroupConcat) Type() sql.Type {
	if g.returnType == sql.Blob {
		if g.maxLen <= 512 {
			return sql.MustCreateString(query.Type_VARBINARY, 512, sql.Collation_binary)
		} else {
			return sql.Blob
		}
	} else {
		if g.maxLen <= 512 {
			return sql.MustCreateString(query.Type_VARCHAR, 512, sql.Collation_Default)
		} else {
			return sql.Text
		}
	}
}

func (g *GroupConcat) IsNullable() bool {
	return false
}

func (g *GroupConcat) Children() []sql.Expression {
	return append(g.sf.ToExpressions(), g.selectExprs...)
}

func (g *GroupConcat) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) == 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(GroupConcat{}, len(children), 2)
	}

	// Get the order by expression using the length of the sort fields.
	sortFieldMarker := len(g.sf)
	orderByExpr := children[:len(g.sf)]

	return NewGroupConcat(ctx, g.distinct, g.sf.FromExpressions(orderByExpr), g.separator, children[sortFieldMarker:], g.maxLen)
}

func (g *GroupConcat) FunctionName() string {
	return "group_concat"
}
