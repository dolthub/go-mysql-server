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
	window      *sql.WindowDefinition
}

var _ sql.FunctionExpression = &GroupConcat{}
var _ sql.Aggregation = &GroupConcat{}
var _ sql.WindowAdaptableExpression = (*GroupConcat)(nil)

func NewEmptyGroupConcat() sql.Expression {
	return &GroupConcat{}
}

// FunctionName implements sql.FunctionExpression
func (g *GroupConcat) FunctionName() string {
	return "group_concat"
}

// Description implements sql.FunctionExpression
func (g *GroupConcat) Description() string {
	return "returns a string result with the concatenated non-NULL values from a group."
}

func NewGroupConcat(distinct string, orderBy sql.SortFields, separator string, selectExprs []sql.Expression, maxLen int) (*GroupConcat, error) {
	return &GroupConcat{distinct: distinct, sf: orderBy, separator: separator, selectExprs: selectExprs, maxLen: maxLen}, nil
}

// WithWindow implements sql.Aggregation
func (g *GroupConcat) WithWindow(window *sql.WindowDefinition) (sql.Aggregation, error) {
	ng := *g
	ng.window = window
	return &ng, nil
}

// Window implements sql.Aggregation
func (g *GroupConcat) Window() *sql.WindowDefinition {
	return g.window
}

// NewBuffer creates a new buffer for the aggregation.
func (g *GroupConcat) NewBuffer() (sql.AggregationBuffer, error) {
	var rows []sql.Row
	distinctSet := make(map[string]bool)
	return &groupConcatBuffer{g, rows, distinctSet}, nil
}

// NewWindowFunctionAggregation implements sql.WindowAdaptableExpression
func (g *GroupConcat) NewWindowFunction() (sql.WindowFunction, error) {
	return NewGroupConcatAgg(g), nil
}

// Eval implements the Expression interface.
func (g *GroupConcat) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return nil, ErrEvalUnsupportedOnAggregation.New("GroupConcat")
}

// Resolved implements the Expression interface.
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

// Type implements the Expression interface.
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

// IsNullable implements the Expression interface.
func (g *GroupConcat) IsNullable() bool {
	return false
}

// Children implements the Expression interface.
func (g *GroupConcat) Children() []sql.Expression {
	return append(g.sf.ToExpressions(), g.selectExprs...)
}

// WithChildren implements the Expression interface.
func (g *GroupConcat) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) == 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(GroupConcat{}, len(children), 2)
	}

	// Get the order by expression using the length of the sort fields.
	sortFieldMarker := len(g.sf)
	orderByExpr := children[:len(g.sf)]

	return NewGroupConcat(g.distinct, g.sf.FromExpressions(orderByExpr...), g.separator, children[sortFieldMarker:], g.maxLen)
}

type groupConcatBuffer struct {
	gc          *GroupConcat
	rows        []sql.Row
	distinctSet map[string]bool
}

// Update implements the AggregationBuffer interface.
func (g *groupConcatBuffer) Update(ctx *sql.Context, originalRow sql.Row) error {
	evalRow, retType, err := evalExprs(ctx, g.gc.selectExprs, originalRow)
	if err != nil {
		return err
	}

	g.gc.returnType = retType

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
	// Check if distinct is active if so look at and update our map
	if g.gc.distinct != "" {
		// If this value exists go ahead and return nil
		if _, ok := g.distinctSet[vs]; ok {
			return nil
		} else {
			g.distinctSet[vs] = true
		}
	}

	// Append the current value to the end of the row. We want to preserve the row's original structure for
	// for sort ordering in the final step.
	g.rows = append(g.rows, append(originalRow, nil, vs))

	return nil
}

// Eval implements the AggregationBuffer interface.
// cc: https://dev.mysql.com/doc/refman/8.0/en/aggregate-functions.html#function_group-concat
func (g *groupConcatBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	rows := g.rows

	if len(rows) == 0 {
		return nil, nil
	}

	// Execute the order operation if it exists.
	if g.gc.sf != nil {
		sorter := &expression.Sorter{
			SortFields: g.gc.sf,
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
			sb.WriteString(g.gc.separator)
			sb.WriteString(row[lastIdx].(string))
		}

		// Don't allow the string to cross maxlen
		if sb.Len() >= g.gc.maxLen {
			break
		}
	}

	ret := sb.String()

	// There might be a couple of character differences even if we broke early in the loop
	if len(ret) > g.gc.maxLen {
		ret = ret[:g.gc.maxLen]
	}

	// Add this to handle any one off errors.
	return ret, nil
}

// Dispose implements the Disposable interface.
func (g *groupConcatBuffer) Dispose() {
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
