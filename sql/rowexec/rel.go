// Copyright 2023 Dolthub, Inc.
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

package rowexec

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/dolthub/jsonpath"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func (b *BaseBuilder) buildTopN(ctx *sql.Context, n *plan.TopN, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.TopN")
	i, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		span.End()
		return nil, err
	}

	limit, err := getInt64Value(ctx, n.Limit)
	if err != nil {
		return nil, err
	}
	return sql.NewSpanIter(span, newTopRowsIter(n.Fields, limit, n.CalcFoundRows, i, len(n.Child.Schema()))), nil
}

func (b *BaseBuilder) buildValueDerivedTable(ctx *sql.Context, n *plan.ValueDerivedTable, row sql.Row) (sql.RowIter, error) {
	rows := make([]sql.Row, len(n.ExpressionTuples))
	for i, et := range n.ExpressionTuples {
		vals := make([]interface{}, len(et))
		for j, e := range et {
			var err error
			p, err := e.Eval(ctx, row)
			if err != nil {
				return nil, err
			}
			// cast all row values to the most permissive type
			vals[j], _, err = n.Schema()[j].Type.Convert(p)
			if err != nil {
				return nil, err
			}
			// decimalType.Convert() does not use the given type precision and scale information
			if t, ok := n.Schema()[j].Type.(sql.DecimalType); ok {
				vals[j] = vals[j].(decimal.Decimal).Round(int32(t.Scale()))
			}
		}

		rows[i] = sql.NewRow(vals...)
	}

	return sql.RowsToRowIter(rows...), nil
}

func (b *BaseBuilder) buildValues(ctx *sql.Context, n *plan.Values, row sql.Row) (sql.RowIter, error) {
	rows := make([]sql.Row, len(n.ExpressionTuples))
	for i, et := range n.ExpressionTuples {
		vals := make([]interface{}, len(et))
		for j, e := range et {
			var err error
			vals[j], err = e.Eval(ctx, row)
			if err != nil {
				return nil, err
			}
		}

		rows[i] = sql.NewRow(vals...)
	}

	return sql.RowsToRowIter(rows...), nil
}

func (b *BaseBuilder) buildWindow(ctx *sql.Context, n *plan.Window, row sql.Row) (sql.RowIter, error) {
	childIter, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		return nil, err
	}
	blockIters, outputOrdinals, err := windowToIter(n)
	if err != nil {
		return nil, err
	}
	return aggregation.NewWindowIter(blockIters, outputOrdinals, childIter), nil
}

func (b *BaseBuilder) buildOffset(ctx *sql.Context, n *plan.Offset, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Offset", trace.WithAttributes(attribute.Stringer("offset", n.Offset)))

	offset, err := getInt64Value(ctx, n.Offset)
	if err != nil {
		span.End()
		return nil, err
	}

	it, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		span.End()
		return nil, err
	}
	return sql.NewSpanIter(span, &offsetIter{offset, it}), nil
}

func (b *BaseBuilder) buildJSONTable(ctx *sql.Context, n *plan.JSONTable, row sql.Row) (sql.RowIter, error) {
	// data must evaluate to JSON string
	data, err := n.DataExpr.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	strData, _, err := types.LongBlob.Convert(data)
	if err != nil {
		return nil, fmt.Errorf("invalid data type for JSON data in argument 1 to function json_table; a JSON string or JSON type is required")
	}

	if strData == nil {
		return &jsonTableRowIter{}, nil
	}

	var jsonData interface{}
	if err := json.Unmarshal(strData.([]byte), &jsonData); err != nil {
		return nil, err
	}

	// Get data specified from initial path
	var jsonPathData []interface{}
	if rootJSONData, err := jsonpath.JsonPathLookup(jsonData, n.Path); err == nil {
		if data, ok := rootJSONData.([]interface{}); ok {
			jsonPathData = data
		} else {
			jsonPathData = []interface{}{rootJSONData}
		}
	} else {
		return nil, err
	}

	return &jsonTableRowIter{
		colPaths: n.ColPaths,
		schema:   n.Schema(),
		data:     jsonPathData,
	}, nil
}

func (b *BaseBuilder) buildHashLookup(ctx *sql.Context, n *plan.HashLookup, row sql.Row) (sql.RowIter, error) {
	n.Mutex.Lock()
	defer n.Mutex.Unlock()
	if n.Lookup == nil {
		// Instead of building the mapping inline here with a special
		// RowIter, we currently make use of CachedResults and require
		// *CachedResults to be our direct child.
		cr := n.UnaryNode.Child.(*plan.CachedResults)
		if res := cr.GetCachedResults(); res != nil {
			n.Lookup = make(map[interface{}][]sql.Row)
			for _, row := range res {
				// TODO: Maybe do not put nil stuff in here.
				key, err := n.GetHashKey(ctx, n.RightEntryKey, row)
				if err != nil {
					return nil, err
				}
				n.Lookup[key] = append(n.Lookup[key], row)
			}
			// CachedResult is safe to Dispose after contents are transferred
			// to |n.lookup|
			cr.Dispose()
		}
	}
	if n.Lookup != nil {
		key, err := n.GetHashKey(ctx, n.LeftProbeKey, row)
		if err != nil {
			return nil, err
		}
		return sql.RowsToRowIter(n.Lookup[key]...), nil
	}
	return b.buildNodeExec(ctx, n.Child, row)
}

func (b *BaseBuilder) buildTableAlias(ctx *sql.Context, n *plan.TableAlias, row sql.Row) (sql.RowIter, error) {
	var table string
	if tbl, ok := n.Child.(sql.Nameable); ok {
		table = tbl.Name()
	} else {
		table = reflect.TypeOf(n.Child).String()
	}

	span, ctx := ctx.Span("sql.TableAlias", trace.WithAttributes(attribute.String("table", table)))

	iter, err := b.Build(ctx, n.Child, row)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

func (b *BaseBuilder) buildJoinNode(ctx *sql.Context, n *plan.JoinNode, row sql.Row) (sql.RowIter, error) {
	switch {
	case n.Op.IsFullOuter():
		return newFullJoinIter(ctx, b, n, row)
	case n.Op.IsPartial():
		return newExistsIter(ctx, b, n, row)
	case n.Op.IsCross():
		return newCrossJoinIter(ctx, b, n, row)
	case n.Op.IsPlaceholder():
		panic(fmt.Sprintf("%s is a placeholder, RowIter called", n.Op))
	case n.Op.IsMerge():
		return newMergeJoinIter(ctx, b, n, row)
	default:
		return newJoinIter(ctx, b, n, row)
	}
}

func (b *BaseBuilder) buildOrderedDistinct(ctx *sql.Context, n *plan.OrderedDistinct, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.OrderedDistinct")

	it, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, newOrderedDistinctIter(it, n.Child.Schema())), nil
}

func (b *BaseBuilder) buildWith(ctx *sql.Context, n *plan.With, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("*plan.With has not execution iterator")
}

func (b *BaseBuilder) buildProject(ctx *sql.Context, n *plan.Project, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Project", trace.WithAttributes(
		attribute.Int("projections", len(n.Projections)),
	))

	i, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, &projectIter{
		p:         n.Projections,
		childIter: i,
	}), nil
}

func (b *BaseBuilder) buildProcedure(ctx *sql.Context, n *plan.Procedure, row sql.Row) (sql.RowIter, error) {
	return b.buildNodeExec(ctx, n.Body, row)
}

func (b *BaseBuilder) buildRecursiveTable(ctx *sql.Context, n *plan.RecursiveTable, row sql.Row) (sql.RowIter, error) {
	return &recursiveTableIter{buf: n.Buf}, nil
}

func (b *BaseBuilder) buildSet(ctx *sql.Context, n *plan.Set, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Set")
	defer span.End()

	var updateExprs []sql.Expression
	for _, v := range n.Exprs {
		setField, ok := v.(*expression.SetField)
		if !ok {
			return nil, fmt.Errorf("unsupported type for set: %T", v)
		}

		switch left := setField.Left.(type) {
		case *expression.SystemVar:
			err := setSystemVar(ctx, left, setField.Right, row)
			if err != nil {
				return nil, err
			}
		case *expression.UserVar:
			err := setUserVar(ctx, left, setField.Right, row)
			if err != nil {
				return nil, err
			}
		case *expression.ProcedureParam:
			value, err := setField.Right.Eval(ctx, row)
			if err != nil {
				return nil, err
			}
			err = left.Set(value, setField.Right.Type())
			if err != nil {
				return nil, err
			}
		case *expression.GetField:
			updateExprs = append(updateExprs, setField)
		default:
			return nil, fmt.Errorf("unsupported type for set: %T", left)
		}
	}

	var resultRow sql.Row
	if len(updateExprs) > 0 {
		newRow, err := applyUpdateExpressions(ctx, updateExprs, row)
		if err != nil {
			return nil, err
		}
		copy(resultRow, row)
		resultRow = row.Append(newRow)
	}

	return sql.RowsToRowIter(resultRow), nil
}

func (b *BaseBuilder) buildGroupBy(ctx *sql.Context, n *plan.GroupBy, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.GroupBy", trace.WithAttributes(
		attribute.Int("groupings", len(n.GroupByExprs)),
		attribute.Int("aggregates", len(n.SelectedExprs)),
	))

	i, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		span.End()
		return nil, err
	}

	var iter sql.RowIter
	if len(n.GroupByExprs) == 0 {
		iter = newGroupByIter(n.SelectedExprs, i)
	} else {
		iter = newGroupByGroupingIter(ctx, n.SelectedExprs, n.GroupByExprs, i)
	}

	return sql.NewSpanIter(span, iter), nil
}

func (b *BaseBuilder) buildFilter(ctx *sql.Context, n *plan.Filter, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Filter")

	i, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, plan.NewFilterIter(n.Expression, i)), nil
}

func (b *BaseBuilder) buildDeclareVariables(ctx *sql.Context, n *plan.DeclareVariables, row sql.Row) (sql.RowIter, error) {
	return &declareVariablesIter{n, row}, nil
}

func (b *BaseBuilder) buildDeclareHandler(ctx *sql.Context, n *plan.DeclareHandler, row sql.Row) (sql.RowIter, error) {
	return &declareHandlerIter{n}, nil
}

func (b *BaseBuilder) buildRecursiveCte(ctx *sql.Context, n *plan.RecursiveCte, row sql.Row) (sql.RowIter, error) {
	var iter sql.RowIter = &recursiveCteIter{
		init:        n.Left(),
		rec:         n.Right(),
		row:         row,
		working:     n.Working,
		temp:        make([]sql.Row, 0),
		deduplicate: n.Union().Distinct,
		b:           b,
	}
	if n.Union().Limit != nil && len(n.Union().SortFields) > 0 {
		limit, err := getInt64Value(ctx, n.Union().Limit)
		if err != nil {
			return nil, err
		}
		iter = newTopRowsIter(n.Union().SortFields, limit, false, iter, len(n.Union().Schema()))
	} else if n.Union().Limit != nil {
		limit, err := getInt64Value(ctx, n.Union().Limit)
		if err != nil {
			return nil, err
		}
		iter = &limitIter{limit: limit, childIter: iter}
	} else if len(n.Union().SortFields) > 0 {
		iter = newSortIter(n.Union().SortFields, iter)
	}
	return iter, nil
}

func (b *BaseBuilder) buildLimit(ctx *sql.Context, n *plan.Limit, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Limit", trace.WithAttributes(attribute.Stringer("limit", n.Limit)))

	limit, err := getInt64Value(ctx, n.Limit)
	if err != nil {
		span.End()
		return nil, err
	}

	childIter, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		span.End()
		return nil, err
	}
	return sql.NewSpanIter(span, &limitIter{
		calcFoundRows: n.CalcFoundRows,
		limit:         limit,
		childIter:     childIter,
	}), nil
}

func (b *BaseBuilder) buildMax1Row(ctx *sql.Context, n *plan.Max1Row, row sql.Row) (sql.RowIter, error) {
	n.Mu.Lock()
	defer n.Mu.Unlock()

	if !n.HasResults() {
		err := b.populateMax1Results(ctx, n, row)
		if err != nil {
			return nil, err
		}
	}

	switch {
	case n.EmptyResult:
		return plan.EmptyIter, nil
	case n.Result != nil:
		return sql.RowsToRowIter(n.Result), nil
	default:
		return nil, fmt.Errorf("Max1Row failed to load results")
	}
}

// PopulateResults loads and stores the state of its child iter:
// 1) no rows returned, 2) 1 row returned, or 3) more than 1 row
// returned
func (b *BaseBuilder) populateMax1Results(ctx *sql.Context, n *plan.Max1Row, row sql.Row) error {
	i, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		return err
	}
	r1, err := i.Next(ctx)
	if errors.Is(err, io.EOF) {
		n.EmptyResult = true
		return nil
	} else if err != nil {
		return err
	}

	_, err = i.Next(ctx)
	if err == nil {
		return sql.ErrExpectedSingleRow.New()
	} else if !errors.Is(err, io.EOF) {
		return err
	}
	n.Result = r1
	return nil
}

func (b *BaseBuilder) buildInto(ctx *sql.Context, n *plan.Into, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Into")
	defer span.End()

	rowIter, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		return nil, err
	}
	rows, err := sql.RowIterToRows(ctx, nil, rowIter)
	if err != nil {
		return nil, err
	}

	rowNum := len(rows)
	if rowNum > 1 {
		return nil, sql.ErrMoreThanOneRow.New()
	}
	if rowNum == 0 {
		// a warning with error code 1329 occurs (No data), and make no change to variables
		return sql.RowsToRowIter(sql.Row{}), nil
	}
	if len(rows[0]) != len(n.IntoVars) {
		return nil, sql.ErrColumnNumberDoesNotMatch.New()
	}

	var rowValues = make([]interface{}, len(rows[0]))

	for j, val := range rows[0] {
		rowValues[j] = val
	}

	for j, v := range n.IntoVars {
		switch variable := v.(type) {
		case *expression.UserVar:
			varType := types.ApproximateTypeFromValue(rowValues[j])
			err = ctx.SetUserVariable(ctx, variable.Name, rowValues[j], varType)
			if err != nil {
				return nil, err
			}
		case *expression.ProcedureParam:
			err = variable.Set(rowValues[j], types.ApproximateTypeFromValue(rowValues[j]))
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsupported type for into: %T", variable)
		}
	}

	return sql.RowsToRowIter(sql.Row{}), nil
}

func (b *BaseBuilder) buildExternalProcedure(ctx *sql.Context, n *plan.ExternalProcedure, row sql.Row) (sql.RowIter, error) {
	// The function's structure has been verified by the analyzer, so no need to double-check any of it here
	funcVal := reflect.ValueOf(n.Function)
	funcType := funcVal.Type()
	// The first parameter is always the context, but it doesn't exist as far as the stored procedures are concerned, so
	// we prepend it here
	funcParams := make([]reflect.Value, len(n.Params)+1)
	funcParams[0] = reflect.ValueOf(ctx)

	for i := range n.Params {
		paramDefinition := n.ParamDefinitions[i]
		var funcParamType reflect.Type
		if paramDefinition.Variadic {
			funcParamType = funcType.In(funcType.NumIn() - 1).Elem()
		} else {
			funcParamType = funcType.In(i + 1)
		}
		// Grab the passed-in variable and convert it to the type we expect
		exprParamVal, err := n.Params[i].Eval(ctx, nil)
		if err != nil {
			return nil, err
		}
		exprParamVal, _, err = paramDefinition.Type.Convert(exprParamVal)
		if err != nil {
			return nil, err
		}

		funcParams[i+1], err = n.ProcessParam(ctx, funcParamType, exprParamVal)
		if err != nil {
			return nil, err
		}
	}
	out := funcVal.Call(funcParams)

	// Again, these types are enforced in the analyzer, so it's safe to assume their types here
	if err, ok := out[1].Interface().(error); ok { // Only evaluates to true when error is not nil
		return nil, err
	}
	for i, paramDefinition := range n.ParamDefinitions {
		if paramDefinition.Direction == plan.ProcedureParamDirection_Inout || paramDefinition.Direction == plan.ProcedureParamDirection_Out {
			exprParam := n.Params[i]
			funcParamVal := funcParams[i+1].Elem().Interface()
			err := exprParam.Set(funcParamVal, exprParam.Type())
			if err != nil {
				return nil, err
			}
		}
	}
	// It's not invalid to return a nil RowIter, as having no rows to return is expected of many stored procedures.
	if rowIter, ok := out[0].Interface().(sql.RowIter); ok {
		return rowIter, nil
	}
	return sql.RowsToRowIter(), nil
}

func (b *BaseBuilder) buildHaving(ctx *sql.Context, n *plan.Having, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Having")
	iter, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, plan.NewFilterIter(n.Cond, iter)), nil
}

func (b *BaseBuilder) buildDistinct(ctx *sql.Context, n *plan.Distinct, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Distinct")

	it, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, newDistinctIter(ctx, it)), nil
}

func (b *BaseBuilder) buildIndexedTableAccess(ctx *sql.Context, n *plan.IndexedTableAccess, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.IndexedTableAccess")

	lookup, err := n.GetLookup(ctx, row)
	if err != nil {
		return nil, err
	}

	partIter, err := n.Table.LookupPartitions(ctx, lookup)
	if err != nil {
		return nil, err
	}

	return sql.NewSpanIter(span, sql.NewTableRowIter(ctx, n.Table, partIter)), nil
}

func (b *BaseBuilder) buildUnion(ctx *sql.Context, u *plan.Union, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Union")
	var iter sql.RowIter
	var err error
	iter, err = b.buildNodeExec(ctx, u.Left(), row)

	if err != nil {
		span.End()
		return nil, err
	}
	iter = &unionIter{
		cur: iter,
		nextIter: func(ctx *sql.Context) (sql.RowIter, error) {
			return b.buildNodeExec(ctx, u.Right(), row)
		},
	}
	if u.Distinct {
		iter = newDistinctIter(ctx, iter)
	}
	// Limit must wrap offset, and not vice-versa, so that
	// skipped rows don't count toward the returned row count.
	if u.Offset != nil {
		offset, err := getInt64Value(ctx, u.Offset)
		if err != nil {
			return nil, err
		}
		iter = &offsetIter{skip: offset, childIter: iter}
	}
	if u.Limit != nil && len(u.SortFields) > 0 {
		limit, err := getInt64Value(ctx, u.Limit)
		if err != nil {
			return nil, err
		}
		iter = newTopRowsIter(u.SortFields, limit, false, iter, len(u.Schema()))
	} else if u.Limit != nil {
		limit, err := getInt64Value(ctx, u.Limit)
		if err != nil {
			return nil, err
		}
		iter = &limitIter{limit: limit, childIter: iter}
	} else if len(u.SortFields) > 0 {
		iter = newSortIter(u.SortFields, iter)
	}
	return sql.NewSpanIter(span, iter), nil
}

func (b *BaseBuilder) buildSubqueryAlias(ctx *sql.Context, n *plan.SubqueryAlias, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.SubqueryAlias")

	if !n.OuterScopeVisibility {
		row = nil
	}
	iter, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, iter), nil
}

func (b *BaseBuilder) buildSort(ctx *sql.Context, n *plan.Sort, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Sort")
	i, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		span.End()
		return nil, err
	}
	return sql.NewSpanIter(span, newSortIter(n.SortFields, i)), nil
}

func (b *BaseBuilder) buildPrepareQuery(ctx *sql.Context, n *plan.PrepareQuery, row sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(sql.NewRow(types.OkResult{RowsAffected: 0, Info: plan.PrepareInfo{}})), nil
}

func (b *BaseBuilder) buildResolvedTable(ctx *sql.Context, n *plan.ResolvedTable, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.ResolvedTable")

	partitions, err := n.Table.Partitions(ctx)
	if err != nil {
		span.End()
		return nil, err
	}

	return sql.NewSpanIter(span, sql.NewTableRowIter(ctx, n.Table, partitions)), nil
}
