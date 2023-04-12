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
	"container/heap"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/oliveagle/jsonpath"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type topRowsIter struct {
	sortFields    sql.SortFields
	calcFoundRows bool
	childIter     sql.RowIter
	limit         int64
	topRows       []sql.Row
	numFoundRows  int64
	idx           int
}

func newTopRowsIter(s sql.SortFields, limit int64, calcFoundRows bool, child sql.RowIter, childSchemaLen int) *topRowsIter {
	return &topRowsIter{
		sortFields:    append(s, sql.SortField{Column: expression.NewGetField(childSchemaLen, types.Int64, "order", false)}),
		limit:         limit,
		calcFoundRows: calcFoundRows,
		childIter:     child,
		idx:           -1,
	}
}

func (i *topRowsIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.idx == -1 {
		err := i.computeTopRows(ctx)
		if err != nil {
			return nil, err
		}
		i.idx = 0
	}

	if i.idx >= len(i.topRows) {
		return nil, io.EOF
	}
	row := i.topRows[i.idx]
	i.idx++
	return row[:len(row)-1], nil
}

func (i *topRowsIter) Close(ctx *sql.Context) error {
	i.topRows = nil

	if i.calcFoundRows {
		ctx.SetLastQueryInfo(sql.FoundRows, i.numFoundRows)
	}

	return i.childIter.Close(ctx)
}

func (i *topRowsIter) computeTopRows(ctx *sql.Context) error {
	topRowsHeap := &expression.TopRowsHeap{
		expression.Sorter{
			SortFields: i.sortFields,
			Rows:       []sql.Row{},
			LastError:  nil,
			Ctx:        ctx,
		},
	}
	for {
		row, err := i.childIter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		i.numFoundRows++

		row = append(row, i.numFoundRows)

		heap.Push(topRowsHeap, row)
		if int64(topRowsHeap.Len()) > i.limit {
			heap.Pop(topRowsHeap)
		}
		if topRowsHeap.LastError != nil {
			return topRowsHeap.LastError
		}
	}

	var err error
	i.topRows, err = topRowsHeap.Rows()
	return err
}

// getInt64Value returns the int64 literal value in the expression given, or an error with the errStr given if it
// cannot.
func getInt64Value(ctx *sql.Context, expr sql.Expression) (int64, error) {
	i, err := expr.Eval(ctx, nil)
	if err != nil {
		return 0, err
	}

	switch i := i.(type) {
	case int:
		return int64(i), nil
	case int8:
		return int64(i), nil
	case int16:
		return int64(i), nil
	case int32:
		return int64(i), nil
	case int64:
		return i, nil
	case uint:
		return int64(i), nil
	case uint8:
		return int64(i), nil
	case uint16:
		return int64(i), nil
	case uint32:
		return int64(i), nil
	case uint64:
		return int64(i), nil
	default:
		// analyzer should catch this already
		panic(fmt.Sprintf("Unsupported type for limit %T", i))
	}
}

// windowToIter transforms a plan.Window into a series
// of aggregation.WindowPartitionIter and a list of output projection indexes
// for each window partition.
// TODO: make partition ordering deterministic
func windowToIter(w *plan.Window) ([]*aggregation.WindowPartitionIter, [][]int, error) {
	partIdToOutputIdxs := make(map[uint64][]int, 0)
	partIdToBlock := make(map[uint64]*aggregation.WindowPartition, 0)
	var window *sql.WindowDefinition
	var agg *aggregation.Aggregation
	var fn sql.WindowFunction
	var err error
	// collect functions in hash map keyed by partitioning scheme
	for i, expr := range w.SelectExprs {
		if alias, ok := expr.(*expression.Alias); ok {
			expr = alias.Child
		}
		switch e := expr.(type) {
		case sql.Aggregation:
			window = e.Window()
			fn, err = e.NewWindowFunction()
		case sql.WindowAggregation:
			window = e.Window()
			fn, err = e.NewWindowFunction()
		default:
			// non window aggregates resolve to LastAgg with empty over clause
			window = sql.NewWindowDefinition(nil, nil, nil, "", "")
			fn, err = aggregation.NewLast(e).NewWindowFunction()
		}
		if err != nil {
			return nil, nil, err
		}
		agg = aggregation.NewAggregation(fn, fn.DefaultFramer())

		id, err := window.PartitionId()
		if err != nil {
			return nil, nil, err
		}

		if block, ok := partIdToBlock[id]; !ok {
			if err != nil {
				return nil, nil, err
			}
			partIdToBlock[id] = aggregation.NewWindowPartition(
				window.PartitionBy,
				window.OrderBy,
				[]*aggregation.Aggregation{agg},
			)
			partIdToOutputIdxs[id] = []int{i}
		} else {
			block.AddAggregation(agg)
			partIdToOutputIdxs[id] = append(partIdToOutputIdxs[id], i)
		}
	}

	// convert partition hash map into list
	blockIters := make([]*aggregation.WindowPartitionIter, len(partIdToBlock))
	outputOrdinals := make([][]int, len(partIdToBlock))
	i := 0
	for id, block := range partIdToBlock {
		outputIdx := partIdToOutputIdxs[id]
		blockIters[i] = aggregation.NewWindowPartitionIter(block)
		outputOrdinals[i] = outputIdx
		i++
	}
	return blockIters, outputOrdinals, nil
}

type offsetIter struct {
	skip      int64
	childIter sql.RowIter
}

func (i *offsetIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.skip > 0 {
		for i.skip > 0 {
			_, err := i.childIter.Next(ctx)
			if err != nil {
				return nil, err
			}
			i.skip--
		}
	}

	row, err := i.childIter.Next(ctx)
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (i *offsetIter) Close(ctx *sql.Context) error {
	return i.childIter.Close(ctx)
}

type jsonTableRowIter struct {
	colPaths []string
	schema   sql.Schema
	data     []interface{}
	pos      int
}

var _ sql.RowIter = &jsonTableRowIter{}

func (j *jsonTableRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if j.pos >= len(j.data) {
		return nil, io.EOF
	}
	obj := j.data[j.pos]
	j.pos++

	row := make(sql.Row, len(j.colPaths))
	for i, p := range j.colPaths {
		var val interface{}
		if v, err := jsonpath.JsonPathLookup(obj, p); err == nil {
			val = v
		}

		value, err := j.schema[i].Type.Convert(val)
		if err != nil {
			return nil, err
		}

		row[i] = value
	}

	return row, nil
}

func (j *jsonTableRowIter) Close(ctx *sql.Context) error {
	return nil
}

// orderedDistinctIter iterates the children iterator and skips all the
// repeated rows assuming the iterator has all rows sorted.
type orderedDistinctIter struct {
	childIter sql.RowIter
	schema    sql.Schema
	prevRow   sql.Row
}

func newOrderedDistinctIter(child sql.RowIter, schema sql.Schema) *orderedDistinctIter {
	return &orderedDistinctIter{childIter: child, schema: schema}
}

func (di *orderedDistinctIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		row, err := di.childIter.Next(ctx)
		if err != nil {
			return nil, err
		}

		if di.prevRow != nil {
			ok, err := di.prevRow.Equals(row, di.schema)
			if err != nil {
				return nil, err
			}

			if ok {
				continue
			}
		}

		di.prevRow = row
		return row, nil
	}
}

func (di *orderedDistinctIter) Close(ctx *sql.Context) error {
	return di.childIter.Close(ctx)
}

type projectIter struct {
	p         []sql.Expression
	childIter sql.RowIter
}

func (i *projectIter) Next(ctx *sql.Context) (sql.Row, error) {
	childRow, err := i.childIter.Next(ctx)
	if err != nil {
		return nil, err
	}

	return ProjectRow(ctx, i.p, childRow)
}

func (i *projectIter) Close(ctx *sql.Context) error {
	return i.childIter.Close(ctx)
}

// ProjectRow evaluates a set of projections.
func ProjectRow(
	ctx *sql.Context,
	projections []sql.Expression,
	row sql.Row,
) (sql.Row, error) {
	var err error
	var secondPass []int
	var fields sql.Row
	for i, expr := range projections {
		// Default values that are expressions may reference other fields, thus they must evaluate after all other exprs.
		// Also default expressions may not refer to other columns that come after them if they also have a default expr.
		// This ensures that all columns referenced by expressions will have already been evaluated.
		// Since literals do not reference other columns, they're evaluated on the first pass.
		if defaultVal, ok := expr.(*sql.ColumnDefaultValue); ok && !defaultVal.IsLiteral() {
			fields = append(fields, nil)
			secondPass = append(secondPass, i)
			continue
		}
		f, fErr := expr.Eval(ctx, row)
		if fErr != nil {
			return nil, fErr
		}
		fields = append(fields, f)
	}
	for _, index := range secondPass {
		fields[index], err = projections[index].Eval(ctx, fields)
		if err != nil {
			return nil, err
		}
	}
	return sql.NewRow(fields...), nil
}

// TODO a queue is probably more optimal
type recursiveTableIter struct {
	pos int
	buf []sql.Row
}

var _ sql.RowIter = (*recursiveTableIter)(nil)

func (r *recursiveTableIter) Next(ctx *sql.Context) (sql.Row, error) {
	if r.buf == nil || r.pos >= len(r.buf) {
		return nil, io.EOF
	}
	r.pos++
	return r.buf[r.pos-1], nil
}

func (r *recursiveTableIter) Close(ctx *sql.Context) error {
	r.buf = nil
	return nil
}

func setUserVar(ctx *sql.Context, userVar *expression.UserVar, right sql.Expression, row sql.Row) error {
	val, err := right.Eval(ctx, row)
	if err != nil {
		return err
	}
	typ := types.ApproximateTypeFromValue(val)

	err = ctx.SetUserVariable(ctx, userVar.Name, val, typ)
	if err != nil {
		return err
	}
	return nil
}

func setSystemVar(ctx *sql.Context, sysVar *expression.SystemVar, right sql.Expression, row sql.Row) error {
	val, err := right.Eval(ctx, row)
	if err != nil {
		return err
	}
	switch sysVar.Scope {
	case sql.SystemVariableScope_Global:
		err = sql.SystemVariables.SetGlobal(sysVar.Name, val)
		if err != nil {
			return err
		}
	case sql.SystemVariableScope_Session:
		err = ctx.SetSessionVariable(ctx, sysVar.Name, val)
		if err != nil {
			return err
		}
	case sql.SystemVariableScope_Persist:
		persistSess, ok := ctx.Session.(sql.PersistableSession)
		if !ok {
			return sql.ErrSessionDoesNotSupportPersistence.New()
		}
		err = persistSess.PersistGlobal(sysVar.Name, val)
		if err != nil {
			return err
		}
		err = sql.SystemVariables.SetGlobal(sysVar.Name, val)
		if err != nil {
			return err
		}
	case sql.SystemVariableScope_PersistOnly:
		persistSess, ok := ctx.Session.(sql.PersistableSession)
		if !ok {
			return sql.ErrSessionDoesNotSupportPersistence.New()
		}
		err = persistSess.PersistGlobal(sysVar.Name, val)
		if err != nil {
			return err
		}
	case sql.SystemVariableScope_ResetPersist:
		// TODO: add parser support for RESET PERSIST
		persistSess, ok := ctx.Session.(sql.PersistableSession)
		if !ok {
			return sql.ErrSessionDoesNotSupportPersistence.New()
		}
		if sysVar.Name == "" {
			err = persistSess.RemoveAllPersistedGlobals()
		}
		err = persistSess.RemovePersistedGlobal(sysVar.Name)
		if err != nil {
			return err
		}
	default: // should never be hit
		return fmt.Errorf("unable to set `%s` due to unknown scope `%v`", sysVar.Name, sysVar.Scope)
	}
	// Setting `character_set_connection`, regardless of how it is set (directly or through SET NAMES) will also set
	// `collation_connection` to the default collation for the given character set.
	if strings.ToLower(sysVar.Name) == "character_set_connection" {
		newSysVar := &expression.SystemVar{
			Name:  "collation_connection",
			Scope: sysVar.Scope,
		}
		if val == nil {
			err = setSystemVar(ctx, newSysVar, expression.NewLiteral("", types.LongText), row)
			if err != nil {
				return err
			}
		} else {
			valStr, ok := val.(string)
			if !ok {
				return sql.ErrInvalidSystemVariableValue.New("collation_connection", val)
			}
			charset, err := sql.ParseCharacterSet(valStr)
			if err != nil {
				return err
			}
			charset = charset
			err = setSystemVar(ctx, newSysVar, expression.NewLiteral(charset.DefaultCollation().Name(), types.LongText), row)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Applies the update expressions given to the row given, returning the new resultant row.
func applyUpdateExpressions(ctx *sql.Context, updateExprs []sql.Expression, row sql.Row) (sql.Row, error) {
	var ok bool
	prev := row
	for _, updateExpr := range updateExprs {
		val, err := updateExpr.Eval(ctx, prev)
		if err != nil {
			return nil, err
		}
		prev, ok = val.(sql.Row)
		if !ok {
			return nil, plan.ErrUpdateUnexpectedSetResult.New(val)
		}
	}
	return prev, nil
}

// declareVariablesIter is the sql.RowIter of *DeclareVariables.
type declareVariablesIter struct {
	*plan.DeclareVariables
	row sql.Row
}

var _ sql.RowIter = (*declareVariablesIter)(nil)

// Next implements the interface sql.RowIter.
func (d *declareVariablesIter) Next(ctx *sql.Context) (sql.Row, error) {
	defaultVal, err := d.DefaultVal.Eval(ctx, d.row)
	if err != nil {
		return nil, err
	}
	for _, varName := range d.Names {
		if err := d.Pref.InitializeVariable(varName, d.Type, defaultVal); err != nil {
			return nil, err
		}
	}
	return nil, io.EOF
}

// Close implements the interface sql.RowIter.
func (d *declareVariablesIter) Close(ctx *sql.Context) error {
	return nil
}

// declareHandlerIter is the sql.RowIter of *DeclareHandler.
type declareHandlerIter struct {
	*plan.DeclareHandler
}

var _ sql.RowIter = (*declareHandlerIter)(nil)

// Next implements the interface sql.RowIter.
func (d *declareHandlerIter) Next(ctx *sql.Context) (sql.Row, error) {
	d.Pref.InitializeHandler(d.Statement, d.Action == plan.DeclareHandlerAction_Exit)
	return nil, io.EOF
}

// Close implements the interface sql.RowIter.
func (d *declareHandlerIter) Close(ctx *sql.Context) error {
	return nil
}

const cteRecursionLimit = 1000

// recursiveCteIter exhaustively executes a recursive
// relation [rec] populated by an [init] base case.
// Refer to RecursiveCte for more details.
type recursiveCteIter struct {
	// base sql.Project
	init sql.Node
	// recursive sql.Project
	rec sql.Node
	// anchor to recursive table to repopulate with [temp]
	working *plan.RecursiveTable
	// true if UNION, false if UNION ALL
	deduplicate bool
	// parent iter initialization state
	row sql.Row

	// active iterator, either [init].RowIter or [rec].RowIter
	iter sql.RowIter
	// number of recursive iterations finished
	cycle int
	// buffer to collect intermediate results for next recursion
	temp []sql.Row
	// duplicate lookup if [deduplicated] set
	cache sql.KeyValueCache
	b     *builder
}

var _ sql.RowIter = (*recursiveCteIter)(nil)

// Next implements sql.RowIter
func (r *recursiveCteIter) Next(ctx *sql.Context) (sql.Row, error) {
	if r.iter == nil {
		// start with [Init].RowIter
		var err error
		if r.deduplicate {
			r.cache = sql.NewMapCache()

		}
		r.iter, err = r.b.buildNodeExec(ctx, r.init, r.row)

		if err != nil {
			return nil, err
		}
	}

	var row sql.Row
	for {
		var err error
		row, err = r.iter.Next(ctx)
		if errors.Is(err, io.EOF) && len(r.temp) > 0 {
			// reset [Rec].RowIter
			err = r.resetIter(ctx)
			if err != nil {
				return nil, err
			}
			continue
		} else if err != nil {
			return nil, err
		}

		var key uint64
		if r.deduplicate {
			key, _ = sql.HashOf(row)
			if k, _ := r.cache.Get(key); k != nil {
				// skip duplicate
				continue
			}
		}
		r.store(row, key)
		if err != nil {
			return nil, err
		}
		break
	}
	return row, nil
}

// store saves a row to the [temp] buffer, and hashes if [deduplicated] = true
func (r *recursiveCteIter) store(row sql.Row, key uint64) {
	if r.deduplicate {
		r.cache.Put(key, struct{}{})
	}
	r.temp = append(r.temp, row)
	return
}

// resetIter creates a new [Rec].RowIter after refreshing the [working] RecursiveTable
func (r *recursiveCteIter) resetIter(ctx *sql.Context) error {
	if len(r.temp) == 0 {
		return io.EOF
	}
	r.cycle++
	if r.cycle > cteRecursionLimit {
		return sql.ErrCteRecursionLimitExceeded.New()
	}

	if r.working != nil {
		r.working.Buf = r.temp
		r.temp = make([]sql.Row, 0)
	}

	err := r.iter.Close(ctx)
	if err != nil {
		return err
	}
	r.iter, err = r.b.buildNodeExec(ctx, r.rec, r.row)
	if err != nil {
		return err
	}
	return nil
}

// Close implements sql.RowIter
func (r *recursiveCteIter) Close(ctx *sql.Context) error {
	r.working.Buf = nil
	r.temp = nil
	if r.iter != nil {
		return r.iter.Close(ctx)
	}
	return nil
}

type limitIter struct {
	calcFoundRows bool
	currentPos    int64
	childIter     sql.RowIter
	limit         int64
}

func (li *limitIter) Next(ctx *sql.Context) (sql.Row, error) {
	if li.currentPos >= li.limit {
		// If we were asked to calc all found rows, then when we are past the limit we iterate over the rest of the
		// result set to count it
		if li.calcFoundRows {
			for {
				_, err := li.childIter.Next(ctx)
				if err != nil {
					return nil, err
				}
				li.currentPos++
			}
		}

		return nil, io.EOF
	}

	childRow, err := li.childIter.Next(ctx)
	li.currentPos++
	if err != nil {
		return nil, err
	}

	return childRow, nil
}

func (li *limitIter) Close(ctx *sql.Context) error {
	err := li.childIter.Close(ctx)
	if err != nil {
		return err
	}

	if li.calcFoundRows {
		ctx.SetLastQueryInfo(sql.FoundRows, li.currentPos)
	}
	return nil
}

type sortIter struct {
	sortFields  sql.SortFields
	childIter   sql.RowIter
	childIter2  sql.RowIter2
	sortedRows  []sql.Row
	sortedRows2 []sql.Row2
	idx         int
}

var _ sql.RowIter = (*sortIter)(nil)
var _ sql.RowIter2 = (*sortIter)(nil)

func newSortIter(s sql.SortFields, child sql.RowIter) *sortIter {
	childIter2, _ := child.(sql.RowIter2)
	return &sortIter{
		sortFields: s,
		childIter:  child,
		childIter2: childIter2,
		idx:        -1,
	}
}

func (i *sortIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.idx == -1 {
		err := i.computeSortedRows(ctx)
		if err != nil {
			return nil, err
		}
		i.idx = 0
	}

	if i.idx >= len(i.sortedRows) {
		return nil, io.EOF
	}
	row := i.sortedRows[i.idx]
	i.idx++
	return row, nil
}

func (i *sortIter) Close(ctx *sql.Context) error {
	i.sortedRows = nil
	return i.childIter.Close(ctx)
}

func (i *sortIter) computeSortedRows(ctx *sql.Context) error {
	cache, dispose := ctx.Memory.NewRowsCache()
	defer dispose()

	for {
		row, err := i.childIter.Next(ctx)

		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err := cache.Add(row); err != nil {
			return err
		}
	}

	rows := cache.Get()
	sorter := &expression.Sorter{
		SortFields: i.sortFields,
		Rows:       rows,
		LastError:  nil,
		Ctx:        ctx,
	}
	sort.Stable(sorter)
	if sorter.LastError != nil {
		return sorter.LastError
	}
	i.sortedRows = rows
	return nil
}

// distinctIter keeps track of the hashes of all rows that have been emitted.
// It does not emit any rows whose hashes have been seen already.
// TODO: come up with a way to use less memory than keeping all hashes in memory.
// Even though they are just 64-bit integers, this could be a problem in large
// result sets.
type distinctIter struct {
	childIter sql.RowIter
	seen      sql.KeyValueCache
	dispose   sql.DisposeFunc
}

func newDistinctIter(ctx *sql.Context, child sql.RowIter) *distinctIter {
	cache, dispose := ctx.Memory.NewHistoryCache()
	return &distinctIter{
		childIter: child,
		seen:      cache,
		dispose:   dispose,
	}
}

func (di *distinctIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		row, err := di.childIter.Next(ctx)
		if err != nil {
			if err == io.EOF {
				di.Dispose()
			}
			return nil, err
		}

		hash, err := sql.HashOf(row)
		if err != nil {
			return nil, err
		}

		if _, err := di.seen.Get(hash); err == nil {
			continue
		}

		if err := di.seen.Put(hash, struct{}{}); err != nil {
			return nil, err
		}

		return row, nil
	}
}

func (di *distinctIter) Close(ctx *sql.Context) error {
	di.Dispose()
	return di.childIter.Close(ctx)
}

func (di *distinctIter) Dispose() {
	if di.dispose != nil {
		di.dispose()
	}
}

type unionIter struct {
	cur      sql.RowIter
	nextIter func(ctx *sql.Context) (sql.RowIter, error)
}

func (ui *unionIter) Next(ctx *sql.Context) (sql.Row, error) {
	res, err := ui.cur.Next(ctx)
	if err == io.EOF {
		if ui.nextIter == nil {
			return nil, io.EOF
		}
		err = ui.cur.Close(ctx)
		if err != nil {
			return nil, err
		}
		ui.cur, err = ui.nextIter(ctx)
		ui.nextIter = nil
		if err != nil {
			return nil, err
		}
		return ui.cur.Next(ctx)
	}
	return res, err
}

func (ui *unionIter) Close(ctx *sql.Context) error {
	if ui.cur != nil {
		return ui.cur.Close(ctx)
	} else {
		return nil
	}
}
