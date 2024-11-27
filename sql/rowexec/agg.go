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
	"errors"
	"fmt"
	"io"

	"github.com/cespare/xxhash/v2"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type groupByIter struct {
	selectedExprs []sql.Expression
	child         sql.RowIter
	ctx           *sql.Context
	buf           []sql.AggregationBuffer
	done          bool
}

func newGroupByIter(selectedExprs []sql.Expression, child sql.RowIter) *groupByIter {
	return &groupByIter{
		selectedExprs: selectedExprs,
		child:         child,
		buf:           make([]sql.AggregationBuffer, len(selectedExprs)),
	}
}

func (i *groupByIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.done {
		return nil, io.EOF
	}

	// special case for any_value
	var err error
	onlyAnyValue := true
	for j, a := range i.selectedExprs {
		i.buf[j], err = newAggregationBuffer(a)
		if err != nil {
			return nil, err
		}
		if agg, ok := a.(sql.Aggregation); ok {
			if _, ok = agg.(*aggregation.AnyValue); !ok {
				onlyAnyValue = false
			}
		}
	}

	// if no aggregate functions other than any_value, it's just a normal select
	if onlyAnyValue {
		row, err := i.child.Next(ctx)
		if err != nil {
			i.done = true
			return nil, err
		}

		if err := updateBuffers(ctx, i.buf, row); err != nil {
			return nil, err
		}
		return evalBuffers(ctx, i.buf)
	}
	i.done = true

	for {
		row, err := i.child.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if err := updateBuffers(ctx, i.buf, row); err != nil {
			return nil, err
		}
	}

	row, err := evalBuffers(ctx, i.buf)
	if err != nil {
		return nil, err
	}
	return row, nil
}

func (i *groupByIter) Close(ctx *sql.Context) error {
	i.Dispose()
	i.buf = nil
	return i.child.Close(ctx)
}

func (i *groupByIter) Dispose() {
	for _, b := range i.buf {
		b.Dispose()
	}
}

type groupByGroupingIter struct {
	selectedExprs []sql.Expression
	groupByExprs  []sql.Expression
	aggregations  sql.KeyValueCache
	keys          []uint64
	pos           int
	child         sql.RowIter
	dispose       sql.DisposeFunc
}

func newGroupByGroupingIter(
	ctx *sql.Context,
	selectedExprs, groupByExprs []sql.Expression,
	child sql.RowIter,
) *groupByGroupingIter {
	return &groupByGroupingIter{
		selectedExprs: selectedExprs,
		groupByExprs:  groupByExprs,
		child:         child,
	}
}

func (i *groupByGroupingIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.aggregations == nil {
		i.aggregations, i.dispose = ctx.Memory.NewHistoryCache()
		if err := i.compute(ctx); err != nil {
			return nil, err
		}
	}

	if i.pos >= len(i.keys) {
		return nil, io.EOF
	}

	buffers, err := i.get(i.keys[i.pos])
	if err != nil {
		return nil, err
	}
	i.pos++

	row, err := evalBuffers(ctx, buffers)
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (i *groupByGroupingIter) compute(ctx *sql.Context) error {
	for {
		row, err := i.child.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		key, err := groupingKey(ctx, i.groupByExprs, row)
		if err != nil {
			return err
		}

		b, err := i.get(key)
		if errors.Is(err, sql.ErrKeyNotFound) {
			b = make([]sql.AggregationBuffer, len(i.selectedExprs))
			for j, a := range i.selectedExprs {
				b[j], err = newAggregationBuffer(a)
				if err != nil {
					return err
				}
			}

			if err := i.aggregations.Put(key, b); err != nil {
				return err
			}

			i.keys = append(i.keys, key)
		} else if err != nil {
			return err
		}

		err = updateBuffers(ctx, b, row)
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *groupByGroupingIter) get(key uint64) ([]sql.AggregationBuffer, error) {
	v, err := i.aggregations.Get(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	return v.([]sql.AggregationBuffer), err
}

func (i *groupByGroupingIter) put(key uint64, val []sql.AggregationBuffer) error {
	return i.aggregations.Put(key, val)
}

func (i *groupByGroupingIter) Close(ctx *sql.Context) error {
	i.Dispose()
	i.aggregations = nil
	if i.dispose != nil {
		i.dispose()
		i.dispose = nil
	}

	return i.child.Close(ctx)
}

func (i *groupByGroupingIter) Dispose() {
	for _, k := range i.keys {
		bs, _ := i.get(k)
		if bs != nil {
			for _, b := range bs {
				b.Dispose()
			}
		}
	}
}

func groupingKey(
	ctx *sql.Context,
	exprs []sql.Expression,
	row sql.Row,
) (uint64, error) {
	hash := xxhash.New()
	for i, expr := range exprs {
		v, err := expr.Eval(ctx, row)
		if err != nil {
			return 0, err
		}

		if i > 0 {
			// separate each expression in the grouping key with a nil byte
			if _, err = hash.Write([]byte{0}); err != nil {
				return 0, err
			}
		}

		t, isStringType := expr.Type().(sql.StringType)
		if isStringType && v != nil {
			v, err = types.ConvertToString(v, t)
			if err == nil {
				err = t.Collation().WriteWeightString(hash, v.(string))
			}
		} else {
			_, err = fmt.Fprintf(hash, "%v", v)
		}
		if err != nil {
			return 0, err
		}
	}

	return hash.Sum64(), nil
}

func newAggregationBuffer(expr sql.Expression) (sql.AggregationBuffer, error) {
	switch n := expr.(type) {
	case sql.Aggregation:
		return n.NewBuffer()
	default:
		// The semantics for a non-aggregation expression in a group by node is First.
		// When ONLY_FULL_GROUP_BY is enabled, this is an error, but it's allowed otherwise.
		return aggregation.NewFirst(expr).NewBuffer()
	}
}

func updateBuffers(
	ctx *sql.Context,
	buffers []sql.AggregationBuffer,
	row sql.Row,
) error {
	for _, b := range buffers {
		if err := b.Update(ctx, row); err != nil {
			return err
		}
	}

	return nil
}

func evalBuffers(
	ctx *sql.Context,
	buffers []sql.AggregationBuffer,
) (sql.Row, error) {
	row := sql.NewSqlRowWithLen(len(buffers))

	var err error
	for i, b := range buffers {
		var v interface{}
		v, err = b.Eval(ctx)
		row.SetValue(i, v)
		if err != nil {
			return nil, err
		}
	}

	return row, nil
}
