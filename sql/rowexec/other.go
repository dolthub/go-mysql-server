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
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func (b *BaseBuilder) buildStripRowNode(ctx *sql.Context, n *plan.StripRowNode, row sql.Row) (sql.RowIter, error) {
	childIter, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		return nil, err
	}

	return &stripRowIter{
		childIter,
		n.NumCols,
	}, nil
}

func (b *BaseBuilder) buildConcat(ctx *sql.Context, n *plan.Concat, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.Concat")
	li, err := b.buildNodeExec(ctx, n.Left(), row)
	if err != nil {
		span.End()
		return nil, err
	}
	i := newConcatIter(
		ctx,
		li,
		func() (sql.RowIter, error) {
			return b.buildNodeExec(ctx, n.Right(), row)
		},
	)
	return sql.NewSpanIter(span, i), nil
}

func (b *BaseBuilder) buildReleaser(ctx *sql.Context, n *plan.Releaser, row sql.Row) (sql.RowIter, error) {
	iter, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		n.Release()
		return nil, err
	}

	return &releaseIter{child: iter, release: n.Release}, nil
}

func (b *BaseBuilder) buildDeallocateQuery(ctx *sql.Context, n *plan.DeallocateQuery, row sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(sql.NewRow(types.OkResult{})), nil
}

func (b *BaseBuilder) buildFetch(ctx *sql.Context, n *plan.Fetch, row sql.Row) (sql.RowIter, error) {
	row, sch, err := n.Pref.FetchCursor(ctx, n.Name)
	if err == io.EOF {
		scope := n.Pref.InnermostScope
		for scope != nil {
			for i := len(scope.Handlers) - 1; i >= 0; i-- {
				//TODO: handle more than NOT FOUND handlers, handlers should check if the error applies to them first
				originalScope := n.Pref.InnermostScope
				defer func() {
					n.Pref.InnermostScope = originalScope
				}()
				n.Pref.InnermostScope = scope
				handlerRefVal := scope.Handlers[i]

				handlerRowIter, err := b.buildNodeExec(ctx, handlerRefVal.Stmt, nil)
				if err != nil {
					return sql.RowsToRowIter(), err
				}
				defer handlerRowIter.Close(ctx)

				for {
					_, err := handlerRowIter.Next(ctx)
					if err == io.EOF {
						break
					} else if err != nil {
						return sql.RowsToRowIter(), err
					}
				}
				if handlerRefVal.IsExit {
					return sql.RowsToRowIter(), expression.ProcedureBlockExitError(handlerRefVal.ScopeHeight)
				}
				return sql.RowsToRowIter(), io.EOF
			}
			scope = scope.Parent
		}
		return sql.RowsToRowIter(), err
	} else if err != nil {
		return nil, err
	}
	if len(row) != len(n.ToSet) {
		return nil, sql.ErrFetchIncorrectCount.New()
	}
	if len(n.ToSet) == 0 {
		return sql.RowsToRowIter(), io.EOF
	}

	if n.Sch == nil {
		n.Sch = sch
	}
	setExprs := make([]sql.Expression, len(n.ToSet))
	for i, expr := range n.ToSet {
		col := sch[i]
		setExprs[i] = expression.NewSetField(expr, expression.NewGetField(i, col.Type, col.Name, col.Nullable))
	}
	set := plan.NewSet(setExprs)
	return b.buildSet(ctx, set, row)
}

func (b *BaseBuilder) buildSignalName(ctx *sql.Context, n *plan.SignalName, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("%T has no exchange iterator", n)
}

func (b *BaseBuilder) buildRepeat(ctx *sql.Context, n *plan.Repeat, row sql.Row) (sql.RowIter, error) {
	return b.buildLoop(ctx, n.Loop, row)
}

func (b *BaseBuilder) buildDeferredFilteredTable(ctx *sql.Context, n *plan.DeferredFilteredTable, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("%T has no execution iterator", n)
}

func (b *BaseBuilder) buildNamedWindows(ctx *sql.Context, n *plan.NamedWindows, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("%T has no execution iterator", n)
}

func (b *BaseBuilder) buildExchange(ctx *sql.Context, n *plan.Exchange, row sql.Row) (sql.RowIter, error) {
	var t sql.Table
	transform.Inspect(n.Child, func(n sql.Node) bool {
		if table, ok := n.(sql.Table); ok {
			t = table
			return false
		}
		return true
	})
	if t == nil {
		return nil, plan.ErrNoPartitionable.New()
	}

	partitions, err := t.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	// How this is structured is a little subtle. A top-level
	// errgroup run |iterPartitions| and listens on the shutdown
	// hook.  A different, dependent, errgroup runs
	// |e.Parallelism| instances of |iterPartitionRows|. A
	// goroutine within the top-level errgroup |Wait|s on the
	// dependent errgroup and closes |rowsCh| once all its
	// goroutines are completed.

	partitionsCh := make(chan sql.Partition)
	rowsCh := make(chan sql.Row, n.Parallelism*16)

	eg, egCtx := ctx.NewErrgroup()
	eg.Go(func() error {
		defer close(partitionsCh)
		return iterPartitions(egCtx, partitions, partitionsCh)
	})

	// Spawn |iterPartitionRows| goroutines in the dependent
	// errgroup.
	getRowIter := b.exchangeIterGen(n, row)
	seg, segCtx := egCtx.NewErrgroup()
	for i := 0; i < n.Parallelism; i++ {
		seg.Go(func() error {
			return iterPartitionRows(segCtx, getRowIter, partitionsCh, rowsCh)
		})
	}

	eg.Go(func() error {
		defer close(rowsCh)
		err := seg.Wait()
		if err != nil {
			return err
		}
		// If everything in |seg| returned |nil|,
		// |iterPartitions| is done, |partitionsCh| is closed,
		// and every partition RowIter returned |EOF|. That
		// means we're EOF here.
		return io.EOF
	})

	waiter := func() error { return eg.Wait() }
	shutdownHook := newShutdownHook(eg, egCtx)
	return &exchangeRowIter{shutdownHook: shutdownHook, waiter: waiter, rows: rowsCh}, nil
}

func (b *BaseBuilder) buildExchangePartition(ctx *sql.Context, n *plan.ExchangePartition, row sql.Row) (sql.RowIter, error) {
	return n.Table.PartitionRows(ctx, n.Partition)
}

func (b *BaseBuilder) buildEmptyTable(ctx *sql.Context, n *plan.EmptyTable, row sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

func (b *BaseBuilder) buildDeclareCursor(ctx *sql.Context, n *plan.DeclareCursor, row sql.Row) (sql.RowIter, error) {
	return &declareCursorIter{n}, nil
}

func (b *BaseBuilder) buildTransformedNamedNode(ctx *sql.Context, n *plan.TransformedNamedNode, row sql.Row) (sql.RowIter, error) {
	return b.buildNodeExec(ctx, n.Child, row)
}

func (b *BaseBuilder) buildCachedResults(ctx *sql.Context, n *plan.CachedResults, row sql.Row) (sql.RowIter, error) {
	n.Mutex.Lock()
	defer n.Mutex.Unlock()

	if n.Disposed {
		return nil, fmt.Errorf("%w: %T", plan.ErrRowIterDisposed, n)
	}

	if rows := n.GetCachedResults(); rows != nil {
		return sql.RowsToRowIter(rows...), nil
	} else if n.NoCache {
		return b.buildNodeExec(ctx, n.Child, row)
	} else if n.Finalized {
		return plan.EmptyIter, nil
	}

	ci, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		return nil, err
	}
	cache, dispose := ctx.Memory.NewRowsCache()
	return &cachedResultsIter{n, ci, cache, dispose}, nil
}

func (b *BaseBuilder) buildBlock(ctx *sql.Context, n *plan.Block, row sql.Row) (sql.RowIter, error) {
	var returnRows []sql.Row
	var returnNode sql.Node
	var returnSch sql.Schema

	selectSeen := false
	for _, s := range n.Children() {
		// TODO: this should happen at iteration time, but this call is where the actual iteration happens
		err := startTransaction(ctx)
		if err != nil {
			return nil, err
		}

		err = func() error {
			rowCache, disposeFunc := ctx.Memory.NewRowsCache()
			defer disposeFunc()

			var isSelect bool
			subIter, err := b.buildNodeExec(ctx, s, row)
			if err != nil {
				return err
			}
			subIterNode := s
			subIterSch := s.Schema()
			if blockSubIter, ok := subIter.(plan.BlockRowIter); ok {
				subIterNode = blockSubIter.RepresentingNode()
				subIterSch = blockSubIter.Schema()
			}
			if isSelect = plan.NodeRepresentsSelect(subIterNode); isSelect {
				selectSeen = true
				returnNode = subIterNode
				returnSch = subIterSch
			} else if !selectSeen {
				returnNode = subIterNode
				returnSch = subIterSch
			}

			for {
				newRow, err := subIter.Next(ctx)
				if err == io.EOF {
					err := subIter.Close(ctx)
					if err != nil {
						return err
					}
					if isSelect || !selectSeen {
						returnRows = rowCache.Get()
					}
					break
				} else if err != nil {
					return err
				} else if isSelect || !selectSeen {
					err = rowCache.Add(newRow)
					if err != nil {
						return err
					}
				}
			}
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}

	n.SetSchema(returnSch)
	return &blockIter{
		internalIter: sql.RowsToRowIter(returnRows...),
		repNode:      returnNode,
		sch:          returnSch,
	}, nil
}

func (b *BaseBuilder) buildDeferredAsOfTable(ctx *sql.Context, n *plan.DeferredAsOfTable, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("%T has no execution iterator", n)
}

func (b *BaseBuilder) buildNothing(ctx *sql.Context, n plan.Nothing, row sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

func (b *BaseBuilder) buildTableCopier(ctx *sql.Context, n *plan.TableCopier, row sql.Row) (sql.RowIter, error) {
	if _, ok := n.Destination.(*plan.CreateTable); ok {
		return n.ProcessCreateTable(ctx, b, row)
	}

	drt, ok := n.Destination.(*plan.ResolvedTable)
	if !ok {
		return nil, fmt.Errorf("TableCopier only accepts CreateTable or TableNode as the destination")
	}

	return n.CopyTableOver(ctx, n.Source.Schema()[0].Source, drt.Name())
}

func (b *BaseBuilder) buildUnresolvedTable(ctx *sql.Context, n *plan.UnresolvedTable, row sql.Row) (sql.RowIter, error) {
	return nil, plan.ErrUnresolvedTable.New()
}

func (b *BaseBuilder) buildPrependNode(ctx *sql.Context, n *plan.PrependNode, row sql.Row) (sql.RowIter, error) {
	childIter, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		return nil, err
	}

	return &prependRowIter{
		row:       n.Row,
		childIter: childIter,
	}, nil
}

func (b *BaseBuilder) buildQueryProcess(ctx *sql.Context, n *plan.QueryProcess, row sql.Row) (sql.RowIter, error) {
	iter, err := b.Build(ctx, n.Child(), row)
	if err != nil {
		return nil, err
	}

	qType := plan.GetQueryType(n.Child())

	trackedIter := plan.NewTrackedRowIter(n.Child(), iter, nil, n.Notify)
	trackedIter.QueryType = qType
	trackedIter.ShouldSetFoundRows = qType == plan.QueryTypeSelect && n.ShouldSetFoundRows()

	return trackedIter, nil
}

func (b *BaseBuilder) buildAnalyzeTable(ctx *sql.Context, n *plan.AnalyzeTable, row sql.Row) (sql.RowIter, error) {
	// Assume table is in current database
	database := ctx.GetCurrentDatabase()
	if database == "" {
		return nil, sql.ErrNoDatabaseSelected.New()
	}

	return &analyzeTableIter{
		idx:    0,
		tables: n.Tables,
		stats:  n.Stats,
	}, nil
}

func (b *BaseBuilder) buildCreateSpatialRefSys(ctx *sql.Context, n *plan.CreateSpatialRefSys, row sql.Row) (sql.RowIter, error) {
	if _, ok := types.SupportedSRIDs[n.SRID]; ok {
		if n.IfNotExists {
			return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), nil
		}
		if !n.OrReplace {
			return nil, sql.ErrSpatialRefSysAlreadyExists.New(n.SRID)
		}
	}

	types.SupportedSRIDs[n.SRID] = types.SpatialRef{
		Name:          n.SrsAttr.Name,
		ID:            n.SRID,
		Organization:  n.SrsAttr.Organization,
		OrgCoordsysId: n.SrsAttr.OrgID,
		Definition:    n.SrsAttr.Definition,
		Description:   n.SrsAttr.Description,
	}

	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), nil
}
