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
	return rowIterWithOkResultWithZeroRowsAffected(), nil
}

func (b *BaseBuilder) buildFetch(ctx *sql.Context, n *plan.Fetch, row sql.Row) (sql.RowIter, error) {
	row, sch, err := n.Pref.FetchCursor(ctx, n.Name)
	if err == io.EOF {
		return sql.RowsToRowIter(), expression.FetchEOF
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
	var returnNode sql.Node
	var repIterIdx = len(n.Children()) - 1
	var subIters = make([]sql.RowIter, len(n.Children()))
	var subIterHandlers = make([][]*expression.HandlerIters, len(n.Children()))
	seenSelect := false
	for ci, child := range n.Children() {
		// build and reorganize handlers for subIter
		hRefs := n.Pref.InnermostScope.Handlers
		hIters := make([]*expression.HandlerIters, len(hRefs))
		for i, hRef := range hRefs {
			hRowIter, hErr := b.buildNodeExec(ctx, hRef.Stmt, nil)
			if hErr != nil {
				return nil, hErr
			}
			hIters[len(hRefs) - i - 1] = expression.NewHandlerIters(hRowIter, hRef.Cond, hRef.Action)
		}
		subIterHandlers[ci] = hIters

		// build subIter and save for later
		subIter, err := b.buildNodeExec(ctx, child, row)
		if err != nil {
			return nil, err
		}
		subIters[ci] = subIter

		// the representing node is the last select node in the block
		// if there is no select node, the representing node is the last node in the block
		subIterNode := child
		if blockSubIter, ok := subIter.(plan.BlockRowIter); ok {
			subIterNode = blockSubIter.RepresentingNode()
		}
		if plan.NodeRepresentsSelect(subIterNode) {
			repIterIdx = ci
			seenSelect = true
			returnNode = subIterNode
			continue
		}
		if !seenSelect {
			returnNode = subIterNode
		}
	}

	if returnNode == nil {
		return nil, fmt.Errorf("block does not contain any statements")
	}

	returnSch := returnNode.Schema()
	n.SetSchema(returnSch)
	return &blockIter{
		internalIter: nil,
		repNode:      returnNode,
		repSch:       returnSch,

		repIterIdx: repIterIdx,
		subIters:   subIters,

		subIterHandlers: subIterHandlers,
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

func (b *BaseBuilder) buildAnalyzeTable(ctx *sql.Context, n *plan.AnalyzeTable, row sql.Row) (sql.RowIter, error) {
	// Assume table is in current database
	database := ctx.GetCurrentDatabase()
	if database == "" {
		return nil, sql.ErrNoDatabaseSelected.New()
	}

	return &analyzeTableIter{
		idx:    0,
		db:     n.Db,
		tables: n.Tables,
		stats:  n.Stats,
	}, nil
}

func (b *BaseBuilder) buildDropHistogram(ctx *sql.Context, n *plan.DropHistogram, row sql.Row) (sql.RowIter, error) {
	// Assume table is in current database
	database := ctx.GetCurrentDatabase()
	if database == "" {
		return nil, sql.ErrNoDatabaseSelected.New()
	}

	return &dropHistogramIter{
		db:      n.Db(),
		table:   n.Table(),
		columns: n.Cols(),
		prov:    n.StatsProvider(),
	}, nil
}

func (b *BaseBuilder) buildUpdateHistogram(ctx *sql.Context, n *plan.UpdateHistogram, row sql.Row) (sql.RowIter, error) {
	// Assume table is in current database
	database := ctx.GetCurrentDatabase()
	if database == "" {
		return nil, sql.ErrNoDatabaseSelected.New()
	}

	return &updateHistogramIter{
		db:      n.Db(),
		table:   n.Table(),
		columns: n.Cols(),
		stats:   n.Stats(),
		prov:    n.StatsProvider(),
	}, nil
}

func (b *BaseBuilder) buildCreateSpatialRefSys(ctx *sql.Context, n *plan.CreateSpatialRefSys, row sql.Row) (sql.RowIter, error) {
	if _, ok := types.SupportedSRIDs[n.SRID]; ok {
		if n.IfNotExists {
			return rowIterWithOkResultWithZeroRowsAffected(), nil
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

	return rowIterWithOkResultWithZeroRowsAffected(), nil
}
