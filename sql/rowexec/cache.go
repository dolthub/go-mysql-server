// Copyright 2020-2026 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"io"
)

func (b *BaseBuilder) buildCachedResults(ctx *sql.Context, n *plan.CachedResults, row sql.Row) (sql.RowIter, error) {
	n.Mutex.Lock()
	defer n.Mutex.Unlock()

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
	return newCachedResultsIter(n, ci), nil
}

type cachedResultsIter struct {
	node      *plan.CachedResults
	childIter sql.RowIter
	results   []sql.Row
}

func newCachedResultsIter(node *plan.CachedResults, childIter sql.RowIter) *cachedResultsIter {
	return &cachedResultsIter{
		node:      node,
		childIter: childIter,
	}
}

func (i *cachedResultsIter) Next(ctx *sql.Context) (sql.Row, error) {
	r, err := i.childIter.Next(ctx)
	if err != nil {
		if err == io.EOF {
			i.saveResultsInNode()
		}
	} else {
		i.results = append(i.results, r)
	}
	return r, err
}

func (i *cachedResultsIter) saveResultsInNode() {
	i.node.SetCachedResults(i.results)
}

func (i *cachedResultsIter) Close(ctx *sql.Context) error {
	return i.childIter.Close(ctx)
}
