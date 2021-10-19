// Copyright 2020-2021 Dolthub, Inc.
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

package analyzer

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type indexAnalyzer struct {
	// TODO: these need to be qualified by database name as well to be valid. Otherwise we can't distinguish between two
	//  tables with the same name in different databases. But right now table nodes aren't qualified by their resolved
	//  database in the plan, so we can't do this.
	indexesByTable map[string][]sql.Index
	indexRegistry  *sql.IndexRegistry
	registryIdxes  []sql.Index
}

// getIndexesForNode returns an analyzer for indexes available in the node given, keyed by the table name. These might
// come from either the tables themselves natively, or else from an index driver that has indexes for the tables
// included in the nodes. Indexes are keyed by the aliased name of the table, if applicable. These names must be
// unaliased when matching against the names of tables in index definitions.
func getIndexesForNode(ctx *sql.Context, a *Analyzer, n sql.Node) (*indexAnalyzer, error) {
	var analysisErr error
	indexes := make(map[string][]sql.Index)

	var indexesForTable = func(name string, rt *plan.ResolvedTable) error {
		it, ok := rt.Table.(sql.IndexedTable)
		if !ok {
			return nil
		}

		idxes, err := it.GetIndexes(ctx)
		if err != nil {
			return err
		}

		indexes[name] = append(indexes[name], idxes...)
		return nil
	}

	// Find all of the native indexed tables in the node (those that don't require a driver)
	if n != nil {
		plan.Inspect(n, func(n sql.Node) bool {
			switch n := n.(type) {
			case *plan.TableAlias:
				rt, ok := n.Child.(*plan.ResolvedTable)
				if !ok {
					return false
				}

				err := indexesForTable(n.Name(), rt)
				if err != nil {
					analysisErr = err
					return false
				}

				return false
			case *plan.ResolvedTable:
				err := indexesForTable(n.Name(), n)
				if err != nil {
					analysisErr = err
					return false
				}
			}

			return true
		})
	}

	if analysisErr != nil {
		return nil, analysisErr
	}

	var idxRegistry *sql.IndexRegistry
	if ctx.GetIndexRegistry().HasIndexes() {
		idxRegistry = ctx.GetIndexRegistry()
	}

	return &indexAnalyzer{
		indexesByTable: indexes,
		indexRegistry:  idxRegistry,
	}, nil
}

// IndexesByTable returns all indexes on the table named. The table must be present in the node used to create the
// analyzer.
func (r *indexAnalyzer) IndexesByTable(ctx *sql.Context, db, table string) []sql.Index {
	indexes := r.indexesByTable[table]

	if r.indexRegistry != nil {
		idxes := r.indexRegistry.IndexesByTable(db, table)
		for _, idx := range idxes {
			indexes = append(indexes, idx)
		}
	}

	return indexes
}

// IndexByExpression returns an index by the given expression. It will return nil if no index is found. If more than
// one expression is given, all of them must match for the index to be matched.
func (r *indexAnalyzer) IndexByExpression(ctx *sql.Context, db string, table string, expr ...sql.Expression) sql.Index {
	// Multiple expressions may be the same so we filter out duplicates
	distinctExprs := make(map[string]struct{})
	var exprStrs []string
	for _, e := range expr {
		es := e.String()
		if _, ok := distinctExprs[es]; !ok {
			distinctExprs[es] = struct{}{}
			exprStrs = append(exprStrs, es)
		}
	}

	for _, idx := range r.indexesByTable[strings.ToLower(table)] {
		if exprListsEqual(idx.Expressions(), exprStrs) {
			return idx
		}
	}

	if r.indexRegistry != nil {
		idx := r.indexRegistry.IndexByExpression(ctx, db, expr...)
		r.registryIdxes = append(r.registryIdxes, idx)
		return idx
	}

	return nil
}

// ExpressionsWithIndexes finds all the combinations of expressions with matching indexes. This only matches
// multi-column indexes.
func (r *indexAnalyzer) ExpressionsWithIndexes(db string, exprs ...sql.Expression) [][]sql.Expression {
	var results [][]sql.Expression

	// First find matches in the native indexes
	for _, idxes := range r.indexesByTable {
	Indexes:
		for _, idx := range idxes {
			if ln := len(idx.Expressions()); ln <= len(exprs) && ln > 1 {
				var used = make(map[int]bool)
				var matched []sql.Expression
				for _, ie := range idx.Expressions() {
					var found bool
					for i, e := range exprs {
						if used[i] {
							continue
						}

						if ie == e.String() {
							used[i] = true
							found = true
							matched = append(matched, e)
							break
						}
					}

					if !found {
						continue Indexes
					}
				}

				results = append(results, matched)
			}
		}
	}

	// Expand the search to the index registry if present
	if r.indexRegistry != nil {
		indexes := r.indexRegistry.ExpressionsWithIndexes(db, exprs...)
		results = append(results, indexes...)
	}

	return results
}

// releaseUsedIndexes should be called in the top level function of index analysis to return any held res
func (r *indexAnalyzer) releaseUsedIndexes() {
	if r.indexRegistry == nil {
		return
	}

	for _, i := range r.registryIdxes {
		if i != nil {
			r.indexRegistry.ReleaseIndex(i)
		}
	}
}

// exprListsEqual returns whether a and b have the same items.
func exprListsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	var visited = make([]bool, len(b))

	for _, va := range a {
		found := false

		for j, vb := range b {
			if visited[j] {
				continue
			}

			if va == vb {
				visited[j] = true
				found = true
				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}
