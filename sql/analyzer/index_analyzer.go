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
	"sort"

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
			case *plan.IndexedTableAccess:
				err := indexesForTable(n.Name(), n.ResolvedTable)
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

// MatchingIndex returns the exact match if an index exists that perfectly matches the given expressions, otherwise it
// returns the longest matching index for the given expressions.
func (r *indexAnalyzer) MatchingIndex(ctx *sql.Context, db string, table string, exprs ...sql.Expression) sql.Index {
	indexes := r.MatchingIndexes(ctx, db, table, exprs...)
	if len(indexes) > 0 {
		return indexes[0]
	}
	return nil
}

// MatchingIndexes returns a list of all matching indexes for the given expressions, with any indexes that exactly match
// the given expressions sorted first, and all other indexes sorted by expression count in descending order after the
// exact matches.
func (r *indexAnalyzer) MatchingIndexes(ctx *sql.Context, db string, table string, exprs ...sql.Expression) []sql.Index {
	// As multiple expressions may be the same, we filter out duplicates
	distinctExprs := make(map[string]struct{})
	var exprStrs []string
	for _, e := range exprs {
		es := e.String()
		if _, ok := distinctExprs[es]; !ok {
			distinctExprs[es] = struct{}{}
			exprStrs = append(exprStrs, es)
		}
	}

	type idxWithLen struct {
		sql.Index
		exprLen int
	}

	var indexes []idxWithLen
	for _, idx := range r.indexesByTable[table] {
		indexExprs := idx.Expressions()
		if exprsAreIndexPrefix(exprStrs, indexExprs) {
			indexes = append(indexes, idxWithLen{idx, len(indexExprs)})
		}
	}

	if r.indexRegistry != nil {
		idx := r.indexRegistry.IndexByExpression(ctx, db, exprs...)
		if idx != nil {
			r.registryIdxes = append(r.registryIdxes, idx)
			indexes = append(indexes, idxWithLen{idx, len(idx.Expressions())})
		}
	}

	exprLen := len(exprStrs)
	sort.Slice(indexes, func(i, j int) bool {
		idxI := indexes[i]
		idxJ := indexes[j]
		if idxI.exprLen == exprLen && idxJ.exprLen != exprLen {
			return true
		} else if idxI.exprLen != exprLen && idxJ.exprLen == exprLen {
			return false
		} else {
			return idxI.exprLen > idxJ.exprLen || idxI.Index.ID() < idxJ.Index.ID()
		}
	})
	sortedIndexes := make([]sql.Index, len(indexes))
	for i := 0; i < len(sortedIndexes); i++ {
		sortedIndexes[i] = indexes[i].Index
	}
	return sortedIndexes
}

// ExpressionsWithIndexes finds all the combinations of expressions with matching indexes. This only matches
// multi-column indexes. Sorts the list of expressions by their length in descending order.
func (r *indexAnalyzer) ExpressionsWithIndexes(db string, exprs ...sql.Expression) [][]sql.Expression {
	var results [][]sql.Expression

	// First find matches in the native indexes
	for _, idxes := range r.indexesByTable {
	Indexes:
		for _, idx := range idxes {
			var used = make(map[int]struct{})
			var matched []sql.Expression
			for _, ie := range idx.Expressions() {
				var found bool
				for i, e := range exprs {
					if _, ok := used[i]; ok {
						continue
					}

					if ie == e.String() {
						used[i] = struct{}{}
						found = true
						matched = append(matched, e)
						break
					}
				}

				if !found {
					break
				}
			}
			if len(matched) == 0 {
				continue Indexes
			}

			results = append(results, matched)
		}
	}

	// Expand the search to the index registry if present
	if r.indexRegistry != nil {
		indexes := r.indexRegistry.ExpressionsWithIndexes(db, exprs...)
		results = append(results, indexes...)
	}

	sort.SliceStable(results, func(i, j int) bool {
		return len(results[i]) > len(results[j])
	})
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

// exprsAreIndexPrefix returns whether exprs are a subset of indexExprs. It is assumed that indexExprs are ordered by their
// declaration. For example `INDEX (v3, v2, v1)` would pass in `[]string{"v3", "v2", v1"}` and no other order.
func exprsAreIndexPrefix(exprs, indexExprs []string) bool {
	if len(exprs) > len(indexExprs) {
		return false
	}

	visitedIndexExprs := make([]bool, len(indexExprs))
	for _, expr := range exprs {
		found := false
		for j, indexExpr := range indexExprs {
			if visitedIndexExprs[j] {
				continue
			}
			if expr == indexExpr {
				visitedIndexExprs[j] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// This checks that the order is preserved, as all true booleans should be first, with every boolean afterward false
	expectation := true
	for _, visitedExpr := range visitedIndexExprs {
		if visitedExpr == expectation {
			continue
		} else if visitedExpr && !expectation {
			return false
		} else if !visitedExpr && expectation {
			expectation = false
		}
	}

	return true
}
