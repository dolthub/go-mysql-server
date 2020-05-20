// Copyright 2020 Liquidata, Inc.
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
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

type indexAnalyzer struct {
	// TODO: these need to be qualified by database name as well to be valid. Otherwise we can't distinguish between two
	//  tables with the same name in different databases. But right now table nodes aren't qualified by their resolved
	//  database in the plan, so we can't do this.
	indexesByTable map[string][]sql.Index
	indexRegistry *sql.IndexRegistry
}

// getIndexesForNode returns a slice of all indexes available in the node given. These might come from either the
// tables themselves natively, or else from an index driver that has indexes for the tables included in the nodes.
func getIndexesForNode(ctx *sql.Context, a Analyzer, n sql.Node) (*indexAnalyzer, error) {
	var analysisErr error
	indexes := make(map[string][]sql.Index)

	// Find all of the native indexed tables in the node (those that don't require a driver)
	plan.Inspect(n, func(node sql.Node) bool {
		switch x := node.(type) {
		case *plan.ResolvedTable:
			it, ok := x.Table.(sql.IndexedTable)
			if !ok {
				return false
			}

			idxes, err := it.GetIndexes(ctx)
			if err != nil {
				analysisErr = err
				return false
			}
			indexes[it.Name()] = append(indexes[it.Name()], idxes...)
		}

		return true
	})

	var idxRegistry *sql.IndexRegistry
	if ctx.HasDrivers() {
		idxRegistry = ctx.IndexRegistry
	}

	return &indexAnalyzer{
		indexesByTable: indexes,
		indexRegistry: idxRegistry,
	}, nil
}

// IndexByExpression returns an index by the given expression. It will return
// nil if an index is not found. If more than one expression is given, all
// of them must match for the index to be matched.
func (r *indexAnalyzer) IndexByExpression(ctx *sql.Context, db string, expr ...sql.Expression) sql.Index {
	exprStrs := make([]string, len(expr))
	for i, e := range expr {
		exprStrs[i] = e.String()
	}

	for _, idxes := range r.indexesByTable {
		for _, idx := range idxes {
			if exprListsEqual(idx.Expressions(), exprStrs) {
				return idx
			}
		}
	}

	if r.indexRegistry != nil {
		return r.IndexByExpression(ctx, db, expr...)
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
		results = append(results, r.indexRegistry.ExpressionsWithIndexes(db, exprs...)...)
	}

	return results
}

// exprListsMatch returns whether any subset of a is the entirety of b.
func exprListsMatch(a, b []string) bool {
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

// exprListsEqual returns whether a and b have the same items.
func exprListsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	return exprListsMatch(a, b)
}