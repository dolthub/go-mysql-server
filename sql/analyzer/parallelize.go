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
	"os"
	"strconv"

	"github.com/go-kit/kit/metrics/discard"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

func init() {
	// check for single-threaded feature flag
	if v, ok := os.LookupEnv(singleThreadedFeatureFlag); ok && v != "" {
		SingleThreadFeatureFlag = true
	}
}

const (
	singleThreadedFeatureFlag = "SINGLE_THREAD_FEATURE_FLAG"
)

var (
	// ParallelQueryCounter describes a metric that accumulates
	// number of parallel queries monotonically.
	ParallelQueryCounter = discard.NewCounter()

	SingleThreadFeatureFlag = false
)

func shouldParallelize(node sql.Node, scope *Scope) bool {
	if SingleThreadFeatureFlag {
		return false
	}

	// Don't parallelize subqueries, this can blow up the execution graph quickly
	if len(scope.Schema()) > 0 {
		return false
	}

	if tc, ok := node.(*plan.TransactionCommittingNode); ok {
		return shouldParallelize(tc.Child(), scope)
	}

	// Do not try to parallelize DDL or descriptive operations
	return !plan.IsNoRowNode(node)
}

func parallelize(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if a.Parallelism <= 1 || !node.Resolved() {
		return node, transform.SameTree, nil
	}

	proc, ok := node.(*plan.QueryProcess)
	if (ok && !shouldParallelize(proc.Child(), nil)) || !shouldParallelize(node, scope) {
		return node, transform.SameTree, nil
	}

	node, same, err := transform.Node(node, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if !isParallelizable(node) {
			return node, transform.SameTree, nil
		}
		ParallelQueryCounter.With("parallelism", strconv.Itoa(a.Parallelism)).Add(1)

		return plan.NewExchange(a.Parallelism, node), transform.NewTree, nil
	})
	if err != nil {
		return nil, transform.SameTree, err
	}
	if same {
		return node, transform.SameTree, nil
	}

	return transform.Node(node, removeRedundantExchanges)
}

// removeRedundantExchanges removes all the exchanges except for the topmost
// of all.
func removeRedundantExchanges(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
	exchange, ok := node.(*plan.Exchange)
	if !ok {
		return node, transform.SameTree, nil
	}

	child, same, err := transform.Node(exchange.Child, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if exchange, ok := node.(*plan.Exchange); ok {
			return exchange.Child, transform.NewTree, nil
		}
		return node, transform.SameTree, nil
	})
	if err != nil {
		return nil, transform.SameTree, err
	}
	if same {
		return node, transform.SameTree, nil
	}
	node, err = exchange.WithChildren(child)
	return node, transform.NewTree, err
}

func isParallelizable(node sql.Node) bool {
	var parallelizable = true
	var tableSeen bool
	var lastWasTable bool

	transform.Inspect(node, func(node sql.Node) bool {
		if node == nil {
			return true
		}

		lastWasTable = false
		if plan.IsBinary(node) {
			parallelizable = false
			return false
		}

		switch node := node.(type) {
		// These are the only unary nodes that can be parallelized. Any other
		// unary nodes will not.
		case *plan.TableAlias, *plan.Exchange:
		// Some nodes may have subquery expressions that make them unparallelizable
		case *plan.Project, *plan.Filter:
			for _, e := range node.(sql.Expressioner).Expressions() {
				sql.Inspect(e, func(e sql.Expression) bool {
					if q, ok := e.(*plan.Subquery); ok {
						subqueryParallelizable := true
						transform.Inspect(q.Query, func(node sql.Node) bool {
							if node == nil {
								return true
							}
							subqueryParallelizable = isParallelizable(node)
							return subqueryParallelizable
						})
						if !subqueryParallelizable {
							parallelizable = false
						}
						return true
					}
					return true
				})
			}
		// IndexedTablesAccess already uses an index for lookups, so parallelizing it won't help in most cases (and can
		// blow up the query execution graph)
		case *plan.IndexedTableAccess:
			// If this IndexedTableAccess received a process node, it is parallelizable
			if _, ok := node.ResolvedTable.Table.(*plan.ProcessTable); !ok {
				parallelizable = false
				return false
			}
			parallelizable = true
			lastWasTable = true
			tableSeen = true
		// Foreign keys expect specific nodes as children and face issues when they're swapped with Exchange nodes
		case *plan.ForeignKeyHandler:
			parallelizable = false
			return false
		case sql.Table:
			lastWasTable = true
			tableSeen = true
		default:
			parallelizable = false
			return false
		}

		return true
	})

	return parallelizable && tableSeen && lastWasTable
}
