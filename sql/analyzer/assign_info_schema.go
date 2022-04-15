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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// loadInfoSchema is a grab-bag analyzer function to assign information schema info
// to any plan nodes that need it, like various SHOW *  statements. The logic for
// each node is necessarily pretty custom.
func loadInfoSchema(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch x := n.(type) {
		case *plan.ShowIndexes:
			tableIndexes, err := getIndexesForTable(ctx, a, x.Child)
			if err != nil {
				return nil, transform.SameTree, err
			}

			x.IndexesToShow = filterGeneratedIndexes(tableIndexes)
			return x, transform.NewTree, nil
		case *plan.ShowCreateTable:
			if !x.IsView {
				tableIndexes, err := getIndexesForTable(ctx, a, x.Child)
				if err != nil {
					return nil, transform.SameTree, err
				}

				x.Indexes = filterGeneratedIndexes(tableIndexes)
				return x, transform.NewTree, nil
			}
		case *plan.ShowColumns:
			tableIndexes, err := getIndexesForTable(ctx, a, x.Child)
			if err != nil {
				return nil, transform.SameTree, err
			}

			x.Indexes = filterGeneratedIndexes(tableIndexes)
			return x, transform.NewTree, nil

		case *plan.ShowCharset:
			rt, err := getInformationSchemaTable(ctx, a, "character_sets")
			if err != nil {
				return nil, transform.SameTree, err
			}

			x.CharacterSetTable = rt
			return x, transform.NewTree, nil
		default:
		}
		return n, transform.SameTree, nil
	})
}

// filterGeneratedIndexes removes all generated indexes from a slice of indexes.
func filterGeneratedIndexes(indexes []sql.Index) []sql.Index {
	var newIndexes []sql.Index
	for _, index := range indexes {
		if !index.IsGenerated() {
			newIndexes = append(newIndexes, index)
		}
	}
	return newIndexes
}

// getIndexesForTable returns all indexes on the table represented by the node given. If the node isn't a
// *(plan.ResolvedTable), returns an empty slice.
func getIndexesForTable(ctx *sql.Context, a *Analyzer, node sql.Node) ([]sql.Index, error) {
	ia, err := newIndexAnalyzerForNode(ctx, node)
	if err != nil {
		return nil, err
	}

	var tableName string
	if rt, ok := node.(*plan.ResolvedTable); ok {
		tableName = rt.Name()
	}

	// TODO: get the DB out of the table, don't just use the current DB
	tableIndexes := ia.IndexesByTable(ctx, ctx.GetCurrentDatabase(), tableName)
	return tableIndexes, nil
}

// getInformationSchemaTable returns a table that is present in the information_schema.
func getInformationSchemaTable(ctx *sql.Context, a *Analyzer, tableName string) (sql.Node, error) {
	rt, database, err := a.Catalog.Table(ctx, "information_schema", tableName)
	if err != nil {
		return nil, err
	}

	a.Log("table resolved: %s", rt.Name())
	return plan.NewResolvedTable(rt, database, nil), nil
}
