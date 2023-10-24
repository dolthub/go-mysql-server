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

package analyzer

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql/fulltext"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// matchAgainst handles the MATCH ... AGAINST ... expression, which will contain all of the information needed to
// perform relevancy calculations using the indexes.
func matchAgainst(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("match_against")
	defer span.End()

	return transform.NodeExprs(n, func(expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		matchAgainstExpr, ok := expr.(*expression.MatchAgainst)
		if !ok {
			return expr, transform.SameTree, nil
		}
		return processMatchAgainst(ctx, matchAgainstExpr, getResolvedTable(n))
	})
}

// processMatchAgainst returns a new MatchAgainst expression that has had all of its tables filled in. This essentially
// grabs the appropriate index (if it hasn't already been grabbed), and then loads the appropriate tables that are
// referenced by the index. The returned expression contains everything needed to calculate relevancy.
//
// A fully resolved MatchAgainst expression is also used by the index filter, since we only need to load the tables once.
// All steps after this one can assume that the expression has been fully resolved and is valid.
// TODO: this won't work with a virtual column
func processMatchAgainst(ctx *sql.Context, matchAgainstExpr *expression.MatchAgainst, tbl *plan.ResolvedTable) (*expression.MatchAgainst, transform.TreeIdentity, error) {
	// Grab the table
	if tbl == nil {
		return nil, transform.NewTree, fmt.Errorf("cannot use MATCH ... AGAINST ... as no table was found")
	}
	innerTbl := tbl.UnderlyingTable()
	indexedTbl, ok := innerTbl.(sql.IndexAddressableTable)
	if !ok {
		return nil, transform.NewTree, fmt.Errorf("cannot use MATCH ... AGAINST ... on a table that does not declare indexes")
	}
	if _, ok = indexedTbl.(sql.StatisticsTable); !ok {
		return nil, transform.NewTree, fmt.Errorf("cannot use MATCH ... AGAINST ... on a table that does not implement sql.StatisticsTable")
	}

	// Verify the indexes that have been set
	ftIndex := matchAgainstExpr.GetIndex()
	if ftIndex == nil {
		indexes, err := indexedTbl.GetIndexes(ctx)
		if err != nil {
			return nil, transform.NewTree, err
		}
		ftIndex = assignMatchAgainstIndex(ctx, matchAgainstExpr, &indexAnalyzer{
			indexesByTable: map[string][]sql.Index{indexedTbl.Name(): indexes},
		})
		if ftIndex == nil {
			return nil, transform.NewTree, sql.ErrNoFullTextIndexFound.New(indexedTbl.Name())
		}
	}

	// Grab the pseudo-index table names
	tableNames, err := ftIndex.FullTextTableNames(ctx)
	if err != nil {
		return nil, transform.NewTree, err
	}
	// Get the config table
	configTbl, ok, err := tbl.SqlDatabase.GetTableInsensitive(ctx, tableNames.Config)
	if err != nil {
		return nil, transform.NewTree, err
	}
	if !ok {
		return nil, transform.NewTree, fmt.Errorf("Full-Text index `%s` on table `%s` is linked to table `%s` which could not be found",
			ftIndex.ID(), indexedTbl.Name(), tableNames.Config)
	}
	indexedConfigTbl, ok := configTbl.(sql.IndexAddressableTable)
	if !ok {
		return nil, transform.NewTree, fmt.Errorf("Full-Text index `%s` on table `%s` requires table `%s` to implement sql.IndexAddressableTable",
			ftIndex.ID(), indexedTbl.Name(), tableNames.Config)
	}
	// Get the position table
	positionTbl, ok, err := tbl.SqlDatabase.GetTableInsensitive(ctx, tableNames.Position)
	if err != nil {
		return nil, transform.NewTree, err
	}
	if !ok {
		return nil, transform.NewTree, fmt.Errorf("Full-Text index `%s` on table `%s` is linked to table `%s` which could not be found",
			ftIndex.ID(), indexedTbl.Name(), tableNames.Position)
	}
	indexedPositionTbl, ok := positionTbl.(sql.IndexAddressableTable)
	if !ok {
		return nil, transform.NewTree, fmt.Errorf("Full-Text index `%s` on table `%s` requires table `%s` to implement sql.IndexAddressableTable",
			ftIndex.ID(), indexedTbl.Name(), tableNames.Position)
	}
	// Get the document count table
	docCountTbl, ok, err := tbl.SqlDatabase.GetTableInsensitive(ctx, tableNames.DocCount)
	if err != nil {
		return nil, transform.NewTree, err
	}
	if !ok {
		return nil, transform.NewTree, fmt.Errorf("Full-Text index `%s` on table `%s` is linked to table `%s` which could not be found",
			ftIndex.ID(), indexedTbl.Name(), tableNames.DocCount)
	}
	indexedDocCountTbl, ok := docCountTbl.(sql.IndexAddressableTable)
	if !ok {
		return nil, transform.NewTree, fmt.Errorf("Full-Text index `%s` on table `%s` requires table `%s` to implement sql.IndexAddressableTable",
			ftIndex.ID(), indexedTbl.Name(), tableNames.DocCount)
	}
	// Get the document count table
	globalCountTbl, ok, err := tbl.SqlDatabase.GetTableInsensitive(ctx, tableNames.GlobalCount)
	if err != nil {
		return nil, transform.NewTree, err
	}
	if !ok {
		return nil, transform.NewTree, fmt.Errorf("Full-Text index `%s` on table `%s` is linked to table `%s` which could not be found",
			ftIndex.ID(), indexedTbl.Name(), tableNames.GlobalCount)
	}
	indexedGlobalCountTbl, ok := globalCountTbl.(sql.IndexAddressableTable)
	if !ok {
		return nil, transform.NewTree, fmt.Errorf("Full-Text index `%s` on table `%s` requires table `%s` to implement sql.IndexAddressableTable",
			ftIndex.ID(), indexedTbl.Name(), tableNames.GlobalCount)
	}
	// Get the row count table
	rowCountTbl, ok, err := tbl.SqlDatabase.GetTableInsensitive(ctx, tableNames.RowCount)
	if err != nil {
		return nil, transform.NewTree, err
	}
	if !ok {
		return nil, transform.NewTree, fmt.Errorf("Full-Text index `%s` on table `%s` is linked to table `%s` which could not be found",
			ftIndex.ID(), indexedTbl.Name(), tableNames.RowCount)
	}
	indexedRowCountTbl, ok := rowCountTbl.(sql.IndexAddressableTable)
	if !ok {
		return nil, transform.NewTree, fmt.Errorf("Full-Text index `%s` on table `%s` requires table `%s` to implement sql.IndexAddressableTable",
			ftIndex.ID(), indexedTbl.Name(), tableNames.RowCount)
	}

	// Get the key columns
	keyCols, err := ftIndex.FullTextKeyColumns(ctx)
	if err != nil {
		return nil, transform.NewTree, err
	}
	return matchAgainstExpr.WithInfo(indexedTbl, indexedConfigTbl, indexedPositionTbl, indexedDocCountTbl, indexedGlobalCountTbl, indexedRowCountTbl, keyCols), transform.NewTree, nil
}

func assignMatchAgainstIndex(ctx *sql.Context, e *expression.MatchAgainst, ia *indexAnalyzer) fulltext.Index {
	if ftIndex := e.GetIndex(); ftIndex != nil {
		return ftIndex
	}
	getFields := e.ColumnsAsGetFields()
	if len(getFields) == 0 {
		return nil
	}
	ftIndexes := ia.MatchingIndexes(ctx, getFields[0].TableID(), e.Columns...)
	for _, idx := range ftIndexes {
		// Full-Text does not support partial matches, so we filter for the exact match
		if idx.IsFullText() && len(getFields) == len(idx.Expressions()) {
			// Ensure that it implements the interface
			if ftIndex, ok := idx.(fulltext.Index); ok {
				e.SetIndex(ftIndex)
				return ftIndex
			}
		}
	}
	return nil
}
