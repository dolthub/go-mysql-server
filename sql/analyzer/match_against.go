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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/fulltext"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// matchAgainst handles the MATCH ... AGAINST ... expression by supplying the tables needed to calculate the necessary
// rows, along with handling special rules (such as its usage differing by position, etc.).
func matchAgainst(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("match_against")
	defer span.End()

	return transform.NodeExprs(n, func(expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		matchAgainstExpr, ok := expr.(*expression.MatchAgainst)
		if !ok {
			return expr, transform.SameTree, nil
		}

		// Grab the table
		tbl := getResolvedTable(n)
		if tbl == nil {
			return nil, transform.NewTree, fmt.Errorf("cannot use MATCH ... AGAINST ... as no table was found")
		}
		indexedTbl, ok := tbl.Table.(sql.IndexAddressableTable)
		if !ok {
			return nil, transform.NewTree, fmt.Errorf("cannot use MATCH ... AGAINST ... on a table that does not declare indexes")
		}

		// Verify the schema
		docIdColPos, parentColMap, err := fulltext.GetDocIdColPos(indexedTbl.Schema())
		if err != nil {
			return nil, transform.NewTree, err
		}
		if docIdColPos == -1 {
			return nil, transform.NewTree, fmt.Errorf("cannot use MATCH ... AGAINST ... on a table that is missing the `%s` column", fulltext.IDColumnName)
		}

		// Verify the indexes that have been set
		docIdIndex, ftIndex := matchAgainstExpr.GetIndexes()
		if docIdIndex == nil {
			return nil, transform.NewTree, fmt.Errorf("table `%s` is missing a unique index over its `%s` column", indexedTbl.Name(), fulltext.IDColumnName)
		}
		if ftIndex == nil {
			return nil, transform.NewTree, fmt.Errorf("no matching Full-Text index found on table `%s`", indexedTbl.Name())
		}

		// Map the source columns
		var ftSourceCols []int //TODO: check if this is necessary
		indexExprs := ftIndex.Expressions()
		sourceCols := make([]int, len(indexExprs))
		for i, expr := range indexExprs {
			sourceCols[i], ok = parentColMap[strings.ToLower(expr)]
			if !ok {
				return nil, transform.NewTree, fmt.Errorf("table `%s` FULLTEXT index `%s` references the column `%s` but it could not be found",
					indexedTbl.Name(), ftIndex.ID(), expr)
			}
		}

		// Grab the pseudo-index table names
		configTblName, wordToPosTblName, countTblName, ok := ftIndex.FullTextTableNames(ctx)
		if !ok {
			return nil, transform.NewTree, fmt.Errorf("Full-Text index `%s` on table `%s` reports missing tables", ftIndex.ID(), indexedTbl.Name())
		}
		// Get the config table
		configTbl, ok, err := tbl.Database.GetTableInsensitive(ctx, configTblName)
		if err != nil {
			return nil, transform.NewTree, err
		}
		if !ok {
			return nil, transform.NewTree, fmt.Errorf("Full-Text index `%s` on table `%s` is linked to table `%s` which could not be found",
				ftIndex.ID(), indexedTbl.Name(), configTblName)
		}
		indexedConfigTbl, ok := configTbl.(sql.IndexAddressableTable)
		if !ok {
			return nil, transform.NewTree, fmt.Errorf("Full-Text index `%s` on table `%s` requires table `%s` to implement sql.IndexAddressableTable",
				ftIndex.ID(), indexedTbl.Name(), configTblName)
		}
		// Get the word to position table
		wordToPosTbl, ok, err := tbl.Database.GetTableInsensitive(ctx, wordToPosTblName)
		if err != nil {
			return nil, transform.NewTree, err
		}
		if !ok {
			return nil, transform.NewTree, fmt.Errorf("Full-Text index `%s` on table `%s` is linked to table `%s` which could not be found",
				ftIndex.ID(), indexedTbl.Name(), wordToPosTblName)
		}
		indexedWordToPosTbl, ok := wordToPosTbl.(sql.IndexAddressableTable)
		if !ok {
			return nil, transform.NewTree, fmt.Errorf("Full-Text index `%s` on table `%s` requires table `%s` to implement sql.IndexAddressableTable",
				ftIndex.ID(), indexedTbl.Name(), wordToPosTblName)
		}
		// Get the word to position table
		countTbl, ok, err := tbl.Database.GetTableInsensitive(ctx, countTblName)
		if err != nil {
			return nil, transform.NewTree, err
		}
		if !ok {
			return nil, transform.NewTree, fmt.Errorf("Full-Text index `%s` on table `%s` is linked to table `%s` which could not be found",
				ftIndex.ID(), indexedTbl.Name(), countTblName)
		}
		indexedCountTbl, ok := countTbl.(sql.IndexAddressableTable)
		if !ok {
			return nil, transform.NewTree, fmt.Errorf("Full-Text index `%s` on table `%s` requires table `%s` to implement sql.IndexAddressableTable",
				ftIndex.ID(), indexedTbl.Name(), countTblName)
		}
		ftSourceCols = ftSourceCols //TODO: this might not be needed
		return matchAgainstExpr.WithInfo(indexedTbl, indexedConfigTbl, indexedWordToPosTbl, indexedCountTbl, docIdColPos), transform.NewTree, nil
	})
}
