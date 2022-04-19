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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func resolveCreateLike(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	ct, ok := n.(*plan.CreateTable)
	if !ok || ct.Like() == nil {
		return n, transform.SameTree, nil
	}
	resolvedLikeTable, ok := ct.Like().(*plan.ResolvedTable)
	if !ok {
		return nil, transform.SameTree, fmt.Errorf("attempted to resolve CREATE LIKE, expected `ResolvedTable` but received `%T`", ct.Like())
	}
	likeTable := resolvedLikeTable.Table
	var idxDefs []*plan.IndexDefinition
	if indexableTable, ok := likeTable.(sql.IndexedTable); ok {
		indexes, err := indexableTable.GetIndexes(ctx)
		if err != nil {
			return nil, transform.SameTree, err
		}
		for _, index := range indexes {
			if index.IsGenerated() {
				continue
			}
			constraint := sql.IndexConstraint_None
			if index.IsUnique() {
				constraint = sql.IndexConstraint_Unique
			}
			columns := make([]sql.IndexColumn, len(index.Expressions()))
			for i, col := range index.Expressions() {
				//TODO: find a better way to get only the column name if the table is present
				col = strings.TrimPrefix(col, indexableTable.Name()+".")
				columns[i] = sql.IndexColumn{
					Name:   col,
					Length: 0,
				}
			}
			idxDefs = append(idxDefs, &plan.IndexDefinition{
				IndexName:  index.ID(),
				Using:      sql.IndexUsing_Default,
				Constraint: constraint,
				Columns:    columns,
				Comment:    index.Comment(),
			})
		}
	}
	origSch := likeTable.Schema()
	newSch := make(sql.Schema, len(origSch))
	for i, col := range origSch {
		tempCol := *col
		tempCol.Source = ct.Name()
		newSch[i] = &tempCol
	}

	var pkOrdinals []int
	if pkTable, ok := likeTable.(sql.PrimaryKeyTable); ok {
		pkOrdinals = pkTable.PrimaryKeySchema().PkOrdinals
	}

	tableSpec := &plan.TableSpec{
		Schema:  sql.NewPrimaryKeySchema(newSch, pkOrdinals...),
		IdxDefs: idxDefs,
	}

	return plan.NewCreateTable(ct.Database(), ct.Name(), ct.IfNotExists(), ct.Temporary(), tableSpec), transform.NewTree, nil
}
