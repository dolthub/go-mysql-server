// Copyright 2022 Dolthub, Inc.
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
	"strings"
)

// addAutocommitNode wraps each query with a TransactionCommittingNode.
func addAutocommitNode(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if !n.Resolved() {
		return n, transform.SameTree, nil
	}

	transactionDatabase, err := GetTransactionDatabase(ctx, n)
	if err != nil {
		return nil, transform.SameTree, err
	}

	// TODO: This is a bit of a hack. Need to figure out better relationship between new transaction node and warnings.
	if hasShowWarningsNode(n) {
		return n, transform.SameTree, nil
	}

	return plan.NewTransactionCommittingNode(n, transactionDatabase), transform.NewTree, nil
}

func hasShowWarningsNode(n sql.Node) bool {
	var ret bool
	transform.Inspect(n, func(n sql.Node) bool {
		if _, ok := n.(plan.ShowWarnings); ok {
			ret = true
			return false
		}

		return true
	})

	return ret
}

// GetTransactionDatabase returns the name of the database that should be considered current for the transaction about
// to begin. The database is not guaranteed to exist.
// For USE DATABASE statements, we consider the transaction database to be the one being USEd.
// If any errors are encountered determining the database for the transaction, an empty string and an error are returned.
func GetTransactionDatabase(ctx *sql.Context, parsed sql.Node) (string, error) {
	dbNames := make(map[string]struct{})
	transform.Inspect(parsed, func(node sql.Node) bool {
		switch n2 := node.(type) {
		case sql.Databaseable:
			if n2.Database() == "" {
				// If no database is explicitly referenced, the current db is implicit
				dbNames[ctx.GetCurrentDatabase()] = struct{}{}
			} else {
				dbNames[n2.Database()] = struct{}{}
			}
		case sql.Databaser:
			if n2.Database() == nil || n2.Database().Name() == "" {
				// If no database is explicitly referenced, the current db is implicit
				dbNames[ctx.GetCurrentDatabase()] = struct{}{}
			} else {
				dbNames[n2.Database().Name()] = struct{}{}
			}
		}
		return true
	})

	if len(dbNames) == 1 {
		for dbName := range dbNames {
			return dbName, nil
		}
	} else if len(dbNames) > 1 {
		s := make([]string, 0, len(dbNames))
		for dbName := range dbNames {
			s = append(s, dbName)
		}
		return "", sql.ErrMultipleDatabaseTransaction.New(strings.Join(s, ", "))
	}

	// If no databases were explicitly referenced, then return any session selected database
	return ctx.GetCurrentDatabase(), nil
}
