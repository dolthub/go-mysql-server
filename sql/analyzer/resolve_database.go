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

// resolveDatabases sets a database for nodes that implement sql.Databaser. Replaces sql.UnresolvedDatabase with the
// actual sql.Database implementation from the catalog.
func resolveDatabases(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, transform.TreeIdentity, error) {
	span, _ := ctx.Span("resolve_database")
	defer span.Finish()

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		d, ok := n.(sql.Databaser)
		if !ok {
			return n, transform.SameTree, nil
		}

		var dbName = ctx.GetCurrentDatabase()
		if db := d.Database(); db != nil {
			if _, ok := db.(sql.UnresolvedDatabase); !ok {
				return n, transform.SameTree, nil
			}

			if db.Name() != "" {
				dbName = db.Name()
			}
		}

		// Nothing to resolve. This can happen if no database is current
		if dbName == "" {
			return n, transform.SameTree, nil
		}

		db, err := a.Catalog.Database(ctx, dbName)
		if err != nil {
			return nil, transform.SameTree, err
		}

		n, err = d.WithDatabase(db)
		return n, transform.NewTree, err
	})
}

// validateDatabaseSet returns an error if any database node that requires a database doesn't have one
func validateDatabaseSet(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, transform.TreeIdentity, error) {
	var err error
	transform.Inspect(n, func(node sql.Node) bool {
		switch n.(type) {
		// TODO: there are probably other kinds of nodes that need this too
		case *plan.ShowTables, *plan.ShowTriggers, *plan.CreateTable:
			n := n.(sql.Databaser)
			if _, ok := n.Database().(sql.UnresolvedDatabase); ok {
				err = sql.ErrNoDatabaseSelected.New()
				return false
			}
		}
		return true
	})
	if err != nil {
		return nil, transform.SameTree, err
	}

	return n, transform.SameTree, nil
}
