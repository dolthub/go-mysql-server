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
)

// resolveDatabase sets a database for nodes that implement sql.Databaser. Replaces sql.UnresolvedDatabase with the
// actual sql.Database implementation from the catalog.
func resolveDatabase(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("resolve_database")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		d, ok := n.(sql.Databaser)
		if !ok {
			return n, nil
		}

		var dbName = ctx.GetCurrentDatabase()
		if db := d.Database(); db != nil {
			if _, ok := db.(sql.UnresolvedDatabase); !ok {
				return n, nil
			}

			if db.Name() != "" {
				dbName = db.Name()
			}
		}

		// Nothing to resolve. This can happen if no database is current
		if dbName == "" {
			return n, nil
		}

		db, err := a.Catalog.Database(dbName)
		if err != nil {
			return nil, err
		}

		return d.WithDatabase(db)
	})
}
