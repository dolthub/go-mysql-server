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
)

// RoutineTable is a Table that depends on a procedures and functions.
type RoutineTable interface {
	sql.Table

	// AssignProcedures assigns an array of procedures to the routines table.
	AssignProcedures(p map[string][]*plan.Procedure) sql.Table
	// TODO: also should assign FUNCTIONS
}

// assignRoutines sets the catalog in the required nodes.
func assignRoutines(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("assign_routines")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		if !n.Resolved() {
			return n, nil
		}

		switch node := n.(type) {
		case *plan.ResolvedTable:
			nc := *node
			ct, ok := nc.Table.(RoutineTable)

			dbs := a.Catalog.AllDatabases(ctx)
			pm := make(map[string][]*plan.Procedure)
			for _, db := range dbs {
				pm[db.Name()] = a.ProcedureCache.AllForDatabase(db.Name())
			}

			if ok {
				nc.Table = ct.AssignProcedures(pm)
			}

			return &nc, nil
		default:
			return n, nil
		}
	})
}
