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
	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// setTargetSchemas fills in the target schema for any nodes in the tree that operate on a table node but also want to
// store supplementary schema information. This is useful for lazy resolution of column default values.
func setTargetSchemas(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("set_target_schema")
	defer span.End()

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		t, ok := n.(sql.SchemaTarget)
		if !ok {
			return n, transform.SameTree, nil
		}

		// Skip filling in target schema info for CreateTable nodes, since the
		// target schema must be provided by the user and we don't want to pick
		//  up any resolved tables from a subquery node.
		if _, ok := n.(*plan.CreateTable); ok {
			return n, transform.SameTree, nil
		}

		table := getResolvedTable(n)
		if table == nil {
			return n, transform.SameTree, nil
		}

		var err error
		n, err = t.WithTargetSchema(table.Schema())
		if err != nil {
			return nil, transform.SameTree, err
		}

		pkst, ok := n.(sql.PrimaryKeySchemaTarget)
		if !ok {
			return n, transform.NewTree, nil
		}

		pkt, ok := table.Table.(sql.PrimaryKeyTable)
		if !ok {
			return n, transform.NewTree, nil
		}

		n, err = pkst.WithPrimaryKeySchema(pkt.PrimaryKeySchema())
		return n, transform.NewTree, err
	})
}

// validateDropTables ensures that each ResolvedTable in DropTable is droppable, any UnresolvedTables are
// skipped due to `IF EXISTS` clause, and there aren't any non-table nodes.
func validateDropTables(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	dt, ok := n.(*plan.DropTable)
	if !ok {
		return n, transform.SameTree, nil
	}

	for _, table := range dt.Tables {
		switch t := table.(type) {
		case *plan.ResolvedTable:
			if _, ok := t.Database.(sql.TableDropper); !ok {
				return nil, transform.SameTree, sql.ErrDropTableNotSupported.New(t.Database.Name())
			}
		case *plan.UnresolvedTable:
			if dt.IfExists() {
				ctx.Session.Warn(&sql.Warning{
					Level:   "Note",
					Code:    mysql.ERBadTable,
					Message: sql.ErrUnknownTable.New(t.Name()).Error(),
				})
				continue
			}
			return nil, transform.SameTree, sql.ErrUnknownTable.New(t.Name())
		default:
			return nil, transform.SameTree, sql.ErrUnknownTable.New(getTableName(table))
		}
	}

	return n, transform.SameTree, nil
}
