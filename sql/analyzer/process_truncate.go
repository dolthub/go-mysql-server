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

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func processTruncate(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("processTruncate")
	defer span.Finish()

	deletePlan, ok := n.(*plan.DeleteFrom)
	if ok {
		if !n.Resolved() {
			return n, nil
		}
		return deleteToTruncate(ctx, a, deletePlan)
	}
	truncatePlan, ok := n.(*plan.Truncate)
	if ok {
		if !n.Resolved() {
			return nil, fmt.Errorf("cannot process TRUNCATE as node is expected to be resolved")
		}
		var db sql.Database
		var err error
		if truncatePlan.DatabaseName() == "" {
			db, err = a.Catalog.Database(ctx.GetCurrentDatabase())
			if err != nil {
				return nil, err
			}
		} else {
			db, err = a.Catalog.Database(truncatePlan.DatabaseName())
			if err != nil {
				return nil, err
			}
		}
		_, err = validateTruncate(ctx, db, truncatePlan.Child)
		if err != nil {
			return nil, err
		}
		return truncatePlan, nil
	}
	return n, nil
}

func deleteToTruncate(ctx *sql.Context, a *Analyzer, deletePlan *plan.DeleteFrom) (sql.Node, error) {
	tbl, ok := deletePlan.Child.(*plan.ResolvedTable)
	if !ok {
		return deletePlan, nil
	}
	tblName := strings.ToLower(tbl.Name())

	// auto_increment behaves differently for TRUNCATE and DELETE
	for _, col := range tbl.Schema() {
		if col.AutoIncrement {
			return deletePlan, nil
		}
	}

	tblFound := false
	currentDb, err := a.Catalog.Database(ctx.GetCurrentDatabase())
	if err != nil {
		return nil, err
	}
	dbTblNames, err := currentDb.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	for _, dbTblName := range dbTblNames {
		if strings.ToLower(dbTblName) == tblName {
			if tblFound == false {
				tblFound = true
			} else {
				return deletePlan, nil
			}
		}
	}
	if !tblFound {
		return deletePlan, nil
	}

	triggers, err := loadTriggersFromDb(ctx, currentDb)
	if err != nil {
		return nil, err
	}
	for _, trigger := range triggers {
		if trigger.TriggerEvent != sqlparser.DeleteStr {
			continue
		}
		triggerTblName, ok := trigger.Table.(*plan.UnresolvedTable)
		if !ok {
			// If we can't determine the name of the table that the trigger is on, we just abort to be safe
			return deletePlan, nil
		}
		if strings.ToLower(triggerTblName.Name()) == tblName {
			// An ON DELETE trigger is present so we can't use TRUNCATE
			return deletePlan, nil
		}
	}

	if ok, err := validateTruncate(ctx, currentDb, tbl); ok {
		// We only check err if ok is true, as some errors won't apply to us attempting to convert from a DELETE
		if err != nil {
			return nil, err
		}
		return plan.NewTruncate(ctx.GetCurrentDatabase(), tbl), nil
	}
	return deletePlan, nil
}

// validateTruncate returns whether the truncate operation adheres to the limitations as specified in
// https://dev.mysql.com/doc/refman/8.0/en/truncate-table.html. In the case of checking if a DELETE may be converted
// to a TRUNCATE operation, check the bool first. If false, then the error should be ignored (such as if the table does
// not support TRUNCATE). If true is returned along with an error, then the error is not expected to happen under
// normal circumstances and should be dealt with.
func validateTruncate(ctx *sql.Context, db sql.Database, tbl sql.Node) (bool, error) {
	truncatable, err := plan.GetTruncatable(tbl)
	if err != nil {
		return false, err // false as any caller besides Truncate would not care for this error
	}
	tableName := strings.ToLower(truncatable.Name())

	tableNames, err := db.GetTableNames(ctx)
	if err != nil {
		return true, err // true as this should not error under normal circumstances
	}
	for _, tableNameToCheck := range tableNames {
		if strings.ToLower(tableNameToCheck) == tableName {
			continue
		}
		tableToCheck, ok, err := db.GetTableInsensitive(ctx, tableNameToCheck)
		if err != nil {
			return true, err // should not error under normal circumstances
		}
		if !ok {
			return true, sql.ErrTableNotFound.New(tableNameToCheck)
		}
		fkTable, ok := tableToCheck.(sql.ForeignKeyTable)
		if ok {
			fks, err := fkTable.GetForeignKeys(ctx)
			if err != nil {
				return true, err
			}
			for _, fk := range fks {
				if strings.ToLower(fk.ReferencedTable) == tableName {
					return false, sql.ErrTruncateReferencedFromForeignKey.New(tableName, fk.Name, tableNameToCheck)
				}
			}
		}
	}
	//TODO: check for an active table lock and error if one is found for the target table
	return true, nil
}
