// Copyright 2021 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// validateCreateTable validates various constraints about CREATE TABLE statements. Some validation is currently done
// at execution time, and should be moved here over time.
func validateCreateTable(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	ct, ok := n.(*plan.CreateTable)
	if !ok {
		return n, nil
	}

	err := validateAutoIncrement(ct.CreateSchema.Schema)
	if err != nil {
		return nil, err
	}

	err = validateIndexes(ct.TableSpec())
	if err != nil {
		return nil, err
	}

	return n, nil
}

func validateAlterColumn(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	if !n.Resolved() {
		return n, nil
	}

	var sch sql.Schema
	plan.Inspect(n, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.ModifyColumn:
			sch = n.Child.Schema()
			return false
		case *plan.RenameColumn:
			sch = n.Child.Schema()
			return false
		case *plan.AddColumn:
			sch = n.Child.Schema()
			return false
		case *plan.DropColumn:
			sch = n.Child.Schema()
			return false
		}
		return true
	})

	// Skip this validation if we didn't find one or more of the above node types
	if len(sch) == 0 {
		return n, nil
	}

	initialSch := sch
	var err error
	// Need a TransformUp here because multiple of these statement types can be nested under other nodes.
	// It doesn't look it, but this is actually an iterative loop over all the independent clauses in an ALTER statement
	plan.Inspect(n, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.ModifyColumn:
			sch, err = validateModifyColumn(initialSch, sch, n)
		case *plan.RenameColumn:
			sch, err = validateRenameColumn(initialSch, sch, n)
		case *plan.AddColumn:
			sch, err = validateAddColumn(initialSch, sch, n)
		case *plan.DropColumn:
			sch, err = validateDropColumn(initialSch, sch, n)
		}
		if err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return n, nil
}

// validateRenameColumn checks that a DDL RenameColumn node can be safely executed (e.g. no collision with other
// column names, doesn't invalidate any table check constraints).
//
// Note that schema is passed in twice, because one version is the initial version before the alter column expressions
// are applied, and the second version is the current schema that is being modified as multiple nodes are processed.
func validateRenameColumn(initialSch, sch sql.Schema, rc *plan.RenameColumn) (sql.Schema, error) {
	table := rc.Child
	nameable := table.(sql.Nameable)

	// Check for column name collisions
	if initialSch.Contains(rc.NewColumnName, nameable.Name()) ||
		sch.Contains(rc.NewColumnName, nameable.Name()) {
		return nil, sql.ErrColumnExists.New(rc.NewColumnName)
	}

	// Make sure this column exists and hasn't already been renamed to something else
	if !initialSch.Contains(rc.ColumnName, nameable.Name()) ||
		!sch.Contains(rc.ColumnName, nameable.Name()) {
		return nil, sql.ErrTableColumnNotFound.New(nameable.Name(), rc.ColumnName)
	}

	err := validateColumnNotUsedInCheckConstraint(rc.ColumnName, rc.Checks)
	if err != nil {
		return nil, err
	}

	return renameInSchema(sch, rc.ColumnName, rc.NewColumnName, nameable.Name()), nil
}

func validateAddColumn(initialSch sql.Schema, schema sql.Schema, ac *plan.AddColumn) (sql.Schema, error) {
	table := ac.Child
	nameable := table.(sql.Nameable)

	// Name collisions
	if initialSch.Contains(ac.Column().Name, nameable.Name()) ||
		schema.Contains(ac.Column().Name, nameable.Name()) {
		return nil, sql.ErrColumnExists.New(ac.Column().Name)
	}

	// None of the checks we do concern ordering, so we don't need to worry about it here
	newSch := append(table.Schema(), ac.Column())

	// TODO: more validation possible to do here
	err := validateAutoIncrement(newSch)
	if err != nil {
		return nil, err
	}

	return newSch, nil
}

func validateModifyColumn(intialSch sql.Schema, schema sql.Schema, mc *plan.ModifyColumn) (sql.Schema, error) {
	table := mc.Child
	nameable := table.(sql.Nameable)
	newSch := replaceInSchema(table.Schema(), mc.NewColumn(), nameable.Name())

	err := validateAutoIncrement(newSch)
	if err != nil {
		return nil, err
	}

	// TODO: When a column is being modified, we should ideally check that any existing table check constraints
	//       are still valid (e.g. if the column type changed) and throw an error if they are invalidated.
	//       That would be consistent with MySQL behavior.

	return newSch, nil
}

func validateDropColumn(initialSch, sch sql.Schema, dc *plan.DropColumn) (sql.Schema, error) {
	table := dc.Child
	nameable := table.(sql.Nameable)

	err := validateColumnNotUsedInCheckConstraint(dc.Column, dc.Checks)
	if err != nil {
		return nil, err
	}

	newSch := removeInSchema(sch, dc.Column, nameable.Name())

	return newSch, nil
}

// validateColumnNotUsedInCheckConstraint validates that the specified column name is not referenced in any of
// the specified table check constraints.
func validateColumnNotUsedInCheckConstraint(columnName string, checks sql.CheckConstraints) error {
	var err error
	for _, check := range checks {
		_ = expression.InspectUp(check.Expr, func(e sql.Expression) bool {
			if unresolvedColumn, ok := e.(*expression.UnresolvedColumn); ok {
				if columnName == unresolvedColumn.Name() {
					err = sql.ErrCheckConstraintInvalidatedByColumnAlter.New(columnName, check.Name)
					return true
				}
			}
			return false
		})

		if err != nil {
			return err
		}
	}
	return nil
}

func replaceInSchema(sch sql.Schema, col *sql.Column, tableName string) sql.Schema {
	idx := sch.IndexOf(col.Name, tableName)
	schCopy := make(sql.Schema, len(sch))
	for i := range sch {
		if i == idx {
			cc := *col
			// Some information about the column is not specified in a MODIFY COLUMN statement, such as being a key
			cc.PrimaryKey = sch[i].PrimaryKey
			cc.Source = sch[i].Source
			schCopy[i] = &cc
		} else {
			cc := *sch[i]
			schCopy[i] = &cc
		}
	}
	return schCopy
}

func renameInSchema(sch sql.Schema, oldColName, newColName, tableName string) sql.Schema {
	idx := sch.IndexOf(oldColName, tableName)
	schCopy := make(sql.Schema, len(sch))
	for i := range sch {
		if i == idx {
			cc := *sch[i]
			cc.Name = newColName
			schCopy[i] = &cc
		} else {
			cc := *sch[i]
			schCopy[i] = &cc
		}
	}
	return schCopy
}

func removeInSchema(sch sql.Schema, colName, tableName string) sql.Schema {
	idx := sch.IndexOf(colName, tableName)
	schCopy := make(sql.Schema, len(sch))
	for i := range sch {
		if i != idx {
			cc := *sch[i]
			schCopy[i] = &cc
		}
	}
	return schCopy
}

func validateAutoIncrement(schema sql.Schema) error {
	seen := false
	for _, col := range schema {
		if col.AutoIncrement {
			if !col.PrimaryKey {
				// AUTO_INCREMENT col must be a pk
				return sql.ErrInvalidAutoIncCols.New()
			}
			if col.Default != nil {
				// AUTO_INCREMENT col cannot have default
				return sql.ErrInvalidAutoIncCols.New()
			}
			if seen {
				// there can be at most one AUTO_INCREMENT col
				return sql.ErrInvalidAutoIncCols.New()
			}
			seen = true
		}
	}
	return nil
}

func validateIndexes(tableSpec *plan.TableSpec) error {
	lwrNames := make(map[string]bool)
	for _, col := range tableSpec.Schema.Schema {
		lwrNames[strings.ToLower(col.Name)] = true
	}

	for _, idx := range tableSpec.IdxDefs {
		for _, col := range idx.Columns {
			if !lwrNames[strings.ToLower(col.Name)] {
				return sql.ErrUnknownIndexColumn.New(col.Name, idx.IndexName)
			}
		}
	}

	return nil
}
