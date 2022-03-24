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
	var indexes []string
	var err error
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
		case *plan.AlterIndex:
			sch = n.Table.Schema()
			indexes, err = getNamesOfIndexes(ctx, a, n.Table)
		case *plan.AlterPK:
			sch = n.Table.Schema()
		case *plan.AlterDefaultSet:
			sch = n.Table.Schema()
		case *plan.AlterDefaultDrop:
			sch = n.Table.Schema()
		}
		return true
	})

	if err != nil {
		return nil, err
	}

	// Skip this validation if we didn't find one or more of the above node types
	if len(sch) == 0 {
		return n, nil
	}

	sch = copySchema(sch) // Make a copy of the original schema to deal with any references to the original table.
	initialSch := sch

	// Need a TransformUp here because multiple of these statement types can be nested under other nodes.
	// It doesn't look it, but this is actually an iterative loop over all the independent clauses in an ALTER statement
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.ModifyColumn:
			sch, err = validateModifyColumn(initialSch, sch, n)
			if err != nil {
				return nil, err
			}
		case *plan.RenameColumn:
			sch, err = validateRenameColumn(initialSch, sch, n)
			if err != nil {
				return nil, err
			}
		case *plan.AddColumn:
			sch, err = validateAddColumn(initialSch, sch, n)
			if err != nil {
				return nil, err
			}
		case *plan.DropColumn:
			sch, err = validateDropColumn(initialSch, sch, n)
			if err != nil {
				return nil, err
			}
		case *plan.AlterIndex:
			indexes, err = validateAlterIndex(initialSch, sch, n, indexes)
			if err != nil {
				return nil, err
			}
		case *plan.AlterPK:
			sch, err = validatePrimaryKey(initialSch, sch, n)
			if err != nil {
				return nil, err
			}
		case *plan.AlterDefaultSet:
			sch, err = validateAlterDefault(initialSch, sch, n)
			if err != nil {
				return nil, err
			}
		case *plan.AlterDefaultDrop:
			sch, err = validateDropDefault(initialSch, sch, n)
			if err != nil {
				return nil, err
			}
		}
		return n, nil
	})
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
	newCol := copyColumn(ac.Column())
	newCol.Source = nameable.Name()
	newSch := append(schema, newCol)

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
	for _, check := range checks {
		_, err := expression.TransformUp(check.Expr, func(e sql.Expression) (sql.Expression, error) {
			if unresolvedColumn, ok := e.(*expression.UnresolvedColumn); ok {
				if columnName == unresolvedColumn.Name() {
					return nil, sql.ErrCheckConstraintInvalidatedByColumnAlter.New(columnName, check.Name)
				}
			}
			return e, nil
		})

		if err != nil {
			return err
		}
	}
	return nil
}

// validateAlterIndex validates the specified column can have an index either dropped or added to it.
func validateAlterIndex(initialSch, sch sql.Schema, ai *plan.AlterIndex, indexes []string) ([]string, error) {
	tableName := getTableName(ai.Table)

	switch ai.Action {
	case plan.IndexAction_Create:
		badColName, ok := schContainsAllIndexColumns(ai.Columns, sch, tableName)
		if !ok {
			return nil, sql.ErrColumnNotFound.New(badColName)
		}

		return append(indexes, ai.IndexName), nil
	case plan.IndexAction_Drop:
		for _, idx := range indexes {
			if strings.EqualFold(idx, ai.IndexName) {
				return indexes, nil
			}
		}

		return nil, sql.ErrCantDropFieldOrKey.New(ai.IndexName)
	case plan.IndexAction_Rename:
		savedIdx := -1
		for i, idx := range indexes {
			if strings.EqualFold(idx, ai.PreviousIndexName) {
				savedIdx = i
			}
		}

		if savedIdx == -1 {
			return nil, sql.ErrCantDropFieldOrKey.New(ai.IndexName)
		}

		// Simulate the rename by deleting the old name and adding the new one.
		return append(append(indexes[:savedIdx], indexes[savedIdx+1:]...), ai.IndexName), nil
	}

	return indexes, nil
}

func schContainsAllIndexColumns(cols []sql.IndexColumn, sch sql.Schema, tableName string) (string, bool) {
	for _, c := range cols {
		if ok := sch.Contains(c.Name, tableName); !ok {
			return c.Name, false
		}
	}

	return "", true
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
	if idx == -1 {
		return sch
	}

	schCopy := make(sql.Schema, len(sch)-1)
	for i := range sch {
		if i < idx {
			cc := *sch[i]
			schCopy[i] = &cc
		} else if i > idx {
			cc := *sch[i-1] // We want to shift stuff over.
			schCopy[i-1] = &cc
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

func getNamesOfIndexes(ctx *sql.Context, a *Analyzer, table sql.Node) ([]string, error) {
	ia, err := getIndexesForNode(ctx, a, table)
	if err != nil {
		return nil, err
	}

	indexes := ia.IndexesByTable(ctx, ctx.GetCurrentDatabase(), getTableName(table))
	names := make([]string, len(indexes))

	for i, index := range indexes {
		names[i] = index.ID()
	}

	return names, nil
}

func validatePrimaryKey(initialSch, sch sql.Schema, ai *plan.AlterPK) (sql.Schema, error) {
	tableName := getTableName(ai.Table)
	switch ai.Action {
	case plan.PrimaryKeyAction_Create:
		badColName, ok := schContainsAllIndexColumns(ai.Columns, sch, tableName)
		if !ok {
			return nil, sql.ErrKeyColumnDoesNotExist.New(badColName)
		}

		if hasPrimaryKeys(sch) {
			return nil, sql.ErrMultiplePrimaryKeysDefined.New()
		}

		// Set the primary keys
		for _, col := range ai.Columns {
			sch[sch.IndexOf(col.Name, tableName)].PrimaryKey = true
		}

		return sch, nil
	case plan.PrimaryKeyAction_Drop:
		if !hasPrimaryKeys(sch) {
			return nil, sql.ErrCantDropFieldOrKey.New("PRIMARY")
		}

		for _, col := range sch {
			if col.PrimaryKey {
				col.PrimaryKey = false
			}
		}

		return sch, nil
	default:
		return sch, nil
	}
}

func hasPrimaryKeys(sch sql.Schema) bool {
	for _, c := range sch {
		if c.PrimaryKey {
			return true
		}
	}

	return false
}

func copySchema(s sql.Schema) sql.Schema {
	newSchema := make(sql.Schema, len(s))
	for i, col := range s {
		newSchema[i] = copyColumn(col)
	}

	return newSchema
}

func copyColumn(c *sql.Column) *sql.Column {
	return &sql.Column{
		Name:          c.Name,
		Type:          c.Type,
		Default:       c.Default,
		AutoIncrement: c.AutoIncrement,
		Nullable:      c.Nullable,
		Source:        c.Source,
		PrimaryKey:    c.PrimaryKey,
		Comment:       c.Comment,
		Extra:         c.Extra,
	}
}

func validateAlterDefault(initialSch, sch sql.Schema, as *plan.AlterDefaultSet) (sql.Schema, error) {
	idx := sch.IndexOf(as.ColumnName, getTableName(as.Table))
	if idx == -1 {
		return nil, sql.ErrTableColumnNotFound.New(as.ColumnName)
	}

	copiedDefault, err := as.Default.WithChildren(as.Default.Children()...)
	if err != nil {
		return nil, err
	}

	sch[idx].Default = copiedDefault.(*sql.ColumnDefaultValue)

	return sch, err
}

func validateDropDefault(initialSch, sch sql.Schema, ad *plan.AlterDefaultDrop) (sql.Schema, error) {
	idx := sch.IndexOf(ad.ColumnName, getTableName(ad.Table))
	if idx == -1 {
		return nil, sql.ErrTableColumnNotFound.New(ad.ColumnName)
	}

	sch[idx].Default = nil

	return sch, nil
}
