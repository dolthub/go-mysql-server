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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// validateCreateTable validates various constraints about CREATE TABLE statements. Some validation is currently done
// at execution time, and should be moved here over time.
func validateCreateTable(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	ct, ok := n.(*plan.CreateTable)
	if !ok {
		return n, transform.SameTree, nil
	}

	err := validateIndexes(ct.TableSpec())
	if err != nil {
		return nil, transform.SameTree, err
	}

	err = validatePkTypes(ct.TableSpec())
	if err != nil {
		return nil, transform.SameTree, err
	}

	// passed validateIndexes, so they all must be valid indexes
	// extract map of columns that have indexes defined over them
	keyedColumns := make(map[string]bool)
	for _, index := range ct.TableSpec().IdxDefs {
		for _, col := range index.Columns {
			keyedColumns[col.Name] = true
		}
	}

	err = validateAutoIncrement(ct.CreateSchema.Schema, keyedColumns)
	if err != nil {
		return nil, transform.SameTree, err
	}

	return n, transform.SameTree, nil
}

func resolveAlterColumn(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	var sch sql.Schema
	var indexes []string
	var keyedColumns map[string]bool
	var err error
	transform.Inspect(n, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.ModifyColumn:
			sch = n.Table.Schema()
			keyedColumns, err = getTableIndexColumns(ctx, n.Table)
			return false
		case *plan.RenameColumn:
			sch = n.Table.Schema()
			return false
		case *plan.AddColumn:
			sch = n.Table.Schema()
			return false
		case *plan.DropColumn:
			sch = n.Table.Schema()
			return false
		case *plan.AlterIndex:
			sch = n.Table.Schema()
			indexes, err = getTableIndexNames(ctx, a, n.Table)
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
		return nil, transform.SameTree, err
	}

	// Skip this validation if we didn't find one or more of the above node types
	if len(sch) == 0 {
		return n, transform.SameTree, nil
	}

	sch = sch.Copy() // Make a copy of the original schema to deal with any references to the original table.
	initialSch := sch

	// Need a TransformUp here because multiple of these statement types can be nested under a Block node.
	// It doesn't look it, but this is actually an iterative loop over all the independent clauses in an ALTER statement
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch nn := n.(type) {
		case *plan.ModifyColumn:
			n, err := nn.WithTargetSchema(sch.Copy())
			if err != nil {
				return nil, transform.SameTree, err
			}
			sch, err = validateModifyColumn(initialSch, sch, n.(*plan.ModifyColumn), keyedColumns)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return n, transform.NewTree, nil
		case *plan.RenameColumn:
			n, err := nn.WithTargetSchema(sch.Copy())
			if err != nil {
				return nil, transform.SameTree, err
			}
			sch, err = validateRenameColumn(initialSch, sch, n.(*plan.RenameColumn))
			if err != nil {
				return nil, transform.SameTree, err
			}
			return n, transform.NewTree, nil
		case *plan.AddColumn:
			// TODO: can't `alter table add column j int unique auto_increment` as it ignores unique
			// TODO: when above works, need to make sure unique index exists first then do what we did for modify
			n, err := nn.WithTargetSchema(sch.Copy())
			if err != nil {
				return nil, transform.SameTree, err
			}
			sch, err = validateAddColumn(initialSch, sch, n.(*plan.AddColumn))
			if err != nil {
				return nil, transform.SameTree, err
			}
			return n, transform.NewTree, nil
		case *plan.DropColumn:
			n, err := nn.WithTargetSchema(sch.Copy())
			if err != nil {
				return nil, transform.SameTree, err
			}
			sch, err = validateDropColumn(initialSch, sch, n.(*plan.DropColumn))
			if err != nil {
				return nil, transform.SameTree, err
			}
			return n, transform.NewTree, nil
		case *plan.AlterIndex:
			indexes, err = validateAlterIndex(initialSch, sch, n.(*plan.AlterIndex), indexes)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return n, transform.NewTree, nil
		case *plan.AlterPK:
			n, err := nn.WithTargetSchema(sch.Copy())
			if err != nil {
				return nil, transform.SameTree, err
			}
			sch, err = validatePrimaryKey(initialSch, sch, n.(*plan.AlterPK))
			if err != nil {
				return nil, transform.SameTree, err
			}
			return n, transform.NewTree, nil
		case *plan.AlterDefaultSet:
			n, err := nn.WithTargetSchema(sch.Copy())
			if err != nil {
				return nil, transform.SameTree, err
			}
			sch, err = validateAlterDefault(initialSch, sch, n.(*plan.AlterDefaultSet))
			if err != nil {
				return nil, transform.SameTree, err
			}
			return n, transform.NewTree, nil
		case *plan.AlterDefaultDrop:
			n, err := nn.WithTargetSchema(sch.Copy())
			if err != nil {
				return nil, transform.SameTree, err
			}
			sch, err = validateDropDefault(initialSch, sch, n.(*plan.AlterDefaultDrop))
			if err != nil {
				return nil, transform.SameTree, err
			}
			return n, transform.NewTree, nil
		}
		return n, transform.SameTree, nil
	})
}

// validateRenameColumn checks that a DDL RenameColumn node can be safely executed (e.g. no collision with other
// column names, doesn't invalidate any table check constraints).
//
// Note that schema is passed in twice, because one version is the initial version before the alter column expressions
// are applied, and the second version is the current schema that is being modified as multiple nodes are processed.
func validateRenameColumn(initialSch, sch sql.Schema, rc *plan.RenameColumn) (sql.Schema, error) {
	table := rc.Table
	nameable := table.(sql.Nameable)

	// Check for column name collisions
	if sch.Contains(rc.NewColumnName, nameable.Name()) {
		return nil, sql.ErrColumnExists.New(rc.NewColumnName)
	}

	// Make sure this column exists. MySQL only checks the original schema, which means you can't add a column and
	// rename it in the same statement. But, it also has to exist in the modified schema -- it can't have been renamed or
	// dropped in this statement.
	if !initialSch.Contains(rc.ColumnName, nameable.Name()) || !sch.Contains(rc.ColumnName, nameable.Name()) {
		return nil, sql.ErrTableColumnNotFound.New(nameable.Name(), rc.ColumnName)
	}

	err := validateColumnNotUsedInCheckConstraint(rc.ColumnName, rc.Checks)
	if err != nil {
		return nil, err
	}

	return renameInSchema(sch, rc.ColumnName, rc.NewColumnName, nameable.Name()), nil
}

func validateAddColumn(initialSch sql.Schema, schema sql.Schema, ac *plan.AddColumn) (sql.Schema, error) {
	table := ac.Table
	nameable := table.(sql.Nameable)

	// Name collisions
	if schema.Contains(ac.Column().Name, nameable.Name()) {
		return nil, sql.ErrColumnExists.New(ac.Column().Name)
	}

	// Make sure columns named in After clause exist
	idx := -1
	if ac.Order() != nil && ac.Order().AfterColumn != "" {
		afterColumn := ac.Order().AfterColumn
		idx = schema.IndexOf(afterColumn, nameable.Name())
		if idx < 0 {
			return nil, sql.ErrTableColumnNotFound.New(nameable.Name(), afterColumn)
		}
	}

	newSch := make(sql.Schema, 0, len(schema)+1)
	if idx >= 0 {
		newSch = append(newSch, schema[:idx+1]...)
		newSch = append(newSch, ac.Column().Copy())
		newSch = append(newSch, schema[idx+1:]...)
	} else { // new column at end
		newSch = append(newSch, schema...)
		newSch = append(newSch, ac.Column().Copy())
	}

	// TODO: more validation possible to do here
	err := validateAutoIncrement(newSch, nil)
	if err != nil {
		return nil, err
	}

	return newSch, nil
}

func validateModifyColumn(initialSch sql.Schema, schema sql.Schema, mc *plan.ModifyColumn, keyedColumns map[string]bool) (sql.Schema, error) {
	table := mc.Table
	nameable := table.(sql.Nameable)

	// Look for the old column and throw an error if it's not there. The column cannot have been renamed in the same
	// statement. This matches the MySQL behavior.
	if !schema.Contains(mc.Column(), nameable.Name()) ||
		!initialSch.Contains(mc.Column(), nameable.Name()) {
		return nil, sql.ErrTableColumnNotFound.New(nameable.Name(), mc.Column())
	}

	newSch := replaceInSchema(schema, mc.NewColumn(), nameable.Name())

	err := validateAutoIncrement(newSch, keyedColumns)
	if err != nil {
		return nil, err
	}

	// TODO: When a column is being modified, we should ideally check that any existing table check constraints
	//       are still valid (e.g. if the column type changed) and throw an error if they are invalidated.
	//       That would be consistent with MySQL behavior.

	return newSch, nil
}

func validateDropColumn(initialSch, sch sql.Schema, dc *plan.DropColumn) (sql.Schema, error) {
	table := dc.Table
	nameable := table.(sql.Nameable)

	// Look for the column to be dropped and throw an error if it's not there. It must exist in the original schema before
	// this statement was run, it cannot have been added as part of this ALTER TABLE statement. This matches the MySQL
	// behavior.
	if !initialSch.Contains(dc.Column, nameable.Name()) || !sch.Contains(dc.Column, nameable.Name()) {
		return nil, sql.ErrTableColumnNotFound.New(nameable.Name(), dc.Column)
	}

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
		_ = transform.InspectExpr(check.Expr, func(e sql.Expression) bool {
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

// validateAlterIndex validates the specified column can have an index added, dropped, or renamed. Returns an updated
// list of index name given the add, drop, or rename operations.
func validateAlterIndex(initialSch, sch sql.Schema, ai *plan.AlterIndex, indexes []string) ([]string, error) {
	tableName := getTableName(ai.Table)

	switch ai.Action {
	case plan.IndexAction_Create:
		badColName, ok := missingIdxColumn(ai.Columns, sch, tableName)
		if !ok {
			return nil, sql.ErrKeyColumnDoesNotExist.New(badColName)
		}

		err := validateIndexType(ai.Columns, sch)
		if err != nil {
			return nil, err
		}

		return append(indexes, ai.IndexName), nil
	case plan.IndexAction_Drop:
		savedIdx := -1
		for i, idx := range indexes {
			if strings.EqualFold(idx, ai.IndexName) {
				savedIdx = i
				break
			}
		}

		if savedIdx == -1 {
			return nil, sql.ErrCantDropFieldOrKey.New(ai.IndexName)
		}

		// Remove the index from the list
		return append(indexes[:savedIdx], indexes[savedIdx+1:]...), nil
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

// validateIndexType prevents indexing blob columns
func validateIndexType(cols []sql.IndexColumn, sch sql.Schema) error {
	for _, c := range cols {
		i := sch.IndexOfColName(c.Name)
		if sql.IsByteType(sch[i].Type) {
			return sql.ErrInvalidByteIndex.New(sch[i].Name)
		} else if sql.IsTextBlob(sch[i].Type) {
			return sql.ErrInvalidTextIndex.New(sch[i].Name)
		}
	}
	return nil
}

// missingIdxColumn takes in a set of IndexColumns and returns false, along with the offending column name, if
// an index Column is not in an index.
func missingIdxColumn(cols []sql.IndexColumn, sch sql.Schema, tableName string) (string, bool) {
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
			cc := *sch[i]
			schCopy[i-1] = &cc // We want to shift stuff over.
		}
	}
	return schCopy
}

func validateAutoIncrement(schema sql.Schema, keyedColumns map[string]bool) error {
	seen := false
	for _, col := range schema {
		if col.AutoIncrement {
			// keyedColumns == nil means they are trying to add auto_increment column
			if !col.PrimaryKey && !keyedColumns[col.Name] {
				// AUTO_INCREMENT col must be a key
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

const textIndexPrefix = 1000

func validateIndexes(tableSpec *plan.TableSpec) error {
	lwrNames := make(map[string]*sql.Column)
	for _, col := range tableSpec.Schema.Schema {
		lwrNames[strings.ToLower(col.Name)] = col
	}

	for _, idx := range tableSpec.IdxDefs {
		for _, idxCol := range idx.Columns {
			col, ok := lwrNames[strings.ToLower(idxCol.Name)]
			if !ok {
				return sql.ErrUnknownIndexColumn.New(idxCol.Name, idx.IndexName)
			}

			if sql.IsByteType(col.Type) {
				return sql.ErrInvalidByteIndex.New(col.Name)
			} else if sql.IsTextBlob(col.Type) {
				return sql.ErrInvalidTextIndex.New(col.Name)
			}
		}
	}

	return nil
}

// validatePkTypes prevents creating tables with blob primary keys
func validatePkTypes(tableSpec *plan.TableSpec) error {
	for _, col := range tableSpec.Schema.Schema {
		if col.PrimaryKey && sql.IsByteType(col.Type) {
			return sql.ErrInvalidBytePrimaryKey.New(col.Name)
		} else if col.PrimaryKey && sql.IsTextBlob(col.Type) {
			return sql.ErrInvalidTextIndex.New(col.Name)
		}
	}

	return nil
}

// getTableIndexColumns returns the columns over which indexes are defined
func getTableIndexColumns(ctx *sql.Context, table sql.Node) (map[string]bool, error) {
	ia, err := newIndexAnalyzerForNode(ctx, table)
	if err != nil {
		return nil, err
	}

	keyedColumns := make(map[string]bool)
	indexes := ia.IndexesByTable(ctx, ctx.GetCurrentDatabase(), getTableName(table))
	for _, index := range indexes {
		for _, expr := range index.Expressions() {
			if col := plan.GetColumnFromIndexExpr(expr, getTable(table)); col != nil {
				keyedColumns[col.Name] = true
			}
		}
	}

	return keyedColumns, nil
}

// getTableIndexNames returns the names of indexes associated with a table.
func getTableIndexNames(ctx *sql.Context, a *Analyzer, table sql.Node) ([]string, error) {
	ia, err := newIndexAnalyzerForNode(ctx, table)
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

// validatePrimaryKey validates a primary key add or drop operation.
func validatePrimaryKey(initialSch, sch sql.Schema, ai *plan.AlterPK) (sql.Schema, error) {
	tableName := getTableName(ai.Table)
	switch ai.Action {
	case plan.PrimaryKeyAction_Create:
		badColName, ok := missingIdxColumn(ai.Columns, sch, tableName)
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

// validateAlterDefault validates the addition of a default value to a column.
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

// validateDropDefault validates the dropping of a default value.
func validateDropDefault(initialSch, sch sql.Schema, ad *plan.AlterDefaultDrop) (sql.Schema, error) {
	idx := sch.IndexOf(ad.ColumnName, getTableName(ad.Table))
	if idx == -1 {
		return nil, sql.ErrTableColumnNotFound.New(ad.ColumnName)
	}

	sch[idx].Default = nil

	return sch, nil
}

func hasPrimaryKeys(sch sql.Schema) bool {
	for _, c := range sch {
		if c.PrimaryKey {
			return true
		}
	}

	return false
}
