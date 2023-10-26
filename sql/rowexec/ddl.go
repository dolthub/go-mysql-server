// Copyright 2023 Dolthub, Inc.
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

package rowexec

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/go-mysql-server/internal/similartext"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/fulltext"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func (b *BaseBuilder) buildAlterAutoIncrement(ctx *sql.Context, n *plan.AlterAutoIncrement, row sql.Row) (sql.RowIter, error) {
	err := b.executeAlterAutoInc(ctx, n)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

func (b *BaseBuilder) buildDropTrigger(ctx *sql.Context, n *plan.DropTrigger, row sql.Row) (sql.RowIter, error) {
	triggerDb, ok := n.Db.(sql.TriggerDatabase)
	if !ok {
		if n.IfExists {
			return sql.RowsToRowIter(), nil
		} else {
			return nil, sql.ErrTriggerDoesNotExist.New(n.TriggerName)
		}
	}
	err := triggerDb.DropTrigger(ctx, n.TriggerName)
	if n.IfExists && sql.ErrTriggerDoesNotExist.Is(err) {
		return sql.RowsToRowIter(), nil
	} else if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (b *BaseBuilder) buildLoadData(ctx *sql.Context, n *plan.LoadData, row sql.Row) (sql.RowIter, error) {
	// Start the parsing by grabbing all the config variables.
	err := n.SetParsingValues()
	if err != nil {
		return nil, err
	}

	var reader io.ReadCloser

	if n.Local {
		_, localInfile, ok := sql.SystemVariables.GetGlobal("local_infile")
		if !ok {
			return nil, fmt.Errorf("error: local_infile variable was not found")
		}

		if localInfile.(int8) == 0 {
			return nil, fmt.Errorf("local_infile needs to be set to 1 to use LOCAL")
		}

		reader, err = ctx.LoadInfile(n.File)
		if err != nil {
			return nil, err
		}
	} else {
		_, dir, ok := sql.SystemVariables.GetGlobal("secure_file_priv")
		if !ok {
			return nil, fmt.Errorf("error: secure_file_priv variable was not found")
		}
		if dir == nil {
			dir = ""
		}

		fileName := filepath.Join(dir.(string), n.File)
		file, err := os.Open(fileName)
		if err != nil {
			return nil, sql.ErrLoadDataCannotOpen.New(err.Error())
		}
		reader = file
	}

	scanner := bufio.NewScanner(reader)

	// Set the split function for lines.
	scanner.Split(n.SplitLines)

	// Skip through the lines that need to be ignored.
	for n.IgnoreNum > 0 && scanner.Scan() {
		scanner.Text()
		n.IgnoreNum--
	}

	if scanner.Err() != nil {
		reader.Close()
		return nil, scanner.Err()
	}

	sch := n.Schema()
	source := sch[0].Source // Schema will always have at least one column
	columnNames := n.ColumnNames
	if len(columnNames) == 0 {
		columnNames = make([]string, len(sch))
		for i, col := range sch {
			columnNames[i] = col.Name
		}
	}
	fieldToColumnMap := make([]int, len(columnNames))
	for fieldIndex, columnName := range columnNames {
		fieldToColumnMap[fieldIndex] = sch.IndexOf(columnName, source)
	}

	return &loadDataIter{
		destSch:                 n.DestSch,
		reader:                  reader,
		scanner:                 scanner,
		columnCount:             len(n.ColumnNames), // Needs to be the original column count
		fieldToColumnMap:        fieldToColumnMap,
		fieldsTerminatedByDelim: n.FieldsTerminatedByDelim,
		fieldsEnclosedByDelim:   n.FieldsEnclosedByDelim,
		fieldsOptionallyDelim:   n.FieldsOptionallyDelim,
		fieldsEscapedByDelim:    n.FieldsEscapedByDelim,
		linesTerminatedByDelim:  n.LinesTerminatedByDelim,
		linesStartingByDelim:    n.LinesStartingByDelim,
	}, nil
}

func (b *BaseBuilder) buildDropConstraint(ctx *sql.Context, n *plan.DropConstraint, row sql.Row) (sql.RowIter, error) {
	// DropConstraint should be replaced by another node type (DropForeignKey, DropCheck, etc.) during analysis,
	// so this is an error
	return nil, fmt.Errorf("%T does not have an execution iterator, this is a bug", n)
}

func (b *BaseBuilder) buildCreateView(ctx *sql.Context, n *plan.CreateView, row sql.Row) (sql.RowIter, error) {
	registry := ctx.GetViewRegistry()
	if n.IsReplace {
		if dropper, ok := n.Database().(sql.ViewDatabase); ok {
			err := dropper.DropView(ctx, n.Name)
			if err != nil && !sql.ErrViewDoesNotExist.Is(err) {
				return sql.RowsToRowIter(), err
			}
		} else {
			err := registry.Delete(n.Database().Name(), n.Name)
			if err != nil && !sql.ErrViewDoesNotExist.Is(err) {
				return sql.RowsToRowIter(), err
			}
		}
	}
	names, err := n.Database().GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		if strings.ToLower(name) == strings.ToLower(n.Name) {
			return nil, sql.ErrTableAlreadyExists.New(n)
		}
	}

	// TODO: isUpdatable should be defined at CREATE VIEW time
	// isUpdatable := GetIsUpdatableFromCreateView(cv)
	creator, ok := n.Database().(sql.ViewDatabase)
	if ok {
		return sql.RowsToRowIter(), creator.CreateView(ctx, n.Name, n.Definition.TextDefinition, n.CreateViewString)
	} else {
		return sql.RowsToRowIter(), registry.Register(n.Database().Name(), n.View())
	}
}

func (b *BaseBuilder) buildCreateCheck(ctx *sql.Context, n *plan.CreateCheck, row sql.Row) (sql.RowIter, error) {
	err := b.executeCreateCheck(ctx, n)
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(sql.Row{types.NewOkResult(0)}), nil
}

func (b *BaseBuilder) buildAlterDefaultSet(ctx *sql.Context, n *plan.AlterDefaultSet, row sql.Row) (sql.RowIter, error) {
	// Grab the table fresh from the database.
	table, err := getTableFromDatabase(ctx, n.Database(), n.Table)
	if err != nil {
		return nil, err
	}

	alterable, ok := table.(sql.AlterableTable)
	if !ok {
		return nil, sql.ErrAlterTableNotSupported.New(n.Table)
	}

	if err != nil {
		return nil, err
	}
	loweredColName := strings.ToLower(n.ColumnName)
	var col *sql.Column
	for _, schCol := range alterable.Schema() {
		if strings.ToLower(schCol.Name) == loweredColName {
			col = schCol
			break
		}
	}
	if col == nil {
		return nil, sql.ErrTableColumnNotFound.New(n.Table, n.ColumnName)
	}
	newCol := &(*col)
	newCol.Default = n.Default
	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), alterable.ModifyColumn(ctx, n.ColumnName, newCol, nil)
}

func (b *BaseBuilder) buildDropCheck(ctx *sql.Context, n *plan.DropCheck, row sql.Row) (sql.RowIter, error) {
	err := b.executeDropCheck(ctx, n)
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (b *BaseBuilder) buildRenameTable(ctx *sql.Context, n *plan.RenameTable, row sql.Row) (sql.RowIter, error) {
	return n.RowIter(ctx, row)
}

func (b *BaseBuilder) buildModifyColumn(ctx *sql.Context, n *plan.ModifyColumn, row sql.Row) (sql.RowIter, error) {
	tbl, err := getTableFromDatabase(ctx, n.Database(), n.Table)
	if err != nil {
		return nil, err
	}

	alterable, ok := tbl.(sql.AlterableTable)
	if !ok {
		return nil, sql.ErrAlterTableNotSupported.New(tbl.Name())
	}

	if err := n.ValidateDefaultPosition(n.TargetSchema()); err != nil {
		return nil, err
	}
	// MySQL assigns the column's type (which contains the collation) at column creation/modification. If a column has
	// an invalid collation, then one has not been assigned at this point, so we assign it the table's collation. This
	// does not create a reference to the table's collation, which may change at any point, and therefore will have no
	// relation to this column after assignment.
	if collatedType, ok := n.NewColumn().Type.(sql.TypeWithCollation); ok && collatedType.Collation() == sql.Collation_Unspecified {
		n.NewColumn().Type, err = collatedType.WithNewCollation(alterable.Collation())
		if err != nil {
			return nil, err
		}
	}
	for _, col := range n.TargetSchema() {
		if collatedType, ok := col.Type.(sql.TypeWithCollation); ok && collatedType.Collation() == sql.Collation_Unspecified {
			col.Type, err = collatedType.WithNewCollation(alterable.Collation())
			if err != nil {
				return nil, err
			}
		}
	}

	return &modifyColumnIter{
		m:         n,
		alterable: alterable,
	}, nil
}

func (b *BaseBuilder) buildSingleDropView(ctx *sql.Context, n *plan.SingleDropView, row sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

func (b *BaseBuilder) buildCreateIndex(ctx *sql.Context, n *plan.CreateIndex, row sql.Row) (sql.RowIter, error) {
	table, ok := n.Table.(*plan.ResolvedTable)
	if !ok {
		return nil, plan.ErrNotIndexable.New()
	}

	indexable, err := getIndexableTable(table.Table)
	if err != nil {
		return nil, err
	}

	var driver sql.IndexDriver
	if n.Driver == "" {
		driver = ctx.GetIndexRegistry().DefaultIndexDriver()
	} else {
		driver = ctx.GetIndexRegistry().IndexDriver(n.Driver)
	}

	if driver == nil {
		return nil, plan.ErrInvalidIndexDriver.New(n.Driver)
	}

	columns, exprs, err := GetColumnsAndPrepareExpressions(n.Exprs)
	if err != nil {
		return nil, err
	}

	for _, e := range exprs {
		if types.IsBlobType(e.Type()) || types.IsJSON(e.Type()) {
			return nil, plan.ErrExprTypeNotIndexable.New(e, e.Type())
		}
	}

	if ch := getChecksumable(table.Table); ch != nil {
		n.Config[sql.ChecksumKey], err = ch.Checksum()
		if err != nil {
			return nil, err
		}
	}

	index, err := driver.Create(
		n.CurrentDatabase,
		table.Name(),
		n.Name,
		exprs,
		n.Config,
	)
	if err != nil {
		return nil, err
	}

	iter, err := indexable.IndexKeyValues(ctx, columns)
	if err != nil {
		return nil, err
	}

	iter = &EvalPartitionKeyValueIter{
		columns: columns,
		exprs:   exprs,
		iter:    iter,
	}

	created, ready, err := ctx.GetIndexRegistry().AddIndex(index)
	if err != nil {
		return nil, err
	}

	log := logrus.WithFields(logrus.Fields{
		"id":     index.ID(),
		"driver": index.Driver(),
	})

	createIndex := func() {
		createIndex(ctx, log, driver, index, iter, created, ready)
	}

	log.Info("starting to save the index")

	createIndex()

	return sql.RowsToRowIter(), nil
}

func (b *BaseBuilder) buildDeclareCondition(ctx *sql.Context, n *plan.DeclareCondition, row sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

func (b *BaseBuilder) buildCreateDB(ctx *sql.Context, n *plan.CreateDB, row sql.Row) (sql.RowIter, error) {
	exists := n.Catalog.HasDatabase(ctx, n.DbName)
	rows := []sql.Row{{types.OkResult{RowsAffected: 1}}}

	if exists {
		if n.IfNotExists {
			ctx.Session.Warn(&sql.Warning{
				Level:   "Note",
				Code:    mysql.ERDbCreateExists,
				Message: fmt.Sprintf("Can't create database %s; database exists ", n.DbName),
			})

			return sql.RowsToRowIter(rows...), nil
		} else {
			return nil, sql.ErrDatabaseExists.New(n.DbName)
		}
	}

	collation := n.Collation
	if collation == sql.Collation_Unspecified {
		collation = sql.Collation_Default
	}
	err := n.Catalog.CreateDatabase(ctx, n.DbName, collation)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(rows...), nil
}

func (b *BaseBuilder) buildAlterDefaultDrop(ctx *sql.Context, n *plan.AlterDefaultDrop, row sql.Row) (sql.RowIter, error) {
	table, ok, err := n.Db.GetTableInsensitive(ctx, getTableName(n.Table))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(n.Table)
	}

	alterable, ok := table.(sql.AlterableTable)
	loweredColName := strings.ToLower(n.ColumnName)
	var col *sql.Column
	for _, schCol := range alterable.Schema() {
		if strings.ToLower(schCol.Name) == loweredColName {
			col = schCol
			break
		}
	}

	if col == nil {
		return nil, sql.ErrTableColumnNotFound.New(getTableName(n.Table), n.ColumnName)
	}
	newCol := &(*col)
	newCol.Default = nil
	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), alterable.ModifyColumn(ctx, n.ColumnName, newCol, nil)
}

func (b *BaseBuilder) buildDropView(ctx *sql.Context, n *plan.DropView, row sql.Row) (sql.RowIter, error) {
	for _, child := range n.Children() {
		drop, ok := child.(*plan.SingleDropView)
		if !ok {
			return sql.RowsToRowIter(), plan.ErrDropViewChild.New()
		}

		if dropper, ok := drop.Database().(sql.ViewDatabase); ok {
			err := dropper.DropView(ctx, drop.ViewName)
			if err != nil {
				allowedError := n.IfExists && sql.ErrViewDoesNotExist.Is(err)
				if !allowedError {
					return sql.RowsToRowIter(), err
				}
			}
		} else {
			err := ctx.GetViewRegistry().Delete(drop.Database().Name(), drop.ViewName)
			allowedError := n.IfExists && sql.ErrViewDoesNotExist.Is(err)
			if !allowedError {
				return sql.RowsToRowIter(), err
			}
		}
	}

	return sql.RowsToRowIter(), nil
}

func (b *BaseBuilder) buildCreateUser(ctx *sql.Context, n *plan.CreateUser, row sql.Row) (sql.RowIter, error) {
	mysqlDb, ok := n.MySQLDb.(*mysql_db.MySQLDb)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}

	editor := mysqlDb.Editor()
	defer editor.Close()

	for _, user := range n.Users {
		// replace empty host with any host
		if user.UserName.Host == "" {
			user.UserName.Host = "%"
		}

		userPk := mysql_db.UserPrimaryKey{
			Host: user.UserName.Host,
			User: user.UserName.Name,
		}
		_, ok := editor.GetUser(userPk)
		if ok {
			if n.IfNotExists {
				continue
			}
			return nil, sql.ErrUserCreationFailure.New(user.UserName.String("'"))
		}

		plugin := "mysql_native_password"
		password := ""
		if user.Auth1 != nil {
			plugin = user.Auth1.Plugin()
			password = user.Auth1.Password()
		}
		if plugin != "mysql_native_password" {
			if err := mysqlDb.VerifyPlugin(plugin); err != nil {
				return nil, sql.ErrUserCreationFailure.New(err)
			}
		}

		// TODO: attributes should probably not be nil, but setting it to &n.Attribute causes unexpected behavior
		// TODO: validate all of the data
		editor.PutUser(&mysql_db.User{
			User:                user.UserName.Name,
			Host:                user.UserName.Host,
			PrivilegeSet:        mysql_db.NewPrivilegeSet(),
			Plugin:              plugin,
			Password:            password,
			PasswordLastChanged: time.Now().UTC(),
			Locked:              false,
			Attributes:          nil,
			IsRole:              false,
			Identity:            user.Identity,
		})
	}
	if err := mysqlDb.Persist(ctx, editor); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(sql.Row{types.NewOkResult(0)}), nil
}

func (b *BaseBuilder) buildAlterPK(ctx *sql.Context, n *plan.AlterPK, row sql.Row) (sql.RowIter, error) {
	// We need to get the current table from the database because this statement could be one clause in an alter table
	// statement and the table may have changed since the analysis phase
	table, err := getTableFromDatabase(ctx, n.Database(), n.Table)
	if err != nil {
		return nil, err
	}

	// TODO: these validation checks belong in the analysis phase, not here
	pkAlterable, ok := table.(sql.PrimaryKeyAlterableTable)
	if !ok {
		return nil, plan.ErrNotPrimaryKeyAlterable.New(n.Table)
	}
	if err != nil {
		return nil, err
	}

	switch n.Action {
	case plan.PrimaryKeyAction_Create:
		if plan.HasPrimaryKeys(pkAlterable) {
			return sql.RowsToRowIter(), sql.ErrMultiplePrimaryKeysDefined.New()
		}

		for _, c := range n.Columns {
			if !pkAlterable.Schema().Contains(c.Name, pkAlterable.Name()) {
				return sql.RowsToRowIter(), sql.ErrKeyColumnDoesNotExist.New(c.Name)
			}
		}

		return &createPkIter{
			targetSchema: n.TargetSchema(),
			columns:      n.Columns,
			pkAlterable:  pkAlterable,
			db:           n.Database(),
		}, nil
	case plan.PrimaryKeyAction_Drop:
		return &dropPkIter{
			targetSchema: n.TargetSchema(),
			pkAlterable:  pkAlterable,
			db:           n.Database(),
		}, nil
	default:
		panic("unreachable")
	}
}

func (b *BaseBuilder) buildDropIndex(ctx *sql.Context, n *plan.DropIndex, row sql.Row) (sql.RowIter, error) {
	db, err := n.Catalog.Database(ctx, n.CurrentDatabase)
	if err != nil {
		return nil, err
	}

	nn, ok := n.Table.(sql.Nameable)
	if !ok {
		return nil, plan.ErrTableNotNameable.New()
	}

	table, ok, err := db.GetTableInsensitive(ctx, nn.Name())

	if err != nil {
		return nil, err
	}

	if !ok {
		tableNames, err := db.GetTableNames(ctx)

		if err != nil {
			return nil, err
		}

		similar := similartext.Find(tableNames, nn.Name())
		return nil, sql.ErrTableNotFound.New(nn.Name() + similar)
	}

	index := ctx.GetIndexRegistry().Index(db.Name(), n.Name)
	if index == nil {
		return nil, plan.ErrIndexNotFound.New(n.Name, nn.Name(), db.Name())
	}
	ctx.GetIndexRegistry().ReleaseIndex(index)

	if !ctx.GetIndexRegistry().CanRemoveIndex(index) {
		return nil, plan.ErrIndexNotAvailable.New(n.Name)
	}

	done, err := ctx.GetIndexRegistry().DeleteIndex(db.Name(), n.Name, true)
	if err != nil {
		return nil, err
	}

	driver := ctx.GetIndexRegistry().IndexDriver(index.Driver())
	if driver == nil {
		return nil, plan.ErrInvalidIndexDriver.New(index.Driver())
	}

	<-done

	partitions, err := table.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	if err := driver.Delete(index, partitions); err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

func (b *BaseBuilder) buildDropProcedure(ctx *sql.Context, n *plan.DropProcedure, row sql.Row) (sql.RowIter, error) {
	procDb, ok := n.Db.(sql.StoredProcedureDatabase)
	if !ok {
		if n.IfExists {
			return sql.RowsToRowIter(), nil
		} else {
			return nil, sql.ErrStoredProceduresNotSupported.New(n.ProcedureName)
		}
	}
	err := procDb.DropStoredProcedure(ctx, n.ProcedureName)
	if n.IfExists && sql.ErrStoredProcedureDoesNotExist.Is(err) {
		return sql.RowsToRowIter(), nil
	} else if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (b *BaseBuilder) buildDropDB(ctx *sql.Context, n *plan.DropDB, row sql.Row) (sql.RowIter, error) {
	exists := n.Catalog.HasDatabase(ctx, n.DbName)
	if !exists {
		if n.IfExists {
			ctx.Session.Warn(&sql.Warning{
				Level:   "Note",
				Code:    mysql.ERDbDropExists,
				Message: fmt.Sprintf("Can't drop database %s; database doesn't exist ", n.DbName),
			})

			rows := []sql.Row{{types.OkResult{RowsAffected: 0}}}

			return sql.RowsToRowIter(rows...), nil
		} else {
			return nil, sql.ErrDatabaseNotFound.New(n.DbName)
		}
	}

	// make sure to notify the EventSchedulerStatus before dropping the database
	if n.EventScheduler != nil {
		n.EventScheduler.RemoveSchemaEvents(n.DbName)
	}

	err := n.Catalog.RemoveDatabase(ctx, n.DbName)
	if err != nil {
		return nil, err
	}

	// Unsets the current database. Database name is case-insensitive.
	if strings.ToLower(ctx.GetCurrentDatabase()) == strings.ToLower(n.DbName) {
		ctx.SetCurrentDatabase("")
	}

	rows := []sql.Row{{types.OkResult{RowsAffected: 1}}}

	return sql.RowsToRowIter(rows...), nil
}

func (b *BaseBuilder) buildRenameColumn(ctx *sql.Context, n *plan.RenameColumn, row sql.Row) (sql.RowIter, error) {
	tbl, err := getTableFromDatabase(ctx, n.Database(), n.Table)
	if err != nil {
		return nil, err
	}

	alterable, ok := tbl.(sql.AlterableTable)
	if !ok {
		return nil, sql.ErrAlterTableNotSupported.New(tbl.Name())
	}

	idx := n.TargetSchema().IndexOf(n.ColumnName, tbl.Name())
	if idx < 0 {
		return nil, sql.ErrTableColumnNotFound.New(tbl.Name(), n.ColumnName)
	}

	nc := *n.TargetSchema()[idx]
	nc.Name = n.NewColumnName
	col := &nc

	if err := updateDefaultsOnColumnRename(ctx, alterable, n.TargetSchema(), strings.ToLower(n.ColumnName), n.NewColumnName); err != nil {
		return nil, err
	}

	// Update the foreign key columns as well
	if fkTable, ok := alterable.(sql.ForeignKeyTable); ok {
		parentFks, err := fkTable.GetReferencedForeignKeys(ctx)
		if err != nil {
			return nil, err
		}
		fks, err := fkTable.GetDeclaredForeignKeys(ctx)
		if err != nil {
			return nil, err
		}
		if len(parentFks) > 0 || len(fks) > 0 {
			err = handleFkColumnRename(ctx, fkTable, n.Db, n.ColumnName, n.NewColumnName)
			if err != nil {
				return nil, err
			}
		}
	}

	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), alterable.ModifyColumn(ctx, n.ColumnName, col, nil)
}

func (b *BaseBuilder) buildAddColumn(ctx *sql.Context, n *plan.AddColumn, row sql.Row) (sql.RowIter, error) {
	table, err := getTableFromDatabase(ctx, n.Database(), n.Table)
	if err != nil {
		return nil, err
	}

	alterable, ok := table.(sql.AlterableTable)
	if !ok {
		return nil, sql.ErrAlterTableNotSupported.New(table.Name())
	}

	tbl := alterable.(sql.Table)
	tblSch := n.TargetSchema()
	if n.Order() != nil && !n.Order().First {
		idx := tblSch.IndexOf(n.Order().AfterColumn, tbl.Name())
		if idx < 0 {
			return nil, sql.ErrTableColumnNotFound.New(tbl.Name(), n.Order().AfterColumn)
		}
	}

	if err := n.ValidateDefaultPosition(tblSch); err != nil {
		return nil, err
	}
	// MySQL assigns the column's type (which contains the collation) at column creation/modification. If a column has
	// an invalid collation, then one has not been assigned at this point, so we assign it the table's collation. This
	// does not create a reference to the table's collation, which may change at any point, and therefore will have no
	// relation to this column after assignment.
	if collatedType, ok := n.Column().Type.(sql.TypeWithCollation); ok && collatedType.Collation() == sql.Collation_Unspecified {
		n.Column().Type, err = collatedType.WithNewCollation(alterable.Collation())
		if err != nil {
			return nil, err
		}
	}
	for _, col := range n.TargetSchema() {
		if collatedType, ok := col.Type.(sql.TypeWithCollation); ok && collatedType.Collation() == sql.Collation_Unspecified {
			col.Type, err = collatedType.WithNewCollation(alterable.Collation())
			if err != nil {
				return nil, err
			}
		}
	}

	return &addColumnIter{
		a:         n,
		alterable: alterable,
		b:         b,
	}, nil
}

func (b *BaseBuilder) buildAlterDB(ctx *sql.Context, n *plan.AlterDB, row sql.Row) (sql.RowIter, error) {
	dbName := n.Database(ctx)

	if !n.Catalog.HasDatabase(ctx, dbName) {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}
	db, err := n.Catalog.Database(ctx, dbName)
	if err != nil {
		return nil, err
	}
	collatedDb, ok := db.(sql.CollatedDatabase)
	if !ok {
		return nil, sql.ErrDatabaseCollationsNotSupported.New(dbName)
	}

	collation := n.Collation
	if collation == sql.Collation_Unspecified {
		collation = sql.Collation_Default
	}
	if err = collatedDb.SetCollation(ctx, collation); err != nil {
		return nil, err
	}

	rows := []sql.Row{{types.OkResult{RowsAffected: 1}}}
	return sql.RowsToRowIter(rows...), nil
}

func (b *BaseBuilder) buildCreateTable(ctx *sql.Context, n *plan.CreateTable, row sql.Row) (sql.RowIter, error) {
	var err error

	// If it's set to Invalid, then no collation has been explicitly defined
	if n.Collation == sql.Collation_Unspecified {
		n.Collation = plan.GetDatabaseCollation(ctx, n.Db)
		// Need to set each type's collation to the correct type as well
		for _, col := range n.CreateSchema.Schema {
			if collatedType, ok := col.Type.(sql.TypeWithCollation); ok && collatedType.Collation() == sql.Collation_Unspecified {
				col.Type, err = collatedType.WithNewCollation(n.Collation)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	err = n.ValidateDefaultPosition()
	if err != nil {
		return sql.RowsToRowIter(), err
	}

	maybePrivDb := n.Db
	if privDb, ok := maybePrivDb.(mysql_db.PrivilegedDatabase); ok {
		maybePrivDb = privDb.Unwrap()
	}

	if n.Temporary() == plan.IsTempTable {
		creatable, ok := maybePrivDb.(sql.TemporaryTableCreator)
		if !ok {
			return sql.RowsToRowIter(), sql.ErrTemporaryTableNotSupported.New()
		}
		err = creatable.CreateTemporaryTable(ctx, n.Name(), n.CreateSchema, n.Collation)
	} else {
		switch creatable := maybePrivDb.(type) {
		case sql.IndexedTableCreator:
			var pkIdxDef sql.IndexDef
			var hasPkIdxDef bool
			for _, idxDef := range n.IdxDefs {
				if idxDef.Constraint == sql.IndexConstraint_Primary {
					hasPkIdxDef = true
					pkIdxDef = sql.IndexDef{
						Name:       idxDef.IndexName,
						Columns:    idxDef.Columns,
						Constraint: idxDef.Constraint,
						Storage:    idxDef.Using,
						Comment:    idxDef.Comment,
					}
				}
			}
			if hasPkIdxDef {
				err = creatable.CreateIndexedTable(ctx, n.Name(), n.CreateSchema, pkIdxDef, n.Collation)
				if sql.ErrUnsupportedIndexPrefix.Is(err) {
					return sql.RowsToRowIter(), err
				}
			} else {
				creatable, ok := maybePrivDb.(sql.TableCreator)
				if !ok {
					return sql.RowsToRowIter(), sql.ErrCreateTableNotSupported.New(n.Db.Name())
				}
				err = creatable.CreateTable(ctx, n.Name(), n.CreateSchema, n.Collation)
			}
		case sql.TableCreator:
			err = creatable.CreateTable(ctx, n.Name(), n.CreateSchema, n.Collation)
		default:
			return sql.RowsToRowIter(), sql.ErrCreateTableNotSupported.New(n.Db.Name())
		}
	}

	if err != nil && !(sql.ErrTableAlreadyExists.Is(err) && (n.IfNotExists() == plan.IfNotExists)) {
		return sql.RowsToRowIter(), err
	}

	if vdb, vok := n.Db.(sql.ViewDatabase); vok {
		_, ok, err := vdb.GetViewDefinition(ctx, n.Name())
		if err != nil {
			return nil, err
		}
		if ok {
			return nil, sql.ErrTableAlreadyExists.New(n.Name())
		}
	}

	//TODO: in the event that foreign keys or indexes aren't supported, you'll be left with a created table and no foreign keys/indexes
	//this also means that if a foreign key or index fails, you'll only have what was declared up to the failure
	tableNode, ok, err := n.Db.GetTableInsensitive(ctx, n.Name())
	if err != nil {
		return sql.RowsToRowIter(), err
	}
	if !ok {
		return sql.RowsToRowIter(), sql.ErrTableCreatedNotFound.New()
	}

	var nonPrimaryIdxes []*plan.IndexDefinition
	for _, def := range n.IdxDefs {
		if def.Constraint != sql.IndexConstraint_Primary {
			nonPrimaryIdxes = append(nonPrimaryIdxes, def)
		}
	}

	if len(nonPrimaryIdxes) > 0 {
		err = createIndexesForCreateTable(ctx, n.Db, tableNode, nonPrimaryIdxes)
		if err != nil {
			return sql.RowsToRowIter(), err
		}
	}

	if len(n.FkDefs) > 0 {
		err = n.CreateForeignKeys(ctx, tableNode)
		if err != nil {
			return sql.RowsToRowIter(), err
		}
	}

	if len(n.Checks()) > 0 {
		err = n.CreateChecks(ctx, tableNode)
		if err != nil {
			return sql.RowsToRowIter(), err
		}
	}

	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), nil
}

func createIndexesForCreateTable(ctx *sql.Context, db sql.Database, tableNode sql.Table, idxes []*plan.IndexDefinition) (err error) {
	idxAlterable, ok := tableNode.(sql.IndexAlterableTable)
	if !ok {
		return plan.ErrNotIndexable.New()
	}

	indexMap := make(map[string]struct{})
	fulltextIndexes := make([]sql.IndexDef, 0, len(idxes))
	for _, idxDef := range idxes {
		indexName := idxDef.IndexName
		// If the name is empty, we create a new name using the columns provided while appending an ascending integer
		// until we get a non-colliding name if the original name (or each preceding name) already exists.
		if indexName == "" {
			indexName = strings.Join(idxDef.ColumnNames(), "")
			if _, ok = indexMap[strings.ToLower(indexName)]; ok {
				for i := 0; true; i++ {
					newIndexName := fmt.Sprintf("%s_%d", indexName, i)
					if _, ok = indexMap[strings.ToLower(newIndexName)]; !ok {
						indexName = newIndexName
						break
					}
				}
			}
		} else if _, ok = indexMap[strings.ToLower(idxDef.IndexName)]; ok {
			return sql.ErrIndexIDAlreadyRegistered.New(idxDef.IndexName)
		}
		// We'll create the Full-Text indexes after all others
		if idxDef.Constraint == sql.IndexConstraint_Fulltext {
			otherDef := idxDef.AsIndexDef()
			otherDef.Name = indexName
			fulltextIndexes = append(fulltextIndexes, otherDef)
			continue
		}
		err := idxAlterable.CreateIndex(ctx, sql.IndexDef{
			Name:       indexName,
			Columns:    idxDef.Columns,
			Constraint: idxDef.Constraint,
			Storage:    idxDef.Using,
			Comment:    idxDef.Comment,
		})
		if err != nil {
			return err
		}
		indexMap[strings.ToLower(indexName)] = struct{}{}
	}

	// Evaluate our Full-Text indexes now
	if len(fulltextIndexes) > 0 {
		database, ok := db.(fulltext.Database)
		if !ok {
			if privDb, ok := db.(mysql_db.PrivilegedDatabase); ok {
				if database, ok = privDb.Unwrap().(fulltext.Database); !ok {
					return sql.ErrCreateTableNotSupported.New(db.Name())
				}
			} else {
				return sql.ErrCreateTableNotSupported.New(db.Name())
			}
		}
		if err = fulltext.CreateFulltextIndexes(ctx, database, idxAlterable, nil, fulltextIndexes...); err != nil {
			return err
		}
	}

	return nil
}

func (b *BaseBuilder) buildCreateProcedure(ctx *sql.Context, n *plan.CreateProcedure, row sql.Row) (sql.RowIter, error) {
	sqlMode := sql.LoadSqlMode(ctx)
	return &createProcedureIter{
		spd: sql.StoredProcedureDetails{
			Name:            n.Name,
			CreateStatement: n.CreateProcedureString,
			CreatedAt:       n.CreatedAt,
			ModifiedAt:      n.ModifiedAt,
			SqlMode:         sqlMode.String(),
		},
		db: n.Database(),
	}, nil
}

func (b *BaseBuilder) buildCreateTrigger(ctx *sql.Context, n *plan.CreateTrigger, row sql.Row) (sql.RowIter, error) {
	sqlMode := sql.LoadSqlMode(ctx)
	return &createTriggerIter{
		definition: sql.TriggerDefinition{
			Name:            n.TriggerName,
			CreateStatement: n.CreateTriggerString,
			CreatedAt:       n.CreatedAt,
			SqlMode:         sqlMode.String(),
		},
		db: n.Database(),
	}, nil
}

func (b *BaseBuilder) buildDropColumn(ctx *sql.Context, n *plan.DropColumn, row sql.Row) (sql.RowIter, error) {
	tbl, err := getTableFromDatabase(ctx, n.Database(), n.Table)
	if err != nil {
		return nil, err
	}

	err = n.Validate(ctx, tbl)
	if err != nil {
		return nil, err
	}

	alterable, ok := tbl.(sql.AlterableTable)
	if !ok {
		return nil, sql.ErrAlterTableNotSupported.New(tbl.Name())
	}

	return &dropColumnIter{
		d:         n,
		alterable: alterable,
	}, nil
}

func (b *BaseBuilder) buildAlterTableCollation(ctx *sql.Context, n *plan.AlterTableCollation, row sql.Row) (sql.RowIter, error) {
	tbl, err := getTableFromDatabase(ctx, n.Database(), n.Table)
	if err != nil {
		return nil, err
	}

	alterable, ok := tbl.(sql.CollationAlterableTable)
	if !ok {
		return nil, sql.ErrAlterTableCollationNotSupported.New(tbl.Name())
	}

	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), alterable.ModifyDefaultCollation(ctx, n.Collation)
}

func (b *BaseBuilder) buildCreateForeignKey(ctx *sql.Context, n *plan.CreateForeignKey, row sql.Row) (sql.RowIter, error) {
	if n.FkDef.OnUpdate == sql.ForeignKeyReferentialAction_SetDefault || n.FkDef.OnDelete == sql.ForeignKeyReferentialAction_SetDefault {
		return nil, sql.ErrForeignKeySetDefault.New()
	}
	db, err := n.DbProvider.Database(ctx, n.FkDef.Database)
	if err != nil {
		return nil, err
	}
	tbl, ok, err := db.GetTableInsensitive(ctx, n.FkDef.Table)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(n.FkDef.Table)
	}

	refDb, err := n.DbProvider.Database(ctx, n.FkDef.ParentDatabase)
	if err != nil {
		return nil, err
	}
	refTbl, ok, err := refDb.GetTableInsensitive(ctx, n.FkDef.ParentTable)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(n.FkDef.ParentTable)
	}

	fkTbl, ok := tbl.(sql.ForeignKeyTable)
	if !ok {
		return nil, sql.ErrNoForeignKeySupport.New(n.FkDef.Table)
	}
	refFkTbl, ok := refTbl.(sql.ForeignKeyTable)
	if !ok {
		return nil, sql.ErrNoForeignKeySupport.New(n.FkDef.ParentTable)
	}

	fkChecks, err := ctx.GetSessionVariable(ctx, "foreign_key_checks")
	if err != nil {
		return nil, err
	}

	err = plan.ResolveForeignKey(ctx, fkTbl, refFkTbl, *n.FkDef, true, fkChecks.(int8) == 1)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), nil
}
