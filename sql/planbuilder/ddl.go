package planbuilder

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/mysql"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func (b *Builder) resolveDb(name string) sql.Database {
	if name == "" {
		err := sql.ErrNoDatabaseSelected.New()
		b.handleErr(err)
	}
	database, err := b.cat.Database(b.ctx, name)
	if err != nil {
		b.handleErr(err)
	}

	// todo show tables as of expects privileged
	//if privilegedDatabase, ok := database.(mysql_db.PrivilegedDatabase); ok {
	//	database = privilegedDatabase.Unwrap()
	//}
	return database
}

// buildAlterTable converts AlterTable AST nodes. If there is a single clause in the statement, it is returned as
// the appropriate node type. Otherwise, a plan.Block is returned with children representing all the various clauses.
// Our validation rules for what counts as a legal set of alter clauses differs from mysql's here. MySQL seems to apply
// some form of precedence rules to the clauses in an ALTER TABLE so that e.g. DROP COLUMN always happens before other
// kinds of statements. So in MySQL, statements like `ALTER TABLE t ADD KEY (a), DROP COLUMN a` fails, whereas our
// analyzer happily produces a plan that adds an index and then drops that column. We do this in part for simplicity,
// and also because we construct more than one node per clause in some cases and really want them executed in a
// particular order in that case.
func (b *Builder) buildAlterTable(inScope *scope, query string, c *ast.AlterTable) (outScope *scope) {
	b.multiDDL = true
	defer func() {
		b.multiDDL = false
	}()

	statements := make([]sql.Node, 0, len(c.Statements))
	for i := 0; i < len(c.Statements); i++ {
		scopes := b.buildAlterTableClause(inScope, c.Statements[i])
		for _, scope := range scopes {
			statements = append(statements, scope.node)
		}
	}

	if len(statements) == 1 {
		outScope = inScope.push()
		outScope.node = statements[0]
		return outScope
	}

	outScope = inScope.push()
	outScope.node = plan.NewBlock(statements)
	return
}

func (b *Builder) buildDDL(inScope *scope, query string, c *ast.DDL) (outScope *scope) {
	outScope = inScope.push()
	switch strings.ToLower(c.Action) {
	case ast.CreateStr:
		if c.TriggerSpec != nil {
			return b.buildCreateTrigger(inScope, query, c)
		}
		if c.ProcedureSpec != nil {
			return b.buildCreateProcedure(inScope, query, c)
		}
		if c.EventSpec != nil {
			return b.buildCreateEvent(inScope, query, c)
		}
		if c.ViewSpec != nil {
			return b.buildCreateView(inScope, query, c)
		}
		return b.buildCreateTable(inScope, c)
	case ast.DropStr:
		// get database
		if c.TriggerSpec != nil {
			dbName := c.TriggerSpec.TrigName.Qualifier.String()
			if dbName == "" {
				dbName = b.ctx.GetCurrentDatabase()
			}
			trigName := c.TriggerSpec.TrigName.Name.String()
			outScope.node = plan.NewDropTrigger(b.resolveDb(dbName), trigName, c.IfExists)
			return
		}
		if c.ProcedureSpec != nil {
			dbName := c.ProcedureSpec.ProcName.Qualifier.String()
			if dbName == "" {
				dbName = b.ctx.GetCurrentDatabase()
			}
			procName := c.ProcedureSpec.ProcName.Name.String()
			outScope.node = plan.NewDropProcedure(b.resolveDb(dbName), procName, c.IfExists)
			return
		}
		if c.EventSpec != nil {
			dbName := c.EventSpec.EventName.Qualifier.String()
			if dbName == "" {
				dbName = b.ctx.GetCurrentDatabase()
			}
			eventName := c.EventSpec.EventName.Name.String()
			outScope.node = plan.NewDropEvent(b.resolveDb(dbName), eventName, c.IfExists)
			return
		}
		if len(c.FromViews) != 0 {
			plans := make([]sql.Node, len(c.FromViews))
			for i, v := range c.FromViews {
				plans[i] = plan.NewSingleDropView(b.currentDb(), v.Name.String())
			}
			outScope.node = plan.NewDropView(plans, c.IfExists)
			return
		}
		return b.buildDropTable(inScope, c)
	case ast.AlterStr:
		if c.EventSpec != nil {
			return b.buildAlterEvent(inScope, query, c)
		}
		b.handleErr(sql.ErrUnsupportedFeature.New(ast.String(c)))
	case ast.RenameStr:
		return b.buildRenameTable(inScope, c)
	case ast.TruncateStr:
		return b.buildTruncateTable(inScope, c)
	default:
		b.handleErr(sql.ErrUnsupportedSyntax.New(ast.String(c)))
	}
	return
}

func (b *Builder) buildDropTable(inScope *scope, c *ast.DDL) (outScope *scope) {
	outScope = inScope.push()
	var dropTables []sql.Node
	dbName := c.FromTables[0].Qualifier.String()
	if dbName == "" {
		dbName = b.currentDb().Name()
	}
	for _, t := range c.FromTables {
		if t.Qualifier.String() != "" && t.Qualifier.String() != dbName {
			err := sql.ErrUnsupportedFeature.New("dropping tables on multiple databases in the same statement")
			b.handleErr(err)
		}
		tableName := strings.ToLower(t.Name.String())
		if c.IfExists {
			_, _, err := b.cat.Table(b.ctx, dbName, tableName)
			if sql.ErrTableNotFound.Is(err) {
				b.ctx.Session.Warn(&sql.Warning{
					Level:   "Note",
					Code:    mysql.ERBadTable,
					Message: fmt.Sprintf("Can't drop table %s; table doesn't exist ", tableName),
				})
				continue
			} else if err != nil {
				b.handleErr(err)
			}
		}

		tableScope, ok := b.buildTablescan(inScope, dbName, tableName, nil)
		if ok {
			dropTables = append(dropTables, tableScope.node)
		} else if !c.IfExists {
			err := sql.ErrTableNotFound.New(tableName)
			b.handleErr(err)
		}
	}

	outScope.node = plan.NewDropTable(dropTables, c.IfExists)
	return
}

func (b *Builder) buildTruncateTable(inScope *scope, c *ast.DDL) (outScope *scope) {
	outScope = inScope.push()
	dbName := c.Table.Qualifier.String()
	tabName := c.Table.Name.String()
	tableScope, ok := b.buildTablescan(inScope, dbName, tabName, nil)
	if !ok {
		b.handleErr(sql.ErrTableNotFound.New(tabName))
	}
	outScope.node = plan.NewTruncate(
		c.Table.Qualifier.String(),
		tableScope.node,
	)
	return
}

func (b *Builder) buildCreateTable(inScope *scope, c *ast.DDL) (outScope *scope) {
	outScope = inScope.push()
	if c.OptLike != nil {
		return b.buildCreateTableLike(inScope, c)
	}

	// In the case that no table spec is given but a SELECT Statement return the CREATE TABLE node.
	// if the table spec != nil it will get parsed below.
	if c.TableSpec == nil && c.OptSelect != nil {
		tableSpec := &plan.TableSpec{}

		selectScope := b.buildSelectStmt(inScope, c.OptSelect.Select)

		outScope.node = plan.NewCreateTableSelect(b.currentDb(), c.Table.Name.String(), selectScope.node, tableSpec, plan.IfNotExistsOption(c.IfNotExists), plan.TempTableOption(c.Temporary))
		return outScope
	}

	idxDefs := b.buildIndexDefs(inScope, c.TableSpec)

	qualifier := c.Table.Qualifier.String()
	if qualifier == "" {
		qualifier = b.ctx.GetCurrentDatabase()
	}
	database := b.resolveDb(qualifier)

	schema, collation := b.tableSpecToSchema(inScope, outScope, strings.ToLower(database.Name()), strings.ToLower(c.Table.Name.String()), c.TableSpec, false)
	fkDefs, chDefs := b.buildConstraintsDefs(outScope, c.Table, c.TableSpec)

	tableSpec := &plan.TableSpec{
		Schema:    schema,
		IdxDefs:   idxDefs,
		FkDefs:    fkDefs,
		ChDefs:    chDefs,
		Collation: collation,
	}

	if c.OptSelect != nil {
		selectScope := b.buildSelectStmt(inScope, c.OptSelect.Select)
		outScope.node = plan.NewCreateTableSelect(database, c.Table.Name.String(), selectScope.node, tableSpec, plan.IfNotExistsOption(c.IfNotExists), plan.TempTableOption(c.Temporary))
	}

	outScope.node = plan.NewCreateTable(
		database, c.Table.Name.String(), plan.IfNotExistsOption(c.IfNotExists), plan.TempTableOption(c.Temporary), tableSpec)
	return
}

func (b *Builder) buildCreateTableLike(inScope *scope, ct *ast.DDL) *scope {
	tableName := ct.OptLike.LikeTable.Name.String()
	likeDbName := ct.OptLike.LikeTable.Qualifier.String()
	if likeDbName == "" {
		likeDbName = b.ctx.GetCurrentDatabase()
	}
	outScope, ok := b.buildTablescan(inScope, likeDbName, tableName, nil)
	if !ok {
		b.handleErr(sql.ErrTableNotFound.New(tableName))
	}
	likeTable, ok := outScope.node.(*plan.ResolvedTable)
	if !ok {
		err := fmt.Errorf("expected resolved table: %s", tableName)
		b.handleErr(err)
	}

	newTableName := strings.ToLower(ct.Table.Name.String())
	outScope.setTableAlias(newTableName)

	var idxDefs []*plan.IndexDefinition
	if indexableTable, ok := likeTable.Table.(sql.IndexAddressableTable); ok {
		indexes, err := indexableTable.GetIndexes(b.ctx)
		if err != nil {
			b.handleErr(err)
		}
		for _, index := range indexes {
			if index.IsGenerated() {
				continue
			}
			constraint := sql.IndexConstraint_None
			if index.IsUnique() {
				if index.ID() == "PRIMARY" {
					constraint = sql.IndexConstraint_Primary
				} else {
					constraint = sql.IndexConstraint_Unique
				}
			}

			columns := make([]sql.IndexColumn, len(index.Expressions()))
			for i, col := range index.Expressions() {
				//TODO: find a better way to get only the column name if the table is present
				col = strings.TrimPrefix(col, indexableTable.Name()+".")
				columns[i] = sql.IndexColumn{
					Name:   col,
					Length: 0,
				}
			}
			idxDefs = append(idxDefs, &plan.IndexDefinition{
				IndexName:  index.ID(),
				Using:      sql.IndexUsing_Default,
				Constraint: constraint,
				Columns:    columns,
				Comment:    index.Comment(),
			})
		}
	}
	origSch := likeTable.Schema()
	newSch := make(sql.Schema, len(origSch))
	for i, col := range origSch {
		tempCol := *col
		tempCol.Source = newTableName
		newSch[i] = &tempCol
	}

	var pkOrdinals []int
	if pkTable, ok := likeTable.Table.(sql.PrimaryKeyTable); ok {
		pkOrdinals = pkTable.PrimaryKeySchema().PkOrdinals
	}

	var checkDefs []*sql.CheckConstraint
	if checksTable, ok := likeTable.Table.(sql.CheckTable); ok {
		checks, err := checksTable.GetChecks(b.ctx)
		if err != nil {
			b.handleErr(err)
		}

		for _, check := range checks {
			checkConstraint := b.buildCheckConstraint(outScope, &check)
			if err != nil {
				b.handleErr(err)
			}

			// Prevent a name collision between old and new checks.
			// New check will be assigned a name during building.
			checkConstraint.Name = ""
			checkDefs = append(checkDefs, checkConstraint)
		}
	}

	tableSpec := &plan.TableSpec{
		Schema:    sql.NewPrimaryKeySchema(newSch, pkOrdinals...),
		IdxDefs:   idxDefs,
		ChDefs:    checkDefs,
		Collation: likeTable.Collation(),
	}

	qualifier := ct.Table.Qualifier.String()
	if qualifier == "" {
		qualifier = b.ctx.GetCurrentDatabase()
	}
	database := b.resolveDb(qualifier)

	outScope.node = plan.NewCreateTable(database, newTableName, plan.IfNotExistsOption(ct.IfNotExists), plan.TempTableOption(ct.Temporary), tableSpec)
	return outScope
}

func (b *Builder) buildRenameTable(inScope *scope, ddl *ast.DDL) (outScope *scope) {
	outScope = inScope.push()
	if len(ddl.FromTables) != len(ddl.ToTables) {
		panic("Expected from tables and to tables of equal length")
	}

	var fromTables, toTables []string
	for _, table := range ddl.FromTables {
		fromTables = append(fromTables, table.Name.String())
	}
	for _, table := range ddl.ToTables {
		toTables = append(toTables, table.Name.String())
	}

	outScope.node = plan.NewRenameTable(b.currentDb(), fromTables, toTables, b.multiDDL)
	return
}

func (b *Builder) isUniqueColumn(tableSpec *ast.TableSpec, columnName string) bool {
	for _, column := range tableSpec.Columns {
		if column.Name.String() == columnName {
			return column.Type.KeyOpt == colKeyUnique ||
				column.Type.KeyOpt == colKeyUniqueKey
		}
	}
	err := fmt.Errorf("unknown column name %s", columnName)
	b.handleErr(err)
	return false

}

func (b *Builder) buildAlterTableClause(inScope *scope, ddl *ast.DDL) []*scope {
	outScopes := make([]*scope, 0, 1)

	if ddl.ColumnAction != "" {
		columnActionOutscope := b.buildAlterTableColumnAction(inScope, ddl)
		outScopes = append(outScopes, columnActionOutscope)

		if ddl.TableSpec != nil {
			if len(ddl.TableSpec.Columns) != 1 {
				err := sql.ErrUnsupportedFeature.New("unexpected number of columns in a single alter column clause")
				b.handleErr(err)
			}

			column := ddl.TableSpec.Columns[0]
			isUnique := b.isUniqueColumn(ddl.TableSpec, column.Name.String())
			if isUnique {
				dbName := ddl.Table.Qualifier.String()
				tableName := ddl.Table.Name.String()
				var ok bool
				columnActionOutscope, ok = b.buildTablescan(inScope, dbName, tableName, nil)
				if !ok {
					b.handleErr(sql.ErrTableNotFound.New(tableName))
				}

				table, ok := columnActionOutscope.node.(*plan.ResolvedTable)
				if !ok {
					err := fmt.Errorf("expected resolved table: %s", tableName)
					b.handleErr(err)
				}

				createIndex := plan.NewAlterCreateIndex(
					table.SqlDatabase,
					table,
					column.Name.String(),
					sql.IndexUsing_BTree,
					sql.IndexConstraint_Unique,
					[]sql.IndexColumn{{Name: column.Name.String()}},
					"",
				)

				createIndexScope := inScope.push()
				createIndexScope.node = createIndex
				outScopes = append(outScopes, createIndexScope)
			}
		}
	}

	if ddl.ConstraintAction != "" {
		if len(ddl.TableSpec.Constraints) != 1 {
			b.handleErr(sql.ErrUnsupportedFeature.New("unexpected number of constraints in a single alter constraint clause"))
		}

		outScopes = append(outScopes, b.buildAlterConstraint(inScope, ddl))
	}

	if ddl.IndexSpec != nil {
		outScopes = append(outScopes, b.buildAlterIndex(inScope, ddl))
	}

	if ddl.AutoIncSpec != nil {
		outScopes = append(outScopes, b.buildAlterAutoIncrement(inScope, ddl))
	}

	if ddl.DefaultSpec != nil {
		outScopes = append(outScopes, b.buildAlterDefault(inScope, ddl))
	}

	if ddl.AlterCollationSpec != nil {
		outScopes = append(outScopes, b.buildAlterCollationSpec(inScope, ddl))
	}

	// RENAME a to b, c to d ..
	if ddl.Action == ast.RenameStr {
		outScopes = append(outScopes, b.buildRenameTable(inScope, ddl))
	}

	return outScopes
}

func (b *Builder) buildAlterTableColumnAction(inScope *scope, ddl *ast.DDL) (outScope *scope) {
	dbName := ddl.Table.Qualifier.String()
	tableName := ddl.Table.Name.String()
	var ok bool
	outScope, ok = b.buildTablescan(inScope, dbName, tableName, nil)
	if !ok {
		b.handleErr(sql.ErrTableNotFound.New(tableName))
	}
	table, ok := outScope.node.(*plan.ResolvedTable)
	if !ok {
		err := fmt.Errorf("expected resolved table: %s", tableName)
		b.handleErr(err)
	}

	switch strings.ToLower(ddl.ColumnAction) {
	case ast.AddStr:
		sch, _ := b.tableSpecToSchema(inScope, outScope, dbName, ddl.Table.Name.String(), ddl.TableSpec, true)
		outScope.node = plan.NewAddColumnResolved(table, *sch.Schema[0], columnOrderToColumnOrder(ddl.ColumnOrder))
	case ast.DropStr:
		drop := plan.NewDropColumnResolved(table, ddl.Column.String())
		drop.Checks = b.loadChecksFromTable(outScope, table.Table)
		outScope.node = drop
	case ast.RenameStr:
		for _, c := range table.Schema() {
			outScope.newColumn(scopeColumn{
				db:       strings.ToLower(table.SqlDatabase.Name()),
				table:    strings.ToLower(c.Source),
				col:      strings.ToLower(c.Name),
				typ:      c.Type,
				nullable: c.Nullable,
			})
		}
		rename := plan.NewRenameColumnResolved(table, ddl.Column.String(), ddl.ToColumn.String())
		rename.Checks = b.loadChecksFromTable(outScope, table.Table)
		outScope.node = rename
	case ast.ModifyStr, ast.ChangeStr:
		sch, _ := b.tableSpecToSchema(inScope, outScope, dbName, ddl.Table.Name.String(), ddl.TableSpec, true)
		outScope.node = plan.NewModifyColumnResolved(table, ddl.Column.String(), *sch.Schema[0], columnOrderToColumnOrder(ddl.ColumnOrder))
	default:
		err := sql.ErrUnsupportedFeature.New(ast.String(ddl))
		b.handleErr(err)
	}

	return outScope
}

func (b *Builder) buildAlterConstraint(inScope *scope, ddl *ast.DDL) (outScope *scope) {
	dbName := ddl.Table.Qualifier.String()
	tableName := ddl.Table.Name.String()
	var ok bool
	outScope, ok = b.buildTablescan(inScope, dbName, tableName, nil)
	if !ok {
		b.handleErr(sql.ErrTableNotFound.New(tableName))
	}
	table, ok := outScope.node.(*plan.ResolvedTable)
	if !ok {
		err := fmt.Errorf("expected resolved table: %s", tableName)
		b.handleErr(err)
	}
	parsedConstraint := b.convertConstraintDefinition(outScope, ddl.TableSpec.Constraints[0])
	switch strings.ToLower(ddl.ConstraintAction) {
	case ast.AddStr:
		switch c := parsedConstraint.(type) {
		case *sql.ForeignKeyConstraint:
			c.Database = table.SqlDatabase.Name()
			c.Table = table.Name()
			alterFk := plan.NewAlterAddForeignKey(c)
			alterFk.DbProvider = b.cat
			outScope.node = alterFk
		case *sql.CheckConstraint:
			outScope.node = plan.NewAlterAddCheck(table, c)
		default:
			err := sql.ErrUnsupportedFeature.New(ast.String(ddl))
			b.handleErr(err)
		}
	case ast.DropStr:
		switch c := parsedConstraint.(type) {
		case *sql.ForeignKeyConstraint:
			database := table.SqlDatabase.Name()
			dropFk := plan.NewAlterDropForeignKey(database, table.Name(), c.Name)
			dropFk.DbProvider = b.cat
			outScope.node = dropFk
		case *sql.CheckConstraint:
			outScope.node = plan.NewAlterDropCheck(table, c.Name)
		case namedConstraint:
			outScope.node = &plan.DropConstraint{
				UnaryNode: plan.UnaryNode{Child: table},
				Name:      c.name,
			}
		default:
			err := sql.ErrUnsupportedFeature.New(ast.String(ddl))
			b.handleErr(err)
		}
	}
	return
}

func (b *Builder) buildConstraintsDefs(inScope *scope, tname ast.TableName, spec *ast.TableSpec) (fks []*sql.ForeignKeyConstraint, checks []*sql.CheckConstraint) {
	for _, unknownConstraint := range spec.Constraints {
		parsedConstraint := b.convertConstraintDefinition(inScope, unknownConstraint)
		switch constraint := parsedConstraint.(type) {
		case *sql.ForeignKeyConstraint:
			constraint.Database = tname.Qualifier.String()
			constraint.Table = tname.Name.String()
			if constraint.Database == "" {
				constraint.Database = b.ctx.GetCurrentDatabase()
			}
			fks = append(fks, constraint)
		case *sql.CheckConstraint:
			checks = append(checks, constraint)
		default:
			err := sql.ErrUnknownConstraintDefinition.New(unknownConstraint.Name, unknownConstraint)
			b.handleErr(err)
		}
	}
	return
}

func columnOrderToColumnOrder(order *ast.ColumnOrder) *sql.ColumnOrder {
	if order == nil {
		return nil
	}
	if order.First {
		return &sql.ColumnOrder{First: true}
	} else {
		return &sql.ColumnOrder{AfterColumn: order.AfterColumn.String()}
	}
}

func (b *Builder) buildIndexDefs(inScope *scope, spec *ast.TableSpec) (idxDefs []*plan.IndexDefinition) {
	for _, idxDef := range spec.Indexes {
		constraint := sql.IndexConstraint_None
		if idxDef.Info.Primary {
			constraint = sql.IndexConstraint_Primary
		} else if idxDef.Info.Unique {
			constraint = sql.IndexConstraint_Unique
		} else if idxDef.Info.Spatial {
			constraint = sql.IndexConstraint_Spatial
		} else if idxDef.Info.Fulltext {
			constraint = sql.IndexConstraint_Fulltext
		}

		columns := b.gatherIndexColumns(idxDef.Columns)

		var comment string
		for _, option := range idxDef.Options {
			if strings.ToLower(option.Name) == strings.ToLower(ast.KeywordString(ast.COMMENT_KEYWORD)) {
				comment = string(option.Value.Val)
			}
		}
		idxDefs = append(idxDefs, &plan.IndexDefinition{
			IndexName:  idxDef.Info.Name.String(),
			Using:      sql.IndexUsing_Default, //TODO: add vitess support for USING
			Constraint: constraint,
			Columns:    columns,
			Comment:    comment,
		})
	}

	for _, colDef := range spec.Columns {
		if colDef.Type.KeyOpt == colKeyFulltextKey {
			idxDefs = append(idxDefs, &plan.IndexDefinition{
				IndexName:  "",
				Using:      sql.IndexUsing_Default,
				Constraint: sql.IndexConstraint_Fulltext,
				Comment:    "",
				Columns: []sql.IndexColumn{{
					Name:   colDef.Name.String(),
					Length: 0,
				}},
			})
		} else if colDef.Type.KeyOpt == colKeyUnique || colDef.Type.KeyOpt == colKeyUniqueKey {
			idxDefs = append(idxDefs, &plan.IndexDefinition{
				IndexName:  "",
				Using:      sql.IndexUsing_Default,
				Constraint: sql.IndexConstraint_Unique,
				Comment:    "",
				Columns: []sql.IndexColumn{{
					Name:   colDef.Name.String(),
					Length: 0,
				}},
			})
		}
	}
	return
}

type namedConstraint struct {
	name string
}

func (b *Builder) convertConstraintDefinition(inScope *scope, cd *ast.ConstraintDefinition) interface{} {
	if fkConstraint, ok := cd.Details.(*ast.ForeignKeyDefinition); ok {
		columns := make([]string, len(fkConstraint.Source))
		for i, col := range fkConstraint.Source {
			columns[i] = col.String()
		}
		refColumns := make([]string, len(fkConstraint.ReferencedColumns))
		for i, col := range fkConstraint.ReferencedColumns {
			refColumns[i] = col.String()
		}
		refDatabase := fkConstraint.ReferencedTable.Qualifier.String()
		if refDatabase == "" {
			refDatabase = b.ctx.GetCurrentDatabase()
		}
		// The database and table are set in the calling function
		return &sql.ForeignKeyConstraint{
			Name:           cd.Name,
			Columns:        columns,
			ParentDatabase: refDatabase,
			ParentTable:    fkConstraint.ReferencedTable.Name.String(),
			ParentColumns:  refColumns,
			OnUpdate:       b.buildReferentialAction(fkConstraint.OnUpdate),
			OnDelete:       b.buildReferentialAction(fkConstraint.OnDelete),
			IsResolved:     false,
		}
	} else if chConstraint, ok := cd.Details.(*ast.CheckConstraintDefinition); ok {
		var c sql.Expression
		if chConstraint.Expr != nil {
			c = b.buildScalar(inScope, chConstraint.Expr)
		}

		return &sql.CheckConstraint{
			Name:     cd.Name,
			Expr:     c,
			Enforced: chConstraint.Enforced,
		}
	} else if len(cd.Name) > 0 && cd.Details == nil {
		return namedConstraint{cd.Name}
	}
	err := sql.ErrUnknownConstraintDefinition.New(cd.Name, cd)
	b.handleErr(err)
	return nil
}

func (b *Builder) buildReferentialAction(action ast.ReferenceAction) sql.ForeignKeyReferentialAction {
	switch action {
	case ast.Restrict:
		return sql.ForeignKeyReferentialAction_Restrict
	case ast.Cascade:
		return sql.ForeignKeyReferentialAction_Cascade
	case ast.NoAction:
		return sql.ForeignKeyReferentialAction_NoAction
	case ast.SetNull:
		return sql.ForeignKeyReferentialAction_SetNull
	case ast.SetDefault:
		return sql.ForeignKeyReferentialAction_SetDefault
	default:
		return sql.ForeignKeyReferentialAction_DefaultAction
	}
}

// todo drop column, rename column
func (b *Builder) buildAlterIndex(inScope *scope, ddl *ast.DDL) (outScope *scope) {
	dbName := ddl.Table.Qualifier.String()
	tableName := ddl.Table.Name.String()
	var ok bool
	outScope, ok = b.buildTablescan(inScope, dbName, tableName, nil)
	if !ok {
		b.handleErr(sql.ErrTableNotFound.New(tableName))
	}
	table, ok := outScope.node.(*plan.ResolvedTable)
	if !ok {
		err := fmt.Errorf("expected resolved table: %s", tableName)
		b.handleErr(err)
	}
	switch strings.ToLower(ddl.IndexSpec.Action) {
	case ast.CreateStr:
		var using sql.IndexUsing
		switch ddl.IndexSpec.Using.Lowered() {
		case "", "btree":
			using = sql.IndexUsing_BTree
		case "hash":
			using = sql.IndexUsing_Hash
		default:
			return b.buildExternalCreateIndex(inScope, ddl)
		}

		var constraint sql.IndexConstraint
		switch ddl.IndexSpec.Type {
		case ast.UniqueStr:
			constraint = sql.IndexConstraint_Unique
		case ast.FulltextStr:
			constraint = sql.IndexConstraint_Fulltext
		case ast.SpatialStr:
			constraint = sql.IndexConstraint_Spatial
		case ast.PrimaryStr:
			constraint = sql.IndexConstraint_Primary
		default:
			constraint = sql.IndexConstraint_None
		}

		columns := b.gatherIndexColumns(ddl.IndexSpec.Columns)

		var comment string
		for _, option := range ddl.IndexSpec.Options {
			if strings.ToLower(option.Name) == strings.ToLower(ast.KeywordString(ast.COMMENT_KEYWORD)) {
				comment = string(option.Value.Val)
			}
		}

		if constraint == sql.IndexConstraint_Primary {
			outScope.node = plan.NewAlterCreatePk(table.SqlDatabase, table, columns)
			return
		}

		indexName := ddl.IndexSpec.ToName.String()
		if strings.ToLower(indexName) == ast.PrimaryStr {
			err := sql.ErrInvalidIndexName.New(indexName)
			b.handleErr(err)
		}

		outScope.node = plan.NewAlterCreateIndex(table.SqlDatabase, table, ddl.IndexSpec.ToName.String(), using, constraint, columns, comment)
		return
	case ast.DropStr:
		if ddl.IndexSpec.Type == ast.PrimaryStr {
			outScope.node = plan.NewAlterDropPk(table.SqlDatabase, table)
			return
		}
		outScope.node = plan.NewAlterDropIndex(table.SqlDatabase, table, ddl.IndexSpec.ToName.String())

		//todo checks

		return
	case ast.RenameStr:
		outScope.node = plan.NewAlterRenameIndex(table.SqlDatabase, table, ddl.IndexSpec.FromName.String(), ddl.IndexSpec.ToName.String())
		//todo checks
		return
	case "disable":
		outScope.node = plan.NewAlterDisableEnableKeys(table.SqlDatabase, table, true)
		return
	case "enable":
		outScope.node = plan.NewAlterDisableEnableKeys(table.SqlDatabase, table, false)
		return
	default:
		err := sql.ErrUnsupportedFeature.New(ast.String(ddl))
		b.handleErr(err)
	}
	return
}

func (b *Builder) gatherIndexColumns(cols []*ast.IndexColumn) []sql.IndexColumn {
	out := make([]sql.IndexColumn, len(cols))
	for i, col := range cols {
		var length int64
		var err error
		if col.Length != nil && col.Length.Type == ast.IntVal {
			length, err = strconv.ParseInt(string(col.Length.Val), 10, 64)
			if err != nil {
				b.handleErr(err)
			}
			if length < 1 {
				err := sql.ErrKeyZero.New(col.Column)
				b.handleErr(err)
			}
		}
		out[i] = sql.IndexColumn{
			Name:   col.Column.String(),
			Length: length,
		}
	}
	return out
}

func (b *Builder) buildAlterAutoIncrement(inScope *scope, ddl *ast.DDL) (outScope *scope) {
	val, ok := ddl.AutoIncSpec.Value.(*ast.SQLVal)
	if !ok {
		err := sql.ErrInvalidSQLValType.New(ddl.AutoIncSpec.Value)
		b.handleErr(err)
	}

	var autoVal uint64
	if val.Type == ast.IntVal {
		i, err := strconv.ParseUint(string(val.Val), 10, 64)
		if err != nil {
			b.handleErr(err)
		}
		autoVal = i
	} else if val.Type == ast.FloatVal {
		f, err := strconv.ParseFloat(string(val.Val), 10)
		if err != nil {
			b.handleErr(err)
		}
		autoVal = uint64(f)
	} else {
		err := sql.ErrInvalidSQLValType.New(ddl.AutoIncSpec.Value)
		b.handleErr(err)
	}

	dbName := ddl.Table.Qualifier.String()
	tableName := ddl.Table.Name.String()
	outScope, ok = b.buildTablescan(inScope, dbName, tableName, nil)
	if !ok {
		b.handleErr(sql.ErrTableNotFound.New(tableName))
	}
	table, ok := outScope.node.(*plan.ResolvedTable)
	if !ok {
		err := fmt.Errorf("expected resolved table: %s", tableName)
		b.handleErr(err)
	}
	outScope.node = plan.NewAlterAutoIncrement(table.SqlDatabase, table, autoVal)
	return
}

func (b *Builder) buildAlterDefault(inScope *scope, ddl *ast.DDL) (outScope *scope) {
	dbName := ddl.Table.Qualifier.String()
	tableName := ddl.Table.Name.String()
	var ok bool
	outScope, ok = b.buildTablescan(inScope, dbName, tableName, nil)
	if !ok {
		b.handleErr(sql.ErrTableNotFound.New(tableName))
	}
	table, ok := outScope.node.(*plan.ResolvedTable)
	if !ok {
		err := fmt.Errorf("expected resolved table: %s", tableName)
		b.handleErr(err)
	}
	switch strings.ToLower(ddl.DefaultSpec.Action) {
	case ast.SetStr:
		defaultVal := b.buildDefaultExpression(inScope, ddl.DefaultSpec.Value)
		outScope.node = plan.NewAlterDefaultSet(table.SqlDatabase, table, ddl.DefaultSpec.Column.String(), defaultVal)
		return
	case ast.DropStr:
		outScope.node = plan.NewAlterDefaultDrop(table.SqlDatabase, table, ddl.DefaultSpec.Column.String())

		return
	default:
		err := sql.ErrUnsupportedFeature.New(ast.String(ddl))
		b.handleErr(err)
	}
	return
}

func (b *Builder) buildAlterCollationSpec(inScope *scope, ddl *ast.DDL) (outScope *scope) {
	dbName := ddl.Table.Qualifier.String()
	tableName := ddl.Table.Name.String()
	var ok bool
	outScope, ok = b.buildTablescan(inScope, dbName, tableName, nil)
	if !ok {
		b.handleErr(sql.ErrTableNotFound.New(tableName))
	}
	table, ok := outScope.node.(*plan.ResolvedTable)
	if !ok {
		err := fmt.Errorf("expected resolved table: %s", tableName)
		b.handleErr(err)
	}
	var charSetStr *string
	var collationStr *string
	if len(ddl.AlterCollationSpec.CharacterSet) > 0 {
		charSetStr = &ddl.AlterCollationSpec.CharacterSet
	}
	if len(ddl.AlterCollationSpec.Collation) > 0 {
		collationStr = &ddl.AlterCollationSpec.Collation
	}
	collation, err := sql.ParseCollation(charSetStr, collationStr, false)
	if err != nil {
		b.handleErr(err)
	}
	outScope.node = plan.NewAlterTableCollationResolved(table, collation)
	return
}

func (b *Builder) buildDefaultExpression(inScope *scope, defaultExpr ast.Expr) *sql.ColumnDefaultValue {
	if defaultExpr == nil {
		return nil
	}
	parsedExpr := b.buildScalar(inScope, defaultExpr)

	// Function expressions must be enclosed in parentheses (except for current_timestamp() and now())
	_, isParenthesized := defaultExpr.(*ast.ParenExpr)
	isLiteral := !isParenthesized

	// A literal will never have children, thus we can also check for that.
	if unaryExpr, is := defaultExpr.(*ast.UnaryExpr); is {
		if _, lit := unaryExpr.Expr.(*ast.SQLVal); lit {
			isLiteral = true
		}
	} else if !isParenthesized {
		if f, ok := parsedExpr.(*expression.UnresolvedFunction); ok {
			// Datetime and Timestamp columns allow now and current_timestamp to not be enclosed in parens,
			// but they still need to be treated as function expressions
			if f.Name() == "now" || f.Name() == "current_timestamp" {
				isLiteral = false
			} else {
				// All other functions must *always* be enclosed in parens
				err := sql.ErrSyntaxError.New("column default function expressions must be enclosed in parentheses")
				b.handleErr(err)
			}
		}
	}

	return ExpressionToColumnDefaultValue(parsedExpr, isLiteral, isParenthesized)
}

// ExpressionToColumnDefaultValue takes in an Expression and returns the equivalent ColumnDefaultValue if the expression
// is valid for a default value. If the expression represents a literal (and not an expression that returns a literal, so "5"
// rather than "(5)"), then the parameter "isLiteral" should be true.
func ExpressionToColumnDefaultValue(inputExpr sql.Expression, isLiteral, isParenthesized bool) *sql.ColumnDefaultValue {
	return &sql.ColumnDefaultValue{
		Expression:    inputExpr,
		OutType:       nil,
		Literal:       isLiteral,
		ReturnNil:     true,
		Parenthesized: isParenthesized,
	}
}

func (b *Builder) buildExternalCreateIndex(inScope *scope, ddl *ast.DDL) (outScope *scope) {
	config := make(map[string]string)
	for _, option := range ddl.IndexSpec.Options {
		if option.Using != "" {
			config[option.Name] = option.Using
		} else {
			config[option.Name] = string(option.Value.Val)
		}
	}

	dbName := ddl.Table.Qualifier.String()
	tableName := ddl.Table.Name.String()
	var ok bool
	outScope, ok = b.buildTablescan(inScope, dbName, tableName, nil)
	if !ok {
		b.handleErr(sql.ErrTableNotFound.New(tableName))
	}
	table, ok := outScope.node.(*plan.ResolvedTable)
	if !ok {
		err := fmt.Errorf("expected resolved table: %s", tableName)
		b.handleErr(err)
	}

	cols := make([]sql.Expression, len(ddl.IndexSpec.Columns))
	for i, col := range ddl.IndexSpec.Columns {
		c, ok := inScope.resolveColumn(tableName, strings.ToLower(col.Column.String()), true)
		if !ok {
			b.handleErr(sql.ErrColumnNotFound.New(col.Column.String()))
		}
		cols[i] = expression.NewGetFieldWithTable(int(c.id), c.typ, c.table, c.col, c.nullable)
	}

	createIndex := plan.NewCreateIndex(
		ddl.IndexSpec.ToName.String(),
		table,
		cols,
		ddl.IndexSpec.Using.Lowered(),
		config,
	)
	createIndex.Catalog = b.cat
	return
}

// TableSpecToSchema creates a sql.Schema from a parsed TableSpec
func (b *Builder) tableSpecToSchema(inScope, outScope *scope, dbName, tableName string, tableSpec *ast.TableSpec, forceInvalidCollation bool) (sql.PrimaryKeySchema, sql.CollationID) {
	tableCollation := sql.Collation_Unspecified
	if !forceInvalidCollation {
		if len(tableSpec.Options) > 0 {
			charsetSubmatches := tableCharsetOptionRegex.FindStringSubmatch(tableSpec.Options)
			collationSubmatches := tableCollationOptionRegex.FindStringSubmatch(tableSpec.Options)
			if len(charsetSubmatches) == 5 && len(collationSubmatches) == 5 {
				var err error
				tableCollation, err = sql.ParseCollation(&charsetSubmatches[4], &collationSubmatches[4], false)
				if err != nil {
					return sql.PrimaryKeySchema{}, sql.Collation_Unspecified
				}
			} else if len(charsetSubmatches) == 5 {
				charset, err := sql.ParseCharacterSet(charsetSubmatches[4])
				if err != nil {
					return sql.PrimaryKeySchema{}, sql.Collation_Unspecified
				}
				tableCollation = charset.DefaultCollation()
			} else if len(collationSubmatches) == 5 {
				var err error
				tableCollation, err = sql.ParseCollation(nil, &collationSubmatches[4], false)
				if err != nil {
					return sql.PrimaryKeySchema{}, sql.Collation_Unspecified
				}
			}
		}
	}

	defaults := make([]ast.Expr, len(tableSpec.Columns))
	var schema sql.Schema
	for i, cd := range tableSpec.Columns {
		// Use the table's collation if no character or collation was specified for the table
		if len(cd.Type.Charset) == 0 && len(cd.Type.Collate) == 0 {
			if tableCollation != sql.Collation_Unspecified {
				cd.Type.Collate = tableCollation.Name()
			}
		}
		defaults[i] = cd.Type.Default
		cd.Type.Default = nil
		column := b.columnDefinitionToColumn(inScope, cd, tableSpec.Indexes)

		if column.PrimaryKey && bool(cd.Type.Null) {
			b.handleErr(ErrPrimaryKeyOnNullField.New())
		}

		schema = append(schema, column)
		outScope.newColumn(scopeColumn{
			db:       dbName,
			table:    tableName,
			col:      strings.ToLower(column.Name),
			typ:      column.Type,
			nullable: column.Nullable,
		})
	}

	for i, def := range defaults {
		schema[i].Default = b.convertDefaultExpression(outScope, def)
	}

	return sql.NewPrimaryKeySchema(schema, getPkOrdinals(tableSpec)...), tableCollation
}

// jsonTableSpecToSchemaHelper creates a sql.Schema from a parsed TableSpec
func (b *Builder) jsonTableSpecToSchemaHelper(jsonTableSpec *ast.JSONTableSpec, sch sql.Schema) {
	for _, cd := range jsonTableSpec.Columns {
		if cd.Spec != nil {
			b.jsonTableSpecToSchemaHelper(cd.Spec, sch)
			continue
		}
		typ, err := types.ColumnTypeToType(&cd.Type)
		if err != nil {
			b.handleErr(err)
		}
		col := &sql.Column{
			Type:          typ,
			Name:          cd.Name.String(),
			AutoIncrement: bool(cd.Type.Autoincrement),
		}
		sch = append(sch, col)
		continue
	}
}

// jsonTableSpecToSchema creates a sql.Schema from a parsed TableSpec
func (b *Builder) jsonTableSpecToSchema(tableSpec *ast.JSONTableSpec) sql.Schema {
	var sch sql.Schema
	b.jsonTableSpecToSchemaHelper(tableSpec, sch)
	return sch
}

// These constants aren't exported from vitess for some reason. This could be removed if we changed this.
const (
	colKeyNone ast.ColumnKeyOption = iota
	colKeyPrimary
	colKeySpatialKey
	colKeyUnique
	colKeyUniqueKey
	colKey
	colKeyFulltextKey
)

func getPkOrdinals(ts *ast.TableSpec) []int {
	for _, idxDef := range ts.Indexes {
		if idxDef.Info.Primary {

			pkOrdinals := make([]int, 0)
			colIdx := make(map[string]int)
			for i := 0; i < len(ts.Columns); i++ {
				colIdx[ts.Columns[i].Name.Lowered()] = i
			}

			for _, i := range idxDef.Columns {
				pkOrdinals = append(pkOrdinals, colIdx[i.Column.Lowered()])
			}

			return pkOrdinals
		}
	}

	// no primary key expression, check for inline PK column
	for i, col := range ts.Columns {
		if col.Type.KeyOpt == colKeyPrimary {
			return []int{i}
		}
	}

	return []int{}
}

// columnDefinitionToColumn returns the sql.Column for the column definition given, as part of a create table statement.
func (b *Builder) columnDefinitionToColumn(inScope *scope, cd *ast.ColumnDefinition, indexes []*ast.IndexDefinition) *sql.Column {
	internalTyp, err := types.ColumnTypeToType(&cd.Type)
	if err != nil {
		b.handleErr(err)
	}

	// Primary key info can either be specified in the column's type info (for in-line declarations), or in a slice of
	// indexes attached to the table def. We have to check both places to find if a column is part of the primary key
	isPkey := cd.Type.KeyOpt == colKeyPrimary

	if !isPkey {
	OuterLoop:
		for _, index := range indexes {
			if index.Info.Primary {
				for _, indexCol := range index.Columns {
					if indexCol.Column.Equal(cd.Name) {
						isPkey = true
						break OuterLoop
					}
				}
			}
		}
	}

	var comment string
	if cd.Type.Comment != nil && cd.Type.Comment.Type == ast.StrVal {
		comment = string(cd.Type.Comment.Val)
	}

	defaultVal := b.convertDefaultExpression(inScope, cd.Type.Default)

	extra := ""

	if cd.Type.Autoincrement {
		extra = "auto_increment"
	}

	if cd.Type.SRID != nil {
		sridVal, err := strconv.ParseInt(string(cd.Type.SRID.Val), 10, 32)
		if err != nil {
			b.handleErr(err)
		}

		if err = types.ValidateSRID(int(sridVal), ""); err != nil {
			b.handleErr(err)
		}
		if s, ok := internalTyp.(sql.SpatialColumnType); ok {
			internalTyp = s.SetSRID(uint32(sridVal))
		} else {
			b.handleErr(sql.ErrInvalidType.New(fmt.Sprintf("cannot define SRID for %s", internalTyp)))
		}
	}

	return &sql.Column{
		Nullable:      !isPkey && !bool(cd.Type.NotNull),
		Type:          internalTyp,
		Name:          cd.Name.String(),
		PrimaryKey:    isPkey,
		Default:       defaultVal,
		AutoIncrement: bool(cd.Type.Autoincrement),
		Comment:       comment,
		Extra:         extra,
	}
}

func (b *Builder) convertDefaultExpression(inScope *scope, defaultExpr ast.Expr) *sql.ColumnDefaultValue {
	if defaultExpr == nil {
		return nil
	}
	parsedExpr := b.buildScalar(inScope, defaultExpr)

	// Function expressions must be enclosed in parentheses (except for current_timestamp() and now())
	_, isParenthesized := defaultExpr.(*ast.ParenExpr)
	isLiteral := !isParenthesized

	// A literal will never have children, thus we can also check for that.
	if unaryExpr, is := defaultExpr.(*ast.UnaryExpr); is {
		if _, lit := unaryExpr.Expr.(*ast.SQLVal); lit {
			isLiteral = true
		}
	} else if !isParenthesized {
		if _, ok := parsedExpr.(sql.FunctionExpression); ok {
			switch parsedExpr.(type) {
			case *function.Now, *function.CurrTimestamp:
				// Datetime and Timestamp columns allow now and current_timestamp to not be enclosed in parens,
				// but they still need to be treated as function expressions
				isLiteral = false
			default:
				// All other functions must *always* be enclosed in parens
				err := sql.ErrSyntaxError.New("column default function expressions must be enclosed in parentheses")
				b.handleErr(err)
			}
		}
	}

	return &sql.ColumnDefaultValue{
		Expression:    parsedExpr,
		OutType:       nil,
		Literal:       isLiteral,
		ReturnNil:     true,
		Parenthesized: isParenthesized,
	}
}

func (b *Builder) buildDBDDL(inScope *scope, c *ast.DBDDL) (outScope *scope) {
	outScope = inScope.push()
	switch strings.ToLower(c.Action) {
	case ast.CreateStr:
		var charsetStr *string
		var collationStr *string
		for _, cc := range c.CharsetCollate {
			ccType := strings.ToLower(cc.Type)
			if ccType == "character set" {
				val := cc.Value
				charsetStr = &val
			} else if ccType == "collate" {
				val := cc.Value
				collationStr = &val
			} else {
				b.ctx.Session.Warn(&sql.Warning{
					Level:   "Warning",
					Code:    mysql.ERNotSupportedYet,
					Message: "Setting CHARACTER SET, COLLATION and ENCRYPTION are not supported yet",
				})
			}
		}
		collation, err := sql.ParseCollation(charsetStr, collationStr, false)
		if err != nil {
			b.handleErr(err)
		}
		createDb := plan.NewCreateDatabase(c.DBName, c.IfNotExists, collation)
		createDb.Catalog = b.cat
		outScope.node = createDb
	case ast.DropStr:
		dropDb := plan.NewDropDatabase(c.DBName, c.IfExists)
		dropDb.Catalog = b.cat
		outScope.node = dropDb
	case ast.AlterStr:
		if len(c.CharsetCollate) == 0 {
			if len(c.DBName) > 0 {
				err := sql.ErrSyntaxError.New(fmt.Sprintf("alter database %s", c.DBName))
				b.handleErr(err)
			} else {
				err := sql.ErrSyntaxError.New("alter database")
				b.handleErr(err)
			}
		}

		var charsetStr *string
		var collationStr *string
		for _, cc := range c.CharsetCollate {
			ccType := strings.ToLower(cc.Type)
			if ccType == "character set" {
				val := cc.Value
				charsetStr = &val
			} else if ccType == "collate" {
				val := cc.Value
				collationStr = &val
			}
		}
		collation, err := sql.ParseCollation(charsetStr, collationStr, false)
		if err != nil {
			b.handleErr(err)
		}
		alterDb := plan.NewAlterDatabase(c.DBName, collation)
		alterDb.Catalog = b.cat
		outScope.node = alterDb
	default:
		err := sql.ErrUnsupportedSyntax.New(ast.String(c))
		b.handleErr(err)
	}
	return outScope
}
