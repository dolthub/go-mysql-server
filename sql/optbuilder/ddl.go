package optbuilder

import (
	"fmt"
	"strconv"
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *PlanBuilder) resolveDb(name string) sql.Database {
	database, err := b.cat.Database(b.ctx, name)
	if err != nil {
		b.handleErr(err)
	}

	if privilegedDatabase, ok := database.(mysql_db.PrivilegedDatabase); ok {
		database = privilegedDatabase.Unwrap()
	}
	return database
}

func (b *PlanBuilder) buildDDL(inScope *scope, query string, c *ast.DDL) (outScope *scope) {
	switch strings.ToLower(c.Action) {
	case ast.CreateStr:
		if c.TriggerSpec != nil {
			//return buildCreateTrigger(ctx, query, c)
			panic("todo")
		}
		if c.ProcedureSpec != nil {
			//return buildCreateProcedure(ctx, query, c)
			panic("todo")
		}
		if c.EventSpec != nil {
			//return buildCreateEvent(ctx, query, c)
			panic("todo")
		}
		if c.ViewSpec != nil {
			//return buildCreateView(ctx, query, c)
			panic("todo")
		}
		return b.buildCreateTable(inScope, c)
	case ast.DropStr:
		// get database
		if c.TriggerSpec != nil {
			dbName := c.TriggerSpec.TrigName.Qualifier.String()
			trigName := c.TriggerSpec.TrigName.Name.String()
			outScope.node = plan.NewDropTrigger(b.resolveDb(dbName), trigName, c.IfExists)
			return
		}
		if c.ProcedureSpec != nil {
			dbName := c.ProcedureSpec.ProcName.Qualifier.String()
			procName := c.ProcedureSpec.ProcName.Name.String()
			outScope.node = plan.NewDropProcedure(b.resolveDb(dbName), procName, c.IfExists)
			return
		}
		if c.EventSpec != nil {
			dbName := c.EventSpec.EventName.Qualifier.String()
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
		return b.buildAlterTable(inScope, c)
	case ast.RenameStr:
		return b.buildRenameTable(inScope, c)
	case ast.TruncateStr:
		return b.buildTruncateTable(inScope, c)
	default:
		err := sql.ErrUnsupportedSyntax.New(ast.String(c))
		b.handleErr(err)
	}
	return
}

func (b *PlanBuilder) buildDropTable(inScope *scope, c *ast.DDL) (outScope *scope) {
	outScope = inScope.push()
	dropTables := make([]sql.Node, len(c.FromTables))
	dbName := c.FromTables[0].Qualifier.String()
	for i, t := range c.FromTables {
		if t.Qualifier.String() != dbName {
			err := sql.ErrUnsupportedFeature.New("dropping tables on multiple databases in the same statement")
			b.handleErr(err)
		}
		tableScope := b.buildTablescan(inScope, dbName, t.Name.String(), nil)
		dropTables[i] = tableScope.node
	}

	outScope.node = plan.NewDropTable(dropTables, c.IfExists)
	return
}

func (b *PlanBuilder) buildTruncateTable(inScope *scope, c *ast.DDL) (outScope *scope) {
	outScope = inScope.push()
	dbName := c.Table.Qualifier.String()
	tabName := c.Table.Name.String()
	tableScope := b.buildTablescan(inScope, dbName, tabName, nil)
	outScope.node = plan.NewTruncate(
		c.Table.Qualifier.String(),
		tableScope.node,
	)
	return
}

func (b *PlanBuilder) buildCreateTable(inScope *scope, c *ast.DDL) (outScope *scope) {
	outScope = inScope.push()
	if c.OptLike != nil {
		tableName := c.OptLike.LikeTable.Name.String()
		dbName := c.OptLike.LikeTable.Qualifier.String()
		outScope = b.buildTablescan(inScope, dbName, tableName, nil)
		table, ok := outScope.node.(*plan.ResolvedTable)
		if !ok {
			err := fmt.Errorf("expected resolved table: %s", tableName)
			b.handleErr(err)
		}
		outScope.node = plan.NewCreateTableLike(
			table.Database,
			table.Database.Name(),
			table,
			plan.IfNotExistsOption(c.IfNotExists),
			plan.TempTableOption(c.Temporary),
		)
		return outScope
	}

	// In the case that no table spec is given but a SELECT Statement return the CREATE TABLE node.
	// if the table spec != nil it will get parsed below.
	if c.TableSpec == nil && c.OptSelect != nil {
		tableSpec := &plan.TableSpec{}

		selectScope := b.buildSelectStmt(inScope, c.OptSelect.Select)

		outScope.node = plan.NewCreateTableSelect(b.currentDb(), c.Table.Name.String(), selectScope.node, tableSpec, plan.IfNotExistsOption(c.IfNotExists), plan.TempTableOption(c.Temporary))
		return outScope
	}

	fkDefs, chDefs := b.buildConstraintsDefs(inScope, c.Table, c.TableSpec)
	idxDefs := b.buildIndexDefs(inScope, c.TableSpec)

	qualifier := c.Table.Qualifier.String()
	if qualifier == "" {
		qualifier = b.ctx.GetCurrentDatabase()
	}
	database := b.resolveDb(qualifier)
	schema, collation := b.tableSpecToSchema(inScope, c.TableSpec, false)

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

func (b *PlanBuilder) buildRenameTable(inScope *scope, ddl *ast.DDL) (outScope *scope) {
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

	outScope.node = plan.NewRenameTable(b.currentDb(), fromTables, toTables)
	return
}

func (b *PlanBuilder) buildAlterTable(inScope *scope, ddl *ast.DDL) (outScope *scope) {
	if ddl.IndexSpec != nil {
		return b.buildAlterIndex(inScope, ddl)
	}
	if ddl.ConstraintAction != "" && len(ddl.TableSpec.Constraints) == 1 {
		dbName := ddl.Table.Qualifier.String()
		tabName := ddl.Table.Name.String()
		outScope = b.buildTablescan(inScope, dbName, tabName, nil)
		table, ok := outScope.node.(*plan.ResolvedTable)
		if !ok {
			err := fmt.Errorf("expected resolved table: %s", tabName)
			b.handleErr(err)
		}
		parsedConstraint := b.buildConstraintDefinition(outScope, ddl.TableSpec.Constraints[0])
		switch strings.ToLower(ddl.ConstraintAction) {
		case ast.AddStr:
			switch c := parsedConstraint.(type) {
			case *sql.ForeignKeyConstraint:
				c.Database = table.Database.Name()
				c.Table = table.Name()
				outScope.node = plan.NewAlterAddForeignKey(c)
			case *sql.CheckConstraint:
				outScope.node = plan.NewAlterAddCheck(table, c)
			default:
				err := sql.ErrUnsupportedFeature.New(ast.String(ddl))
				b.handleErr(err)
			}
		case ast.DropStr:
			switch c := parsedConstraint.(type) {
			case *sql.ForeignKeyConstraint:
				database := table.Database.Name()
				outScope.node = plan.NewAlterDropForeignKey(database, table.Name(), c.Name)
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
	if ddl.ColumnAction != "" {
		dbName := ddl.Table.Qualifier.String()
		tabName := ddl.Table.Name.String()
		outScope = b.buildTablescan(inScope, dbName, tabName, nil)
		table, ok := outScope.node.(*plan.ResolvedTable)
		if !ok {
			err := fmt.Errorf("expected resolved table: %s", tabName)
			b.handleErr(err)
		}
		switch strings.ToLower(ddl.ColumnAction) {
		case ast.AddStr:
			sch, _ := b.tableSpecToSchema(inScope, ddl.TableSpec, true)
			outScope.node = plan.NewAddColumnResolved(table, sch.Schema[0], columnOrderToColumnOrder(ddl.ColumnOrder))
		case ast.DropStr:
			outScope.node = plan.NewDropColumnResolved(table, ddl.Column.String())
		case ast.RenameStr:
			outScope.node = plan.NewRenameColumnResolved(table, ddl.Column.String(), ddl.ToColumn.String())
		case ast.ModifyStr, ast.ChangeStr:
			sch, _ := b.tableSpecToSchema(inScope, ddl.TableSpec, true)
			outScope.node = plan.NewModifyColumnResolved(table, ddl.Column.String(), sch.Schema[0], columnOrderToColumnOrder(ddl.ColumnOrder))
		}
		// TODO default case?
		return
	}
	if ddl.AutoIncSpec != nil {
		return b.buildAlterAutoIncrement(inScope, ddl)
	} else if ddl.DefaultSpec != nil {
		return b.buildAlterDefault(inScope, ddl)
	} else if ddl.AlterCollationSpec != nil {
		return b.buildAlterCollationSpec(inScope, ddl)
	}
	err := sql.ErrUnsupportedFeature.New(ast.String(ddl))
	b.handleErr(err)
	return
}

func (b *PlanBuilder) buildConstraintsDefs(inScope *scope, tname ast.TableName, spec *ast.TableSpec) (fks []*sql.ForeignKeyConstraint, checks []*sql.CheckConstraint) {
	for _, unknownConstraint := range spec.Constraints {
		parsedConstraint := b.buildConstraintDefinition(inScope, unknownConstraint)
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

func (b *PlanBuilder) buildIndexDefs(inScope *scope, spec *ast.TableSpec) (idxDefs []*plan.IndexDefinition) {
	for _, idxDef := range spec.Indexes {
		constraint := sql.IndexConstraint_None
		if idxDef.Info.Primary {
			constraint = sql.IndexConstraint_Primary
		} else if idxDef.Info.Unique {
			constraint = sql.IndexConstraint_Unique
		} else if idxDef.Info.Spatial {
			constraint = sql.IndexConstraint_Spatial
		} else if idxDef.Info.Fulltext {
			// TODO: We do not support FULLTEXT indexes or keys
			err := sql.ErrUnsupportedFeature.New("fulltext keys are unsupported")
			b.handleErr(err)
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
			err := sql.ErrUnsupportedFeature.New("fulltext keys are unsupported")
			b.handleErr(err)
		}
		if colDef.Type.KeyOpt == colKeyUnique || colDef.Type.KeyOpt == colKeyUniqueKey {
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

func (b *PlanBuilder) buildConstraintDefinition(inScope *scope, cd *ast.ConstraintDefinition) interface{} {
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

func (b *PlanBuilder) buildReferentialAction(action ast.ReferenceAction) sql.ForeignKeyReferentialAction {
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

func (b *PlanBuilder) buildAlterIndex(inScope *scope, ddl *ast.DDL) (outScope *scope) {
	dbName := ddl.Table.Qualifier.String()
	tabName := ddl.Table.Name.String()
	outScope = b.buildTablescan(inScope, dbName, tabName, nil)
	table, ok := outScope.node.(*plan.ResolvedTable)
	if !ok {
		err := fmt.Errorf("expected resolved table: %s", tabName)
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
			err := sql.ErrUnsupportedFeature.New("fulltext keys are unsupported")
			b.handleErr(err)
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
			outScope.node = plan.NewAlterCreatePk(table.Database, table, columns)
			return
		}

		outScope.node = plan.NewAlterCreateIndex(table.Database, table, ddl.IndexSpec.ToName.String(), using, constraint, columns, comment)
		return
	case ast.DropStr:
		if ddl.IndexSpec.Type == ast.PrimaryStr {
			outScope.node = plan.NewAlterDropPk(table.Database, table)
			return
		}
		outScope.node = plan.NewAlterDropIndex(table.Database, table, ddl.IndexSpec.ToName.String())
		return
	case ast.RenameStr:
		outScope.node = plan.NewAlterRenameIndex(table.Database, table, ddl.IndexSpec.FromName.String(), ddl.IndexSpec.ToName.String())
		return
	case "disable":
		outScope.node = plan.NewAlterDisableEnableKeys(table.Database, table, true)
		return
	case "enable":
		outScope.node = plan.NewAlterDisableEnableKeys(table.Database, table, false)
		return
	default:
		err := sql.ErrUnsupportedFeature.New(ast.String(ddl))
		b.handleErr(err)
	}
	return
}

func (b *PlanBuilder) gatherIndexColumns(cols []*ast.IndexColumn) []sql.IndexColumn {
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

func (b *PlanBuilder) buildAlterAutoIncrement(inScope *scope, ddl *ast.DDL) (outScope *scope) {
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
	tabName := ddl.Table.Name.String()
	outScope = b.buildTablescan(inScope, dbName, tabName, nil)
	table, ok := outScope.node.(*plan.ResolvedTable)
	if !ok {
		err := fmt.Errorf("expected resolved table: %s", tabName)
		b.handleErr(err)
	}
	outScope.node = plan.NewAlterAutoIncrement(table.Database, table, autoVal)
	return
}

func (b *PlanBuilder) buildAlterDefault(inScope *scope, ddl *ast.DDL) (outScope *scope) {
	dbName := ddl.Table.Qualifier.String()
	tabName := ddl.Table.Name.String()
	outScope = b.buildTablescan(inScope, dbName, tabName, nil)
	table, ok := outScope.node.(*plan.ResolvedTable)
	if !ok {
		err := fmt.Errorf("expected resolved table: %s", tabName)
		b.handleErr(err)
	}
	switch strings.ToLower(ddl.DefaultSpec.Action) {
	case ast.SetStr:
		defaultVal := b.buildDefaultExpression(inScope, ddl.DefaultSpec.Value)
		outScope.node = plan.NewAlterDefaultSet(table.Database, table, ddl.DefaultSpec.Column.String(), defaultVal)
		return
	case ast.DropStr:
		outScope.node = plan.NewAlterDefaultDrop(table.Database, table, ddl.DefaultSpec.Column.String())
		return
	default:
		err := sql.ErrUnsupportedFeature.New(ast.String(ddl))
		b.handleErr(err)
	}
	return
}

func (b *PlanBuilder) buildAlterCollationSpec(inScope *scope, ddl *ast.DDL) (outScope *scope) {
	dbName := ddl.Table.Qualifier.String()
	tabName := ddl.Table.Name.String()
	outScope = b.buildTablescan(inScope, dbName, tabName, nil)
	table, ok := outScope.node.(*plan.ResolvedTable)
	if !ok {
		err := fmt.Errorf("expected resolved table: %s", tabName)
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

func (b *PlanBuilder) buildDefaultExpression(inScope *scope, defaultExpr ast.Expr) *sql.ColumnDefaultValue {
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

func (b *PlanBuilder) buildExternalCreateIndex(inScope *scope, ddl *ast.DDL) (outScope *scope) {
	config := make(map[string]string)
	for _, option := range ddl.IndexSpec.Options {
		if option.Using != "" {
			config[option.Name] = option.Using
		} else {
			config[option.Name] = string(option.Value.Val)
		}
	}

	dbName := ddl.Table.Qualifier.String()
	tabName := ddl.Table.Name.String()
	outScope = b.buildTablescan(inScope, dbName, tabName, nil)
	table, ok := outScope.node.(*plan.ResolvedTable)
	if !ok {
		err := fmt.Errorf("expected resolved table: %s", tabName)
		b.handleErr(err)
	}

	cols := make([]sql.Expression, len(ddl.IndexSpec.Columns))
	for i, col := range ddl.IndexSpec.Columns {
		c, idx := b.resolveColumn(inScope, tabName, col.Column.String(), true)
		if idx == -1 {
			b.handleErr(sql.ErrColumnNotFound.New(col.Column.String()))
		}
		cols[i] = expression.NewGetFieldWithTable(int(c.id), c.typ, c.table, c.col, c.nullable)
	}

	outScope.node = plan.NewCreateIndex(
		ddl.IndexSpec.ToName.String(),
		table,
		cols,
		ddl.IndexSpec.Using.Lowered(),
		config,
	)
	return
}
