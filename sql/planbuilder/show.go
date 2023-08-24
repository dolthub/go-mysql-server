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

package planbuilder

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"strings"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func (b *Builder) buildShow(inScope *scope, s *ast.Show, query string) (outScope *scope) {
	showType := strings.ToLower(s.Type)
	switch showType {
	case "processlist":
		outScope.node = plan.NewShowProcessList()
	case ast.CreateTableStr, "create view":
		return b.buildShowTable(inScope, s, showType)
	case "create database", "create schema":
		return b.buildShowDatabase(inScope, s)
	case ast.CreateTriggerStr:
		return b.buildShowTrigger(inScope, s)
	case ast.CreateProcedureStr:
		return b.buildShowProcedure(inScope, s)
	case ast.CreateEventStr:
		return b.buildShowEvent(inScope, s)
	case "triggers":
		return b.buildShowAllTriggers(inScope, s)
	case "events":
		return b.buildShowAllEvents(inScope, s)
	case ast.ProcedureStatusStr:
		return b.buildShowProcedureStatus(inScope, s)
	case ast.FunctionStatusStr:
		return b.buildShowFunctionStatus(inScope, s)
	case ast.TableStatusStr:
		return b.buildShowTableStatus(inScope, s)
	case "index":
		return b.buildShowIndex(inScope, s)
	case ast.KeywordString(ast.VARIABLES):
		return b.buildShowVariables(inScope, s)
	case ast.KeywordString(ast.TABLES):
		return b.buildShowAllTables(inScope, s)
	case ast.KeywordString(ast.DATABASES), ast.KeywordString(ast.SCHEMAS):
		return b.buildShowAllDatabases(inScope, s)
	case ast.KeywordString(ast.FIELDS), ast.KeywordString(ast.COLUMNS):
		return b.buildShowAllColumns(inScope, s)
	case ast.KeywordString(ast.WARNINGS):
		return b.buildShowWarnings(inScope, s)
	case ast.KeywordString(ast.COLLATION):
		return b.buildShowCollation(inScope, s)
	case ast.KeywordString(ast.CHARSET):
		return b.buildShowCharset(inScope, s)
	case ast.KeywordString(ast.ENGINES):
		return b.buildShowEngines(inScope, s)
	case ast.KeywordString(ast.STATUS):
		return b.buildShowStatus(inScope, s)
	case "replica status":
		outScope = inScope.push()
		showRep := plan.NewShowReplicaStatus()
		if binCat, ok := b.cat.(binlogreplication.BinlogReplicaCatalog); ok && binCat.IsBinlogReplicaCatalog() {
			showRep.ReplicaController = binCat.GetBinlogReplicaController()
		}
		outScope.node = showRep
	default:
		unsupportedShow := fmt.Sprintf("SHOW %s", s.Type)
		b.handleErr(sql.ErrUnsupportedFeature.New(unsupportedShow))
	}
	return
}

func (b *Builder) buildShowTable(inScope *scope, s *ast.Show, showType string) (outScope *scope) {
	outScope = inScope.push()
	var asOf *ast.AsOf
	var asOfExpr sql.Expression
	if s.ShowTablesOpt != nil && s.ShowTablesOpt.AsOf != nil {
		asOfExpr = b.buildAsOfExpr(inScope, s.ShowTablesOpt.AsOf)
		asOf = &ast.AsOf{Time: s.ShowTablesOpt.AsOf}
	}

	db := s.Database
	if db == "" {
		db = s.Table.Qualifier.String()
	}
	if db == "" {
		db = b.currentDb().Name()
	}

	tableName := strings.ToLower(s.Table.Name.String())
	tableScope, ok := b.buildTablescan(inScope, db, tableName, asOf)
	if !ok {
		err := sql.ErrTableNotFound.New(tableName)
		b.handleErr(err)
	}
	rt, _ := tableScope.node.(*plan.ResolvedTable)
	for _, c := range tableScope.node.Schema() {
		outScope.newColumn(scopeColumn{
			db:       strings.ToLower(db),
			table:    strings.ToLower(c.Source),
			col:      strings.ToLower(c.Name),
			typ:      c.Type,
			nullable: c.Nullable,
		})
	}

	showCreate := plan.NewShowCreateTableWithAsOf(tableScope.node, showType == "create view", asOfExpr)
	outScope.node = showCreate

	if rt != nil {
		checks := b.loadChecksFromTable(outScope, rt.Table)
		// To match MySQL output format, transform the column names and wrap with backticks
		for i, check := range checks {
			checks[i].Expr, _, _ = transform.Expr(check.Expr, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				if t, ok := e.(*expression.GetField); ok {
					return expression.NewUnresolvedColumn(fmt.Sprintf("`%s`", t.Name())), transform.NewTree, nil
				}
				return e, transform.SameTree, nil
			})
		}
		showCreate.Checks = checks

		showCreate.Indexes = b.getInfoSchemaIndexes(rt)

		pks, _ := rt.Table.(sql.PrimaryKeyTable)
		if pks != nil {
			showCreate.PrimaryKeySchema = pks.PrimaryKeySchema()
		}
		outScope.node = b.modifySchemaTarget(outScope, showCreate, rt)

	}
	return
}

func (b *Builder) buildShowDatabase(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	dbName := s.Database
	if dbName == "" {
		dbName = b.ctx.GetCurrentDatabase()
	}
	db := b.resolveDb(dbName)
	outScope.node = plan.NewShowCreateDatabase(
		db,
		s.IfNotExists,
	)
	return
}

func (b *Builder) buildShowTrigger(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	dbName := s.Table.Qualifier.String()
	if dbName == "" {
		dbName = b.ctx.GetCurrentDatabase()
	}
	db, err := b.cat.Database(b.ctx, dbName)
	if err != nil {
		b.handleErr(err)
	}
	outScope.node = plan.NewShowCreateTrigger(db, s.Table.Name.String())
	return
}

func (b *Builder) buildShowAllTriggers(inScope *scope, s *ast.Show) (outScope *scope) {
	dbName := s.Table.Qualifier.String()
	if dbName == "" {
		dbName = b.ctx.GetCurrentDatabase()
	}
	if dbName == "" && &s.ShowTablesOpt != nil {
		dbName = s.ShowTablesOpt.DbName
	}
	db := b.resolveDb(dbName)

	var node sql.Node = plan.NewShowTriggers(db)

	outScope = inScope.push()
	for _, c := range node.Schema() {
		outScope.newColumn(scopeColumn{
			db:       strings.ToLower(db.Name()),
			table:    strings.ToLower(c.Source),
			col:      strings.ToLower(c.Name),
			typ:      c.Type,
			nullable: c.Nullable,
		})
	}
	var filter sql.Expression
	if s.ShowTablesOpt != nil {
		dbName = s.ShowTablesOpt.DbName
		if s.ShowTablesOpt.Filter != nil {
			if s.ShowTablesOpt.Filter.Filter != nil {
				filter = b.buildScalar(outScope, s.ShowTablesOpt.Filter.Filter)
			} else if s.ShowTablesOpt.Filter.Like != "" {
				filter = expression.NewLike(
					expression.NewGetField(2, types.LongText, "Table", false),
					expression.NewLiteral(s.ShowTablesOpt.Filter.Like, types.LongText),
					nil,
				)
			}
		}
	}

	if filter != nil {
		node = plan.NewFilter(filter, node)
	}

	outScope.node = node
	return
}

func (b *Builder) buildShowEvent(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	dbName := strings.ToLower(s.Table.Qualifier.String())
	if dbName == "" {
		dbName = b.ctx.GetCurrentDatabase()
	}
	outScope.node = plan.NewShowCreateEvent(b.resolveDb(dbName), s.Table.Name.String())
	return
}

func (b *Builder) buildShowAllEvents(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	var dbName string

	if dbName == "" {
		dbName = b.ctx.GetCurrentDatabase()
	}
	db := b.resolveDb(dbName)
	var node sql.Node = plan.NewShowEvents(db)
	for _, c := range node.Schema() {
		outScope.newColumn(scopeColumn{table: c.Source, col: c.Name, typ: c.Type, nullable: c.Nullable})
	}
	var filter sql.Expression
	if s.ShowTablesOpt != nil {
		dbName = s.ShowTablesOpt.DbName
		if s.ShowTablesOpt.Filter != nil {
			if s.ShowTablesOpt.Filter.Filter != nil {
				filter = b.buildScalar(outScope, s.ShowTablesOpt.Filter.Filter)
			} else if s.ShowTablesOpt.Filter.Like != "" {
				filter = expression.NewLike(
					expression.NewGetField(1, types.LongText, "name", false),
					expression.NewLiteral(s.ShowTablesOpt.Filter.Like, types.LongText),
					nil,
				)
			}
		}
	}
	if filter != nil {
		node = plan.NewFilter(filter, node)
	}

	outScope.node = node
	return
}

func (b *Builder) buildShowProcedure(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	var db sql.Database
	dbName := s.Table.Qualifier.String()
	if dbName != "" {
		db = b.resolveDb(dbName)
	} else {
		db = b.currentDb()
	}
	outScope.node = plan.NewShowCreateProcedure(db, s.Table.Name.String())
	return
}

func (b *Builder) buildShowProcedureStatus(inScope *scope, s *ast.Show) (outScope *scope) {
	var filter sql.Expression

	node, _, _, err := b.Parse("select routine_schema as `Db`, routine_name as `Name`, routine_type as `Type`,"+
		"definer as `Definer`, last_altered as `Modified`, created as `Created`, security_type as `Security_type`,"+
		"routine_comment as `Comment`, CHARACTER_SET_CLIENT as `character_set_client`, COLLATION_CONNECTION as `collation_connection`,"+
		"database_collation as `Database Collation` from information_schema.routines where routine_type = 'PROCEDURE'", false)
	if err != nil {
		b.handleErr(err)
	}

	outScope = inScope.push()
	for _, c := range node.Schema() {
		outScope.newColumn(scopeColumn{table: c.Source, col: c.Name, typ: c.Type, nullable: c.Nullable})
	}
	if s.Filter != nil {
		if s.Filter.Filter != nil {
			filter = b.buildScalar(outScope, s.Filter.Filter)
		} else if s.Filter.Like != "" {
			filter = expression.NewLike(
				expression.NewGetField(1, types.MustCreateString(sqltypes.VarChar, 64, sql.Collation_Information_Schema_Default), "Name", false),
				expression.NewLiteral(s.Filter.Like, types.LongText),
				nil,
			)
		}
	}

	if filter != nil {
		node = plan.NewHaving(filter, node)
	}
	outScope.node = node
	return
}

func (b *Builder) buildShowFunctionStatus(inScope *scope, s *ast.Show) (outScope *scope) {
	var filter sql.Expression
	node, _, _, err := b.Parse("select routine_schema as `Db`, routine_name as `Name`, routine_type as `Type`,"+
		"definer as `Definer`, last_altered as `Modified`, created as `Created`, security_type as `Security_type`,"+
		"routine_comment as `Comment`, character_set_client, collation_connection,"+
		"database_collation as `Database Collation` from information_schema.routines where routine_type = 'FUNCTION'", false)
	if err != nil {
		b.handleErr(err)
	}

	outScope = inScope.push()
	for _, c := range node.Schema() {
		outScope.newColumn(scopeColumn{table: c.Source, col: c.Name, typ: c.Type, nullable: c.Nullable})
	}

	if s.Filter != nil {
		if s.Filter.Filter != nil {
			filter = b.buildScalar(outScope, s.Filter.Filter)
		} else if s.Filter.Like != "" {
			filter = expression.NewLike(
				expression.NewGetField(1, types.MustCreateString(sqltypes.VarChar, 64, sql.Collation_Information_Schema_Default), "Name", false),
				expression.NewLiteral(s.Filter.Like, types.LongText),
				nil,
			)
		}
	}

	if filter != nil {
		node = plan.NewHaving(filter, node)
	}
	outScope.node = node
	return
}

func (b *Builder) buildShowTableStatus(inScope *scope, s *ast.Show) (outScope *scope) {
	dbName := b.ctx.GetCurrentDatabase()
	if s.Database != "" {
		dbName = s.Database
	}

	if dbName == "" {
		dbName = b.ctx.GetCurrentDatabase()
	}
	db := b.resolveDb(dbName)

	showStatus := plan.NewShowTableStatus(db)
	showStatus.Catalog = b.cat
	var node sql.Node = showStatus

	outScope = inScope.push()
	for _, c := range node.Schema() {
		outScope.newColumn(scopeColumn{
			db:       strings.ToLower(db.Name()),
			table:    strings.ToLower(c.Source),
			col:      strings.ToLower(c.Name),
			typ:      c.Type,
			nullable: c.Nullable,
		})
	}

	var filter sql.Expression
	if s.Filter != nil {
		if s.Filter.Filter != nil {
			filter = b.buildScalar(outScope, s.Filter.Filter)
		} else if s.Filter.Like != "" {
			filter = expression.NewLike(
				expression.NewGetField(0, types.LongText, "Name", false),
				expression.NewLiteral(s.Filter.Like, types.LongText),
				nil,
			)
		}
	}

	if filter != nil {
		node = plan.NewFilter(filter, node)
	}

	outScope.node = node
	return
}

func (b *Builder) buildShowIndex(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	dbName := s.Table.Qualifier.String()
	if dbName == "" {
		dbName = b.ctx.GetCurrentDatabase()
	}
	tableName := strings.ToLower(s.Table.Name.String())
	table := b.resolveTable(tableName, strings.ToLower(dbName), nil)
	showIdx := plan.NewShowIndexes(table)
	showIdx.IndexesToShow = b.getInfoSchemaIndexes(table)
	outScope.node = showIdx
	return
}

func (b *Builder) getInfoSchemaIndexes(rt *plan.ResolvedTable) []sql.Index {
	it, ok := rt.Table.(sql.IndexAddressableTable)
	if !ok {
		return nil
	}

	indexes, err := it.GetIndexes(b.ctx)
	if err != nil {
		b.handleErr(err)
	}

	for i := 0; i < len(indexes); i++ {
		// remove generated indexes
		idx := indexes[i]
		if idx.IsGenerated() {
			indexes[i], indexes[len(indexes)-1] = indexes[len(indexes)-1], indexes[i]
			indexes = indexes[:len(indexes)-1]
			i--
		}
	}

	if b.ctx.GetIndexRegistry().HasIndexes() {
		idxRegistry := b.ctx.GetIndexRegistry()
		for _, idx := range idxRegistry.IndexesByTable(rt.Database().Name(), rt.Table.Name()) {
			if !idx.IsGenerated() {
				indexes = append(indexes, idx)
			}
		}
	}

	return indexes
}

func (b *Builder) buildShowVariables(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	dummy := &plan.ShowVariables{}
	for _, c := range dummy.Schema() {
		outScope.newColumn(scopeColumn{
			table:    strings.ToLower(c.Source),
			col:      strings.ToLower(c.Name),
			typ:      c.Type,
			nullable: c.Nullable,
		})
	}

	var filter sql.Expression
	var like sql.Expression
	if s.Filter != nil {
		if s.Filter.Filter != nil {
			filter = b.buildScalar(outScope, s.Filter.Filter)
		}
		if s.Filter.Like != "" {
			like = expression.NewLike(
				expression.NewGetField(0, types.LongText, "variable_name", false),
				expression.NewLiteral(s.Filter.Like, types.LongText),
				nil,
			)
			if filter != nil {
				filter = expression.NewAnd(like, filter)
			} else {
				filter = like
			}
		}
	}
	if filter == nil {
		filter = expression.NewLiteral(true, types.Boolean)
	}
	outScope.node = plan.NewShowVariables(filter, strings.ToLower(s.Scope) == "global")

	return
}

func (b *Builder) buildAsOfLit(inScope *scope, t ast.Expr) interface{} {
	expr := b.buildAsOfExpr(inScope, t)
	res, err := expr.Eval(b.ctx, nil)
	if err != nil {
		b.handleErr(err)
	}
	switch res.(type) {
	case string, time.Time:
		return res
	}

	if res != nil {
		err = sql.ErrInvalidAsOfExpression.New(res)
	} else {
		err = sql.ErrInvalidAsOfExpression.New(t)
	}
	b.handleErr(err)
	return nil
}

func (b *Builder) buildAsOfExpr(inScope *scope, time ast.Expr) sql.Expression {
	switch v := time.(type) {
	case *ast.SQLVal:
		ret, _, err := types.Text.Convert(v.Val)
		if err != nil {
			b.handleErr(err)
		}
		return expression.NewLiteral(ret.(string), types.LongText)
	case *ast.ColName:
		sysVar, _, ok := b.buildSysVar(v, ast.SetScope_None)
		if ok {
			return sysVar
		}
		return expression.NewLiteral(v.String(), types.LongText)
	case *ast.FuncExpr:
		// todo(max): more specific validation for nested ASOF functions
		if isWindowFunc(v.Name.Lowered()) || isAggregateFunc(v.Name.Lowered()) {
			err := sql.ErrInvalidAsOfExpression.New(v)
			b.handleErr(err)
		}
	default:
	}
	return b.buildScalar(b.newScope(), time)
}

func (b *Builder) buildShowAllTables(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()

	var dbName string
	var filter sql.Expression
	var asOf sql.Expression
	if s.ShowTablesOpt != nil {
		dbName = s.ShowTablesOpt.DbName
		if s.ShowTablesOpt.AsOf != nil {
			asOf = b.buildAsOfExpr(inScope, s.ShowTablesOpt.AsOf)
		}
	}

	if dbName == "" {
		dbName = b.ctx.GetCurrentDatabase()
	}
	db := b.resolveDb(dbName)

	showTabs := plan.NewShowTables(db, s.Full, asOf)
	for _, c := range showTabs.Schema() {
		outScope.newColumn(scopeColumn{table: c.Source, col: c.Name, typ: c.Type, nullable: c.Nullable})
	}

	if s.ShowTablesOpt.Filter != nil {
		if s.ShowTablesOpt.Filter.Filter != nil {
			filter = b.buildScalar(outScope, s.ShowTablesOpt.Filter.Filter)
		} else if s.ShowTablesOpt.Filter.Like != "" {
			filter = expression.NewLike(
				expression.NewGetField(0, types.LongText, fmt.Sprintf("Tables_in_%s", dbName), false),
				expression.NewLiteral(s.ShowTablesOpt.Filter.Like, types.LongText),
				nil,
			)
		}
	}

	outScope.node = showTabs

	if filter != nil {
		outScope.node = plan.NewFilter(filter, outScope.node)
	}

	return
}

func (b *Builder) buildShowAllDatabases(inScope *scope, s *ast.Show) (outScope *scope) {
	showDbs := plan.NewShowDatabases()
	showDbs.Catalog = b.cat
	outScope = inScope.push()
	for _, c := range showDbs.Schema() {
		outScope.newColumn(scopeColumn{table: c.Source, col: c.Name, typ: c.Type, nullable: c.Nullable})
	}
	var filter sql.Expression
	if s.Filter != nil {
		if s.Filter.Filter != nil {
			filter = b.buildScalar(outScope, s.Filter.Filter)
		} else if s.Filter.Like != "" {
			filter = expression.NewLike(
				expression.NewGetField(0, types.LongText, "Database", false),
				expression.NewLiteral(s.Filter.Like, types.LongText),
				nil,
			)
		}
	}
	outScope.node = showDbs
	if filter != nil {
		outScope.node = plan.NewFilter(filter, outScope.node)
	}
	return
}

func (b *Builder) buildShowAllColumns(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	full := s.Full
	var table sql.Node

	var asOf *ast.AsOf
	if s.ShowTablesOpt != nil && s.ShowTablesOpt.AsOf != nil {
		asOf = &ast.AsOf{Time: s.ShowTablesOpt.AsOf}
	}

	db := s.Database
	if db == "" {
		db = s.Table.Qualifier.String()
	}
	if db == "" {
		db = b.currentDb().Name()
	}

	tableName := strings.ToLower(s.Table.Name.String())
	tableScope, ok := b.buildTablescan(inScope, db, tableName, asOf)
	if !ok {
		err := sql.ErrTableNotFound.New(tableName)
		b.handleErr(err)
	}
	table = tableScope.node

	show := plan.NewShowColumns(full, table)

	for _, c := range show.Schema() {
		outScope.newColumn(scopeColumn{
			table:    strings.ToLower(c.Source),
			col:      strings.ToLower(c.Name),
			typ:      c.Type,
			nullable: c.Nullable,
		})
	}

	var node sql.Node = show
	switch t := table.(type) {
	case *plan.ResolvedTable:
		show.Indexes = b.getInfoSchemaIndexes(t)
		node = b.modifySchemaTarget(tableScope, show, t)
	case *plan.SubqueryAlias:
		var err error
		node, err = show.WithTargetSchema(t.Schema())
		if err != nil {
			b.handleErr(err)
		}
	default:
	}

	if s.ShowTablesOpt != nil && s.ShowTablesOpt.Filter != nil {
		if s.ShowTablesOpt.Filter.Like != "" {
			pattern := expression.NewLiteral(s.ShowTablesOpt.Filter.Like, types.LongText)

			node = plan.NewFilter(
				expression.NewLike(
					expression.NewGetField(0, plan.VarChar25000, "Field", false),
					pattern,
					nil,
				),
				node,
			)
		}

		if s.ShowTablesOpt.Filter.Filter != nil {
			filter := b.buildScalar(outScope, s.ShowTablesOpt.Filter.Filter)
			node = plan.NewFilter(filter, node)
		}
	}

	outScope.node = node
	return
}

func (b *Builder) buildShowWarnings(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	if s.CountStar {
		unsupportedShow := "SHOW COUNT(*) WARNINGS"
		b.handleErr(sql.ErrUnsupportedFeature.New(unsupportedShow))
	}
	var node sql.Node
	node = plan.ShowWarnings(b.ctx.Session.Warnings())
	if s.Limit != nil {
		if s.Limit.Offset != nil {
			offset := b.buildScalar(inScope, s.Limit.Offset)
			node = plan.NewOffset(offset, node)
		}
		limit := b.buildScalar(inScope, s.Limit.Rowcount)
		node = plan.NewLimit(limit, node)
	}

	outScope.node = node
	return
}

func (b *Builder) buildShowCollation(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	// show collation statements are functionally identical to selecting from the collations table in
	// information_schema, with slightly different syntax and with some columns aliased.
	// TODO: install information_schema automatically for all catalogs
	node, _, _, err := b.Parse("select collation_name as `collation`, character_set_name as charset, id,"+
		"is_default as `default`, is_compiled as compiled, sortlen, pad_attribute from information_schema.collations", false)
	if err != nil {
		b.handleErr(err)
	}

	for _, c := range node.Schema() {
		outScope.newColumn(scopeColumn{
			table:    strings.ToLower(c.Source),
			col:      strings.ToLower(c.Name),
			typ:      c.Type,
			nullable: c.Nullable,
		})
	}

	if s.ShowCollationFilterOpt != nil {
		filterExpr := b.buildScalar(outScope, s.ShowCollationFilterOpt)
		// TODO: once collations are properly implemented, we should better be able to handle utf8 -> utf8mb3 comparisons as they're aliases
		filterExpr, _, _ = transform.Expr(filterExpr, func(expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			if exprLiteral, ok := expr.(*expression.Literal); ok {
				const utf8Prefix = "utf8_"
				if strLiteral, ok := exprLiteral.Value().(string); ok && strings.HasPrefix(strLiteral, utf8Prefix) {
					return expression.NewLiteral("utf8mb3_"+strLiteral[len(utf8Prefix):], exprLiteral.Type()), transform.NewTree, nil
				}
			}
			return expr, transform.SameTree, nil
		})
		node = plan.NewHaving(filterExpr, node)
	}

	outScope.node = node
	return
}

func (b *Builder) buildShowEngines(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	infoSchemaSelect, _, _, err := b.Parse("select * from information_schema.engines", false)
	if err != nil {
		b.handleErr(err)
	}

	outScope.node = infoSchemaSelect
	return
}

func (b *Builder) buildShowStatus(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()
	var node sql.Node
	if s.Scope == ast.GlobalStr {
		node = plan.NewShowStatus(plan.ShowStatusModifier_Global)
	} else {
		node = plan.NewShowStatus(plan.ShowStatusModifier_Session)
	}

	for _, c := range node.Schema() {
		outScope.newColumn(scopeColumn{
			table:    strings.ToLower(c.Source),
			col:      strings.ToLower(c.Name),
			typ:      c.Type,
			nullable: c.Nullable,
		})
	}

	var filter sql.Expression
	if s.Filter != nil {
		if s.Filter.Like != "" {
			filter = expression.NewLike(
				expression.NewGetField(0, node.Schema()[0].Type, plan.ShowStatusVariableCol, false),
				expression.NewLiteral(s.Filter.Like, types.LongText),
				nil,
			)
		} else if s.Filter.Filter != nil {
			filter = b.buildScalar(outScope, s.Filter.Filter)
		}
	}

	if filter != nil {
		node = plan.NewFilter(filter, node)
	}

	outScope.node = node

	return
}

func (b *Builder) buildShowCharset(inScope *scope, s *ast.Show) (outScope *scope) {
	outScope = inScope.push()

	showCharset := plan.NewShowCharset()
	showCharset.CharacterSetTable = b.resolveTable("character_sets", "information_schema", nil)

	var node sql.Node = showCharset
	for _, c := range node.Schema() {
		outScope.newColumn(scopeColumn{table: c.Source, col: c.Name, typ: c.Type, nullable: c.Nullable})
	}

	var filter sql.Expression
	if s.Filter != nil {
		if s.Filter.Filter != nil {
			filter = b.buildScalar(outScope, s.Filter.Filter)
		} else if s.Filter.Like != "" {
			filter = expression.NewLike(
				expression.NewGetField(0, types.MustCreateStringWithDefaults(sqltypes.VarChar, 64), "Charset", false),
				expression.NewLiteral(s.Filter.Like, types.LongText),
				nil,
			)
		}
	}

	if filter != nil {
		node = plan.NewFilter(filter, node)
	}
	outScope.node = node
	return
}
