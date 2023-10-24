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
	"strings"
	"time"
	"unicode"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func (b *Builder) buildCreateTrigger(inScope *scope, query string, c *ast.DDL) (outScope *scope) {
	outScope = inScope.push()
	var triggerOrder *plan.TriggerOrder
	if c.TriggerSpec.Order != nil {
		triggerOrder = &plan.TriggerOrder{
			PrecedesOrFollows: c.TriggerSpec.Order.PrecedesOrFollows,
			OtherTriggerName:  c.TriggerSpec.Order.OtherTriggerName,
		}
	} else {
		//TODO: fix vitess->sql.y, in CREATE TRIGGER, if trigger_order_opt evaluates to empty then SubStatementPositionStart swallows the first token of the body
		beforeSwallowedToken := strings.LastIndexFunc(strings.TrimRightFunc(query[:c.SubStatementPositionStart], unicode.IsSpace), unicode.IsSpace)
		if beforeSwallowedToken != -1 {
			c.SubStatementPositionStart = beforeSwallowedToken
		}
	}

	// resolve table -> create initial scope
	dbName := c.Table.Qualifier.String()
	if dbName == "" {
		dbName = b.ctx.GetCurrentDatabase()
	}

	prevTriggerCtxActive := b.TriggerCtx().Active
	b.TriggerCtx().Active = true
	defer func() {
		b.TriggerCtx().Active = prevTriggerCtxActive
	}()

	tableName := strings.ToLower(c.Table.Name.String())
	tableScope, ok := b.buildResolvedTable(inScope, dbName, tableName, nil)
	if !ok {
		b.handleErr(sql.ErrTableNotFound.New(tableName))
	}
	if _, ok := tableScope.node.(*plan.UnresolvedTable); ok {
		// unknown table in trigger body is OK, but the target table must exist
		b.handleErr(sql.ErrTableNotFound.New(tableName))
	}

	// todo scope with new and old columns provided
	// insert/update have "new"
	// update/delete have "old"
	newScope := tableScope.replace()
	oldScope := tableScope.replace()
	for _, col := range tableScope.cols {
		switch c.TriggerSpec.Event {
		case ast.InsertStr:
			newScope.newColumn(col)
		case ast.UpdateStr:
			newScope.newColumn(col)
			oldScope.newColumn(col)
		case ast.DeleteStr:
			oldScope.newColumn(col)
		}
	}
	newScope.setTableAlias("new")
	oldScope.setTableAlias("old")
	triggerScope := tableScope.replace()

	triggerScope.addColumns(newScope.cols)
	triggerScope.addColumns(oldScope.cols)

	bodyStr := strings.TrimSpace(query[c.SubStatementPositionStart:c.SubStatementPositionEnd])
	bodyScope := b.build(triggerScope, c.TriggerSpec.Body, bodyStr)
	definer := getCurrentUserForDefiner(b.ctx, c.TriggerSpec.Definer)
	db := b.resolveDb(dbName)

	if _, ok := tableScope.node.(*plan.ResolvedTable); !ok {
		if prevTriggerCtxActive {
			// previous ctx set means this is an INSERT or SHOW
			// old version of Dolt permitted a bad trigger on VIEW
			// warn and noop
			b.ctx.Warn(0, fmt.Sprintf("trigger on view is not supported; 'DROP TRIGGER  %s' to fix", c.TriggerSpec.TrigName.Name.String()))
			bodyScope.node = plan.NewResolvedDualTable()
		} else {
			// top-level call is DDL
			err := sql.ErrExpectedTableFoundView.New(tableName)
			b.handleErr(err)
		}
	}

	outScope.node = plan.NewCreateTrigger(
		db,
		c.TriggerSpec.TrigName.Name.String(),
		c.TriggerSpec.Time,
		c.TriggerSpec.Event,
		triggerOrder,
		tableScope.node,
		bodyScope.node,
		query,
		bodyStr,
		b.ctx.QueryTime(),
		definer,
	)
	return outScope
}

func getCurrentUserForDefiner(ctx *sql.Context, definer string) string {
	if definer == "" {
		client := ctx.Session.Client()
		definer = fmt.Sprintf("`%s`@`%s`", client.User, client.Address)
	}
	return definer
}

func (b *Builder) buildCreateProcedure(inScope *scope, query string, c *ast.DDL) (outScope *scope) {
	var params []plan.ProcedureParam
	for _, param := range c.ProcedureSpec.Params {
		var direction plan.ProcedureParamDirection
		switch param.Direction {
		case ast.ProcedureParamDirection_In:
			direction = plan.ProcedureParamDirection_In
		case ast.ProcedureParamDirection_Inout:
			direction = plan.ProcedureParamDirection_Inout
		case ast.ProcedureParamDirection_Out:
			direction = plan.ProcedureParamDirection_Out
		default:
			err := fmt.Errorf("unknown procedure parameter direction: `%s`", string(param.Direction))
			b.handleErr(err)
		}
		internalTyp, err := types.ColumnTypeToType(&param.Type)
		if err != nil {
			b.handleErr(err)
		}
		params = append(params, plan.ProcedureParam{
			Direction: direction,
			Name:      param.Name,
			Type:      internalTyp,
			Variadic:  false,
		})
	}

	var characteristics []plan.Characteristic
	securityType := plan.ProcedureSecurityContext_Definer // Default Security Context
	comment := ""
	for _, characteristic := range c.ProcedureSpec.Characteristics {
		switch characteristic.Type {
		case ast.CharacteristicValue_Comment:
			comment = characteristic.Comment
		case ast.CharacteristicValue_LanguageSql:
			characteristics = append(characteristics, plan.Characteristic_LanguageSql)
		case ast.CharacteristicValue_Deterministic:
			characteristics = append(characteristics, plan.Characteristic_Deterministic)
		case ast.CharacteristicValue_NotDeterministic:
			characteristics = append(characteristics, plan.Characteristic_NotDeterministic)
		case ast.CharacteristicValue_ContainsSql:
			characteristics = append(characteristics, plan.Characteristic_ContainsSql)
		case ast.CharacteristicValue_NoSql:
			characteristics = append(characteristics, plan.Characteristic_NoSql)
		case ast.CharacteristicValue_ReadsSqlData:
			characteristics = append(characteristics, plan.Characteristic_ReadsSqlData)
		case ast.CharacteristicValue_ModifiesSqlData:
			characteristics = append(characteristics, plan.Characteristic_ModifiesSqlData)
		case ast.CharacteristicValue_SqlSecurityDefiner:
			// This is already the default value, so this prevents the default switch case
		case ast.CharacteristicValue_SqlSecurityInvoker:
			securityType = plan.ProcedureSecurityContext_Invoker
		default:
			err := fmt.Errorf("unknown procedure characteristic: `%s`", string(characteristic.Type))
			b.handleErr(err)
		}
	}

	inScope.initProc()
	procName := strings.ToLower(c.ProcedureSpec.ProcName.Name.String())
	for _, p := range params {
		// populate inScope with the procedure parameters. this will be
		// subject maybe a bug where an inner procedure has access to
		// outer procedure parameters.
		inScope.proc.AddVar(expression.NewProcedureParam(strings.ToLower(p.Name)))
	}
	bodyStr := strings.TrimSpace(query[c.SubStatementPositionStart:c.SubStatementPositionEnd])

	bodyScope := b.build(inScope, c.ProcedureSpec.Body, bodyStr)

	var db sql.Database = nil
	dbName := c.ProcedureSpec.ProcName.Qualifier.String()
	if dbName != "" {
		db = b.resolveDb(dbName)
	} else {
		db = b.currentDb()
	}

	outScope = inScope.push()
	outScope.node = plan.NewCreateProcedure(
		db,
		procName,
		c.ProcedureSpec.Definer,
		params,
		time.Now(),
		time.Now(),
		securityType,
		characteristics,
		bodyScope.node,
		comment,
		query,
		bodyStr,
	)
	return outScope
}

func (b *Builder) buildCreateEvent(inScope *scope, query string, c *ast.DDL) (outScope *scope) {
	outScope = inScope.push()
	eventSpec := c.EventSpec
	dbName := strings.ToLower(eventSpec.EventName.Qualifier.String())
	if dbName == "" {
		dbName = b.ctx.GetCurrentDatabase()
	}
	database := b.resolveDb(dbName)
	definer := getCurrentUserForDefiner(b.ctx, c.EventSpec.Definer)

	// both 'undefined' and 'not preserve' are considered 'not preserve'
	onCompletionPreserve := false
	if eventSpec.OnCompletionPreserve == ast.EventOnCompletion_Preserve {
		onCompletionPreserve = true
	}

	var status sql.EventStatus
	switch eventSpec.Status {
	case ast.EventStatus_Undefined:
		status = sql.EventStatus_Enable
	case ast.EventStatus_Enable:
		status = sql.EventStatus_Enable
	case ast.EventStatus_Disable:
		status = sql.EventStatus_Disable
	case ast.EventStatus_DisableOnSlave:
		status = sql.EventStatus_DisableOnSlave
	}

	bodyStr := strings.TrimSpace(query[c.SubStatementPositionStart:c.SubStatementPositionEnd])
	bodyScope := b.build(inScope, c.EventSpec.Body, bodyStr)

	var at, starts, ends *plan.OnScheduleTimestamp
	var everyInterval *expression.Interval
	if eventSpec.OnSchedule.At != nil {
		ts, intervals := b.buildEventScheduleTimeSpec(inScope, eventSpec.OnSchedule.At)
		at = plan.NewOnScheduleTimestamp("AT", ts, intervals)
	} else {
		everyInterval = b.intervalExprToExpression(inScope, &eventSpec.OnSchedule.EveryInterval)
		if eventSpec.OnSchedule.Starts != nil {
			startsTs, startsIntervals := b.buildEventScheduleTimeSpec(inScope, eventSpec.OnSchedule.Starts)
			starts = plan.NewOnScheduleTimestamp("STARTS", startsTs, startsIntervals)
		}
		if eventSpec.OnSchedule.Ends != nil {
			endsTs, endsIntervals := b.buildEventScheduleTimeSpec(inScope, eventSpec.OnSchedule.Ends)
			ends = plan.NewOnScheduleTimestamp("ENDS", endsTs, endsIntervals)
		}
	}

	comment := ""
	if eventSpec.Comment != nil {
		comment = string(eventSpec.Comment.Val)
	}

	outScope.node = plan.NewCreateEvent(
		database,
		eventSpec.EventName.Name.String(), definer,
		at, starts, ends, everyInterval,
		onCompletionPreserve,
		status, comment, bodyStr, bodyScope.node, eventSpec.IfNotExists,
	)
	return outScope
}

func (b *Builder) buildEventScheduleTimeSpec(inScope *scope, spec *ast.EventScheduleTimeSpec) (sql.Expression, []sql.Expression) {
	ts := b.buildScalar(inScope, spec.EventTimestamp)
	if len(spec.EventIntervals) == 0 {
		return ts, nil
	}
	var intervals = make([]sql.Expression, len(spec.EventIntervals))
	for i, interval := range spec.EventIntervals {
		e := b.intervalExprToExpression(inScope, &interval)
		intervals[i] = e
	}
	return ts, intervals
}

func (b *Builder) buildAlterEvent(inScope *scope, query string, c *ast.DDL) (outScope *scope) {
	eventSpec := c.EventSpec

	var database sql.Database
	if dbName := eventSpec.EventName.Qualifier.String(); dbName != "" {
		database = b.resolveDb(dbName)
	} else {
		database = b.currentDb()
	}

	definer := getCurrentUserForDefiner(b.ctx, c.EventSpec.Definer)

	var (
		alterSchedule    = eventSpec.OnSchedule != nil
		at, starts, ends *plan.OnScheduleTimestamp
		everyInterval    *expression.Interval

		alterOnComp       = eventSpec.OnCompletionPreserve != ast.EventOnCompletion_Undefined
		newOnCompPreserve = eventSpec.OnCompletionPreserve == ast.EventOnCompletion_Preserve

		alterEventName = !eventSpec.RenameName.IsEmpty()
		newName        string

		alterStatus = eventSpec.Status != ast.EventStatus_Undefined
		newStatus   sql.EventStatus

		alterComment = eventSpec.Comment != nil
		newComment   string

		alterDefinition  = eventSpec.Body != nil
		newDefinitionStr string
		newDefinition    sql.Node
	)

	if alterSchedule {
		if eventSpec.OnSchedule.At != nil {
			ts, intervals := b.buildEventScheduleTimeSpec(inScope, eventSpec.OnSchedule.At)
			at = plan.NewOnScheduleTimestamp("AT", ts, intervals)
		} else {
			everyInterval = b.intervalExprToExpression(inScope, &eventSpec.OnSchedule.EveryInterval)
			if eventSpec.OnSchedule.Starts != nil {
				startsTs, startsIntervals := b.buildEventScheduleTimeSpec(inScope, eventSpec.OnSchedule.Starts)
				starts = plan.NewOnScheduleTimestamp("STARTS", startsTs, startsIntervals)
			}
			if eventSpec.OnSchedule.Ends != nil {
				endsTs, endsIntervals := b.buildEventScheduleTimeSpec(inScope, eventSpec.OnSchedule.Ends)
				ends = plan.NewOnScheduleTimestamp("ENDS", endsTs, endsIntervals)
			}
		}
	}
	if alterEventName {
		// events can be moved to different database using RENAME TO clause option
		// TODO: we do not support moving events to different database yet
		renameEventDb := eventSpec.RenameName.Qualifier.String()
		if renameEventDb != "" && database.Name() != renameEventDb {
			err := fmt.Errorf("moving events to different database using ALTER EVENT is not supported yet")
			b.handleErr(err)
		}
		newName = eventSpec.RenameName.Name.String()
	}
	if alterStatus {
		switch eventSpec.Status {
		case ast.EventStatus_Undefined:
			// this should not happen but sanity check
			newStatus = sql.EventStatus_Enable
		case ast.EventStatus_Enable:
			newStatus = sql.EventStatus_Enable
		case ast.EventStatus_Disable:
			newStatus = sql.EventStatus_Disable
		case ast.EventStatus_DisableOnSlave:
			newStatus = sql.EventStatus_DisableOnSlave
		}
	}
	if alterComment {
		newComment = string(eventSpec.Comment.Val)
	}
	if alterDefinition {
		newDefinitionStr = strings.TrimSpace(query[c.SubStatementPositionStart:c.SubStatementPositionEnd])
		defScope := b.build(inScope, c.EventSpec.Body, newDefinitionStr)
		newDefinition = defScope.node
	}

	eventName := strings.ToLower(eventSpec.EventName.Name.String())
	eventDb, ok := database.(sql.EventDatabase)
	if !ok {
		err := sql.ErrEventsNotSupported.New(database.Name())
		b.handleErr(err)
	}

	event, exists, err := eventDb.GetEvent(b.ctx, eventName)
	if err != nil {
		b.handleErr(err)
	}
	if !exists {
		err := sql.ErrEventDoesNotExist.New(eventName)
		b.handleErr(err)
	}

	outScope = inScope.push()
	alterEvent := plan.NewAlterEvent(
		database, eventName, definer,
		alterSchedule, at, starts, ends, everyInterval,
		alterOnComp, newOnCompPreserve,
		alterEventName, newName,
		alterStatus, newStatus,
		alterComment, newComment,
		alterDefinition, newDefinitionStr, newDefinition,
	)
	alterEvent.Event = event
	outScope.node = alterEvent
	return
}

func (b *Builder) buildCreateView(inScope *scope, query string, c *ast.DDL) (outScope *scope) {
	outScope = inScope.push()
	selectStatement, ok := c.ViewSpec.ViewExpr.(ast.SelectStatement)
	if !ok {
		err := sql.ErrUnsupportedSyntax.New(ast.String(c.ViewSpec.ViewExpr))
		b.handleErr(err)
	}

	queryScope := b.buildSelectStmt(inScope, selectStatement)

	selectStr := query[c.SubStatementPositionStart:c.SubStatementPositionEnd]
	queryAlias := plan.NewSubqueryAlias(c.ViewSpec.ViewName.Name.String(), selectStr, queryScope.node)
	definer := getCurrentUserForDefiner(b.ctx, c.ViewSpec.Definer)

	if len(c.ViewSpec.Columns) > 0 {
		if len(c.ViewSpec.Columns) != len(queryScope.cols) {
			err := sql.ErrInvalidColumnNumber.New(len(queryScope.cols), len(c.ViewSpec.Columns))
			b.handleErr(err)
		}
		queryAlias = queryAlias.WithColumns(columnsToStrings(c.ViewSpec.Columns))
	}

	dbName := c.Table.Qualifier.String()
	if dbName == "" {
		dbName = b.ctx.GetCurrentDatabase()
	}
	db := b.resolveDb(dbName)
	outScope.node = plan.NewCreateView(db, c.ViewSpec.ViewName.Name.String(), queryAlias, c.OrReplace, query, c.ViewSpec.Algorithm, definer, c.ViewSpec.Security)
	return outScope
}
