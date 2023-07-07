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

func (b *PlanBuilder) buildCreateTrigger(inScope *scope, query string, c *ast.DDL) (outScope *scope) {
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

	tableScope := b.buildTablescan(inScope, dbName, c.Table.Name.String(), nil)

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

func (b *PlanBuilder) buildCreateProcedure(inScope *scope, query string, c *ast.DDL) (outScope *scope) {
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

	bodyStr := strings.TrimSpace(query[c.SubStatementPositionStart:c.SubStatementPositionEnd])
	bodyScope := b.build(inScope, c.ProcedureSpec.Body, bodyStr)
	outScope.node = plan.NewCreateProcedure(
		sql.UnresolvedDatabase(c.ProcedureSpec.ProcName.Qualifier.String()),
		c.ProcedureSpec.ProcName.Name.String(),
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

func (b *PlanBuilder) buildCreateEvent(inScope *scope, query string, c *ast.DDL) (outScope *scope) {
	eventSpec := c.EventSpec
	database := b.resolveDb(eventSpec.EventName.Qualifier.String())
	definer := getCurrentUserForDefiner(b.ctx, c.EventSpec.Definer)

	// both 'undefined' and 'not preserve' are considered 'not preserve'
	onCompletionPreserve := false
	if eventSpec.OnCompletionPreserve == ast.EventOnCompletion_Preserve {
		onCompletionPreserve = true
	}

	var status plan.EventStatus
	switch eventSpec.Status {
	case ast.EventStatus_Undefined:
		status = plan.EventStatus_Enable
	case ast.EventStatus_Enable:
		status = plan.EventStatus_Enable
	case ast.EventStatus_Disable:
		status = plan.EventStatus_Disable
	case ast.EventStatus_DisableOnSlave:
		status = plan.EventStatus_DisableOnSlave
	}

	bodyStr := strings.TrimSpace(query[c.SubStatementPositionStart:c.SubStatementPositionEnd])
	bodyScope := b.build(inScope, c.EventSpec.Body, bodyStr)

	var at, starts, ends *plan.OnScheduleTimestamp
	var everyInterval *expression.Interval
	if eventSpec.OnSchedule.At != nil {
		ts, intervals := b.buildEventScheduleTimeSpec(inScope, eventSpec.OnSchedule.At)
		at = plan.NewOnScheduleTimestamp(ts, intervals)
	} else {
		every := b.intervalExprToExpression(inScope, &eventSpec.OnSchedule.EveryInterval)
		var ok bool
		everyInterval, ok = every.(*expression.Interval)
		if !ok {
			err := fmt.Errorf("expected everyInterval but got: %s", every)
			b.handleErr(err)
		}

		if eventSpec.OnSchedule.Starts != nil {
			startsTs, startsIntervals := b.buildEventScheduleTimeSpec(inScope, eventSpec.OnSchedule.Starts)
			starts = plan.NewOnScheduleTimestamp(startsTs, startsIntervals)
		}
		if eventSpec.OnSchedule.Ends != nil {
			endsTs, endsIntervals := b.buildEventScheduleTimeSpec(inScope, eventSpec.OnSchedule.Ends)
			ends = plan.NewOnScheduleTimestamp(endsTs, endsIntervals)
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

func (b *PlanBuilder) buildEventScheduleTimeSpec(inScope *scope, spec *ast.EventScheduleTimeSpec) (sql.Expression, []sql.Expression) {
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

func (b *PlanBuilder) buildCreateView(inScope *scope, query string, c *ast.DDL) (outScope *scope) {
	selectStatement, ok := c.ViewSpec.ViewExpr.(ast.SelectStatement)
	if !ok {
		err := sql.ErrUnsupportedSyntax.New(ast.String(c.ViewSpec.ViewExpr))
		b.handleErr(err)
	}

	queryScope := b.buildSelectStmt(inScope, selectStatement)

	selectStr := query[c.SubStatementPositionStart:c.SubStatementPositionEnd]
	queryAlias := plan.NewSubqueryAlias(c.ViewSpec.ViewName.Name.String(), selectStr, queryScope.node)
	definer := getCurrentUserForDefiner(b.ctx, c.ViewSpec.Definer)

	outScope.node = plan.NewCreateView(
		sql.UnresolvedDatabase(""),
		c.ViewSpec.ViewName.Name.String(),
		[]string{},
		queryAlias,
		c.OrReplace,
		query,
		c.ViewSpec.Algorithm,
		definer,
		c.ViewSpec.Security,
	)
	return outScope
}
