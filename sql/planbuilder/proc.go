package planbuilder

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func (b *PlanBuilder) buildBeginEndBlock(inScope *scope, n *sqlparser.BeginEndBlock) (outScope *scope) {
	outScope = inScope.push()
	block := b.buildBlock(inScope, n.Statements)
	outScope.node = plan.NewBeginEndBlock(n.Label, block)
	return outScope
}

func (b *PlanBuilder) buildIfBlock(inScope *scope, n *sqlparser.IfStatement) (outScope *scope) {
	outScope = inScope.push()
	ifConditionals := make([]*plan.IfConditional, len(n.Conditions))
	for i, ic := range n.Conditions {
		ifConditionalScope := b.buildIfConditional(inScope, ic)
		ifConditionals[i] = ifConditionalScope.node.(*plan.IfConditional)
	}
	elseBlock := b.buildBlock(inScope, n.Else)
	outScope.node = plan.NewIfElse(ifConditionals, elseBlock)
	return outScope
}

func (b *PlanBuilder) buildCaseStatement(inScope *scope, n *sqlparser.CaseStatement) (outScope *scope) {
	outScope = inScope.push()
	ifConditionals := make([]*plan.IfConditional, len(n.Cases))
	for i, c := range n.Cases {
		ifConditionalScope := b.buildIfConditional(inScope, sqlparser.IfStatementCondition{
			Expr:       c.Case,
			Statements: c.Statements,
		})
		ifConditionals[i] = ifConditionalScope.node.(*plan.IfConditional)
	}
	var elseBlock sql.Node
	if n.Else != nil {
		elseBlock = b.buildBlock(inScope, n.Else)
	}
	if n.Expr == nil {
		outScope.node = plan.NewCaseStatement(nil, ifConditionals, elseBlock)
		return outScope
	} else {
		caseExpr := b.buildScalar(inScope, n.Expr)
		outScope.node = plan.NewCaseStatement(caseExpr, ifConditionals, elseBlock)
		return outScope
	}
}

func (b *PlanBuilder) buildIfConditional(inScope *scope, n sqlparser.IfStatementCondition) (outScope *scope) {
	outScope = inScope.push()
	block := b.buildBlock(inScope, n.Statements)
	condition := b.buildScalar(inScope, n.Expr)
	outScope.node = plan.NewIfConditional(condition, block)
	return outScope
}

func (b *PlanBuilder) buildCall(inScope *scope, c *sqlparser.Call) (outScope *scope) {
	outScope = inScope.push()
	params := make([]sql.Expression, len(c.Params))
	for i, param := range c.Params {
		expr := b.buildScalar(inScope, param)
		params[i] = expr
	}

	var db sql.Database = nil
	dbName := c.ProcName.Qualifier.String()
	if dbName != "" {
		db = b.resolveDb(dbName)
	} else {
		db = b.currentDb()
	}

	var asOf sql.Expression = nil
	if c.AsOf != nil {
		asOf = b.buildScalar(inScope, c.AsOf)
	}

	outScope.node = plan.NewCall(
		db,
		c.ProcName.Name.String(),
		params,
		asOf)
	return outScope
}

func (b *PlanBuilder) buildDeclare(inScope *scope, d *sqlparser.Declare, query string) (outScope *scope) {
	outScope = inScope.push()
	if d.Condition != nil {
		return b.buildDeclareCondition(inScope, d)
	} else if d.Variables != nil {
		return b.buildDeclareVariables(inScope, d)
	} else if d.Cursor != nil {
		return b.buildDeclareCursor(inScope, d)
	} else if d.Handler != nil {
		return b.buildDeclareHandler(inScope, d, query)
	}
	err := sql.ErrUnsupportedSyntax.New(sqlparser.String(d))
	b.handleErr(err)
	return
}

func (b *PlanBuilder) buildDeclareCondition(inScope *scope, d *sqlparser.Declare) (outScope *scope) {
	outScope = inScope.push()
	dc := d.Condition
	if dc.SqlStateValue != "" {
		if len(dc.SqlStateValue) != 5 {
			err := fmt.Errorf("SQLSTATE VALUE must be a string with length 5 consisting of only integers")
			b.handleErr(err)
		}
		if dc.SqlStateValue[0:2] == "00" {
			err := fmt.Errorf("invalid SQLSTATE VALUE: '%s'", dc.SqlStateValue)
			b.handleErr(err)
		}
	} else {
		number, err := strconv.ParseUint(string(dc.MysqlErrorCode.Val), 10, 64)
		if err != nil || number == 0 {
			// We use our own error instead
			err := fmt.Errorf("invalid value '%s' for MySQL error code", string(dc.MysqlErrorCode.Val))
			b.handleErr(err)
		}
		//TODO: implement MySQL error code support
		err = sql.ErrUnsupportedSyntax.New(sqlparser.String(d))
		b.handleErr(err)
	}
	outScope.node = plan.NewDeclareCondition(strings.ToLower(dc.Name), 0, dc.SqlStateValue)
	return outScope
}

func (b *PlanBuilder) buildDeclareVariables(inScope *scope, d *sqlparser.Declare) (outScope *scope) {
	outScope = inScope.push()
	dVars := d.Variables
	names := make([]string, len(dVars.Names))
	for i, variable := range dVars.Names {
		names[i] = variable.String()
	}
	typ, err := types.ColumnTypeToType(&dVars.VarType)
	if err != nil {
		err := err
		b.handleErr(err)
	}
	defaultVal := b.buildDefaultExpression(inScope, dVars.VarType.Default)
	outScope.node = plan.NewDeclareVariables(names, typ, defaultVal)
	return outScope
}

func (b *PlanBuilder) buildDeclareCursor(inScope *scope, d *sqlparser.Declare) (outScope *scope) {
	outScope = inScope.push()
	dCursor := d.Cursor
	selectScope := b.buildSelectStmt(inScope, dCursor.SelectStmt)
	outScope.node = plan.NewDeclareCursor(dCursor.Name, selectScope.node)
	return outScope
}

func (b *PlanBuilder) buildDeclareHandler(inScope *scope, d *sqlparser.Declare, query string) (outScope *scope) {
	outScope = inScope.push()
	dHandler := d.Handler
	//TODO: support other condition values besides NOT FOUND
	if len(dHandler.ConditionValues) != 1 || dHandler.ConditionValues[0].ValueType != sqlparser.DeclareHandlerCondition_NotFound {
		err := sql.ErrUnsupportedSyntax.New(sqlparser.String(d))
		b.handleErr(err)
	}
	stmtScope := b.build(inScope, dHandler.Statement, query)

	var action plan.DeclareHandlerAction
	switch dHandler.Action {
	case sqlparser.DeclareHandlerAction_Continue:
		action = plan.DeclareHandlerAction_Continue
	case sqlparser.DeclareHandlerAction_Exit:
		action = plan.DeclareHandlerAction_Exit
	case sqlparser.DeclareHandlerAction_Undo:
		action = plan.DeclareHandlerAction_Undo
	default:
		err := fmt.Errorf("unknown DECLARE ... HANDLER action: %v", dHandler.Action)
		b.handleErr(err)
	}
	if action == plan.DeclareHandlerAction_Undo {
		err := sql.ErrDeclareHandlerUndo.New()
		b.handleErr(err)
	}

	outScope.node = &plan.DeclareHandler{
		Action:    action,
		Statement: stmtScope.node,
	}
	return outScope
}

func (b *PlanBuilder) buildBlock(inScope *scope, parserStatements sqlparser.Statements) *plan.Block {
	var statements []sql.Node
	for _, s := range parserStatements {
		stmtScope := b.build(inScope, s, sqlparser.String(s))
		statements = append(statements, stmtScope.node)
	}
	return plan.NewBlock(statements)
}

func (b *PlanBuilder) buildFetchCursor(inScope *scope, fetchCursor *sqlparser.FetchCursor) (outScope *scope) {
	outScope = inScope.push()
	outScope.node = plan.NewFetch(fetchCursor.Name, fetchCursor.Variables)
	return outScope
}

func (b *PlanBuilder) buildOpenCursor(inScope *scope, openCursor *sqlparser.OpenCursor) (outScope *scope) {
	outScope = inScope.push()
	outScope.node = plan.NewOpen(openCursor.Name)
	return outScope
}

func (b *PlanBuilder) buildCloseCursor(inScope *scope, closeCursor *sqlparser.CloseCursor) (outScope *scope) {
	outScope = inScope.push()
	outScope.node = plan.NewClose(closeCursor.Name)
	return outScope
}

func (b *PlanBuilder) buildLoop(inScope *scope, loop *sqlparser.Loop) (outScope *scope) {
	outScope = inScope.push()
	block := b.buildBlock(inScope, loop.Statements)
	outScope.node = plan.NewLoop(loop.Label, block)
	return outScope
}

func (b *PlanBuilder) buildRepeat(inScope *scope, repeat *sqlparser.Repeat) (outScope *scope) {
	outScope = inScope.push()
	block := b.buildBlock(inScope, repeat.Statements)
	expr := b.buildScalar(inScope, repeat.Condition)
	outScope.node = plan.NewRepeat(repeat.Label, expr, block)
	return outScope
}

func (b *PlanBuilder) buildWhile(inScope *scope, while *sqlparser.While) (outScope *scope) {
	outScope = inScope.push()
	block := b.buildBlock(inScope, while.Statements)
	expr := b.buildScalar(inScope, while.Condition)
	outScope.node = plan.NewWhile(while.Label, expr, block)
	return outScope
}

func (b *PlanBuilder) buildLeave(inScope *scope, leave *sqlparser.Leave) (outScope *scope) {
	outScope = inScope.push()
	outScope.node = plan.NewLeave(leave.Label)
	return outScope
}

func (b *PlanBuilder) buildIterate(inScope *scope, iterate *sqlparser.Iterate) (outScope *scope) {
	outScope = inScope.push()
	outScope.node = plan.NewIterate(iterate.Label)
	return outScope
}
