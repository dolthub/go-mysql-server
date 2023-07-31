package planbuilder

import (
	"fmt"
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func (b *Builder) buildSet(inScope *scope, n *ast.Set) (outScope *scope) {
	var setVarExprs []*ast.SetVarExpr
	for _, setExpr := range n.Exprs {
		switch strings.ToLower(setExpr.Name.String()) {
		case "names":
			// Special case: SET NAMES expands to 3 different system variables.
			setVarExprs = append(setVarExprs, getSetVarExprsFromSetNamesExpr(setExpr)...)
		case "charset":
			// Special case: SET CHARACTER SET (CHARSET) expands to 3 different system variables.
			csd, err := b.ctx.GetSessionVariable(b.ctx, "character_set_database")
			if err != nil {
				b.handleErr(err)
			}
			setVarExprs = append(setVarExprs, getSetVarExprsFromSetCharsetExpr(setExpr, []byte(csd.(string)))...)
		default:
			setVarExprs = append(setVarExprs, setExpr)
		}
	}

	exprs := b.setExprsToExpressions(inScope, setVarExprs)

	outScope = inScope.push()
	outScope.node = plan.NewSet(exprs)
	return outScope
}

func getSetVarExprsFromSetNamesExpr(expr *ast.SetVarExpr) []*ast.SetVarExpr {
	return []*ast.SetVarExpr{
		{
			Name: ast.NewColName("character_set_client"),
			Expr: expr.Expr,
		},
		{
			Name: ast.NewColName("character_set_connection"),
			Expr: expr.Expr,
		},
		{
			Name: ast.NewColName("character_set_results"),
			Expr: expr.Expr,
		},
		// TODO (9/24/20 Zach): this should also set the collation_connection to the default collation for the character set named
	}
}

func getSetVarExprsFromSetCharsetExpr(expr *ast.SetVarExpr, csd []byte) []*ast.SetVarExpr {
	return []*ast.SetVarExpr{
		{
			Name: ast.NewColName("character_set_client"),
			Expr: expr.Expr,
		},
		{
			Name: ast.NewColName("character_set_results"),
			Expr: expr.Expr,
		},
		{
			Name: ast.NewColName("character_set_connection"),
			Expr: &ast.SQLVal{Type: ast.StrVal, Val: csd},
		},
	}
}

func (b *Builder) setExprsToExpressions(inScope *scope, e ast.SetVarExprs) []sql.Expression {
	res := make([]sql.Expression, len(e))
	for i, setExpr := range e {
		if expr, ok := setExpr.Expr.(*ast.SQLVal); ok && strings.ToLower(setExpr.Name.String()) == "transaction" &&
			(setExpr.Scope == ast.SetScope_Global || setExpr.Scope == ast.SetScope_Session || string(setExpr.Scope) == "") {
			scope := sql.SystemVariableScope_Session
			if setExpr.Scope == ast.SetScope_Global {
				scope = sql.SystemVariableScope_Global
			}
			switch strings.ToLower(expr.String()) {
			case "'isolation level repeatable read'":
				varToSet := expression.NewSystemVar("transaction_isolation", scope)
				res[i] = expression.NewSetField(varToSet, expression.NewLiteral("REPEATABLE-READ", types.LongText))
				continue
			case "'isolation level read committed'":
				varToSet := expression.NewSystemVar("transaction_isolation", scope)
				res[i] = expression.NewSetField(varToSet, expression.NewLiteral("READ-COMMITTED", types.LongText))
				continue
			case "'isolation level read uncommitted'":
				varToSet := expression.NewSystemVar("transaction_isolation", scope)
				res[i] = expression.NewSetField(varToSet, expression.NewLiteral("READ-UNCOMMITTED", types.LongText))
				continue
			case "'isolation level serializable'":
				varToSet := expression.NewSystemVar("transaction_isolation", scope)
				res[i] = expression.NewSetField(varToSet, expression.NewLiteral("SERIALIZABLE", types.LongText))
				continue
			case "'read write'":
				varToSet := expression.NewSystemVar("transaction_read_only", scope)
				res[i] = expression.NewSetField(varToSet, expression.NewLiteral(false, types.Boolean))
				continue
			case "'read only'":
				varToSet := expression.NewSystemVar("transaction_read_only", scope)
				res[i] = expression.NewSetField(varToSet, expression.NewLiteral(true, types.Boolean))
				continue
			}
		}

		// left => convert to user var or system var expression, validate system var
		// right => getSetExpr, not adapted for defaults yet, special keywords need to be converted, variables replaced
		c, ok := inScope.resolveColumn(strings.ToLower(setExpr.Name.Qualifier.String()), strings.ToLower(setExpr.Name.Name.String()), true)
		var setVar sql.Expression
		if ok {
			setVar = c.scalarGf()
		} else {
			setVar, ok = b.buildSysVar(setExpr.Name, setExpr.Scope)
			if !ok {
				b.handleErr(sql.ErrColumnNotFound.New(setExpr.Name.String()))
			}
		}

		sysVarType, _ := setVar.Type().(sql.SystemVariableType)
		innerExpr, ok := b.simplifySetExpr(setExpr.Name, setExpr.Expr, sysVarType)
		if !ok {
			innerExpr = b.buildScalar(inScope, setExpr.Expr)
		}

		res[i] = expression.NewSetField(setVar, innerExpr)
	}
	return res
}

func (b *Builder) buildSysVar(colName *ast.ColName, scopeHint ast.SetScope) (sql.Expression, bool) {
	// convert to system or user var, validate system var
	table := colName.Qualifier.String()
	col := colName.Name.String()
	var varName string
	var scope ast.SetScope
	var err error
	if table != "" {
		varName, scope, err = ast.VarScope(table, col)
	} else {
		varName, scope, err = ast.VarScope(col)
	}
	if err != nil {
		b.handleErr(err)
	}

	if scope == "" {
		scope = scopeHint
	}

	switch scope {
	case ast.SetScope_Global:
		_, _, ok := sql.SystemVariables.GetGlobal(varName)
		if !ok {
			return nil, false
		}
		return expression.NewSystemVar(varName, sql.SystemVariableScope_Global), true
	case ast.SetScope_None, ast.SetScope_Session:
		switch strings.ToLower(varName) {
		case "character_set_database", "collation_database":
			sysVar := expression.NewSystemVar(varName, sql.SystemVariableScope_Session)
			sysVar.Collation = sql.Collation_Default
			if db, err := b.cat.Database(b.ctx, b.ctx.GetCurrentDatabase()); err == nil {
				sysVar.Collation = plan.GetDatabaseCollation(b.ctx, db)
			}
		default:
			_, err = b.ctx.GetSessionVariable(b.ctx, varName)
			if err != nil {
				return nil, false
			}
			return expression.NewSystemVar(varName, sql.SystemVariableScope_Session), true
		}
	case ast.SetScope_User:
		t, _, err := b.ctx.GetUserVariable(b.ctx, varName)
		if err != nil {
			b.handleErr(err)
		}
		return expression.NewUserVarWithType(varName, t), true
	case ast.SetScope_Persist:
		return expression.NewSystemVar(varName, sql.SystemVariableScope_Persist), true
	case ast.SetScope_PersistOnly:
		return expression.NewSystemVar(varName, sql.SystemVariableScope_PersistOnly), true
	default: // shouldn't happen
		err := fmt.Errorf("unknown set scope %v", scope)
		b.handleErr(err)
	}
	return nil, false
}

func (b *Builder) simplifySetExpr(name *ast.ColName, val ast.Expr, sysVarType sql.Type) (sql.Expression, bool) {
	// can |val| be nested?
	switch val := val.(type) {
	case *ast.ColName:
		// convert and eval
		// todo check whether right side needs variable replacement
		sysVar, ok := b.buildSysVar(val, ast.SetScope_None)
		if ok {
			return sysVar, true
		}
		e := expression.NewLiteral(val.Name.String(), types.Text)
		res, err := e.Eval(b.ctx, nil)
		if err != nil {
			b.handleErr(err)
		}
		setVal, ok := res.(string)
		if !ok {
			return nil, false
		}

		switch strings.ToLower(setVal) {
		case ast.KeywordString(ast.ON):
			return expression.NewLiteral(true, types.Boolean), true
		case ast.KeywordString(ast.TRUE):
			return expression.NewLiteral(true, types.Boolean), true
		case ast.KeywordString(ast.OFF):
			return expression.NewLiteral(false, types.Boolean), true
		case ast.KeywordString(ast.FALSE):
			return expression.NewLiteral(false, types.Boolean), true
		default:
		}

		if sysVarType == nil {
			return nil, false
		}

		enum, _, err := sysVarType.Convert(setVal)
		if err != nil {
			b.handleErr(err)
		}
		return expression.NewLiteral(enum, sysVarType), true
	case *ast.BoolVal:
		// conv
		e := expression.NewLiteral(val, types.Text)
		res, err := e.Eval(b.ctx, nil)
		if err != nil {
			b.handleErr(err)
		}
		setVal, ok := res.(bool)
		if !ok {
			err := fmt.Errorf("expected *ast.BoolVal to evaluate to bool type, found: %T", val)
			b.handleErr(err)
		}

		if setVal {
			return expression.NewLiteral(1, types.Boolean), true
		} else {
			return expression.NewLiteral(0, types.Boolean), true
		}
	case *ast.Default:
		// set back to default value
		var scope ast.SetScope
		var err error
		var varName string
		table := name.Qualifier.String()
		col := name.Name.Lowered()
		if table != "" {
			varName, scope, err = ast.VarScope(table, col)
		} else {
			varName, scope, err = ast.VarScope(col)
		}
		if err != nil {
			b.handleErr(err)
		}

		switch scope {
		case ast.SetScope_None, ast.SetScope_Session, ast.SetScope_Global:
			_, value, ok := sql.SystemVariables.GetGlobal(varName)
			if ok {
				return expression.NewLiteral(value, types.ApproximateTypeFromValue(value)), true
			}
			err = sql.ErrUnknownSystemVariable.New(varName)
		case ast.SetScope_Persist, ast.SetScope_PersistOnly:
			err = fmt.Errorf("%wsetting default for '%s'", sql.ErrUnsupportedFeature.New(), scope)
		case ast.SetScope_User:
			err = sql.ErrUserVariableNoDefault.New(varName)
		default: // shouldn't happen
			err = fmt.Errorf("unknown set scope %v", scope)
		}
		b.handleErr(err)
	}
	return nil, false
}
