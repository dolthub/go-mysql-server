package optbuilder

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *PlanBuilder) buildSet(inScope *scope, n *ast.Set) (outScope *scope) {
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

func (b *PlanBuilder) setExprsToExpressions(inScope *scope, e ast.SetVarExprs) []sql.Expression {
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

		innerExpr := b.buildScalar(inScope, setExpr.Expr)
		switch setExpr.Scope {
		case ast.SetScope_None:
			colName := b.buildScalar(inScope, setExpr.Name)
			res[i] = expression.NewSetField(colName, innerExpr)
		case ast.SetScope_Global:
			varToSet := expression.NewSystemVar(setExpr.Name.String(), sql.SystemVariableScope_Global)
			res[i] = expression.NewSetField(varToSet, innerExpr)
		case ast.SetScope_Persist:
			varToSet := expression.NewSystemVar(setExpr.Name.String(), sql.SystemVariableScope_Persist)
			res[i] = expression.NewSetField(varToSet, innerExpr)
		case ast.SetScope_PersistOnly:
			varToSet := expression.NewSystemVar(setExpr.Name.String(), sql.SystemVariableScope_PersistOnly)
			res[i] = expression.NewSetField(varToSet, innerExpr)
			// TODO reset persist
		//case ast.SetScope_ResetPersist:
		//	varToSet := expression.NewSystemVar(setExpr.Name.String(), sql.SystemVariableScope_ResetPersist)
		//	res[i] = expression.NewSetField(varToSet, innerExpr)
		case ast.SetScope_Session:
			varToSet := expression.NewSystemVar(setExpr.Name.String(), sql.SystemVariableScope_Session)
			res[i] = expression.NewSetField(varToSet, innerExpr)
		case ast.SetScope_User:
			varToSet := expression.NewUserVar(setExpr.Name.String())
			res[i] = expression.NewSetField(varToSet, innerExpr)
		default: // shouldn't happen
			err := fmt.Errorf("unknown set scope %v", setExpr.Scope)
			b.handleErr(err)
		}
	}
	return res
}
