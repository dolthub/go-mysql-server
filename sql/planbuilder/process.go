package planbuilder

import (
	"fmt"
	"strconv"
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func (b *PlanBuilder) buildKill(inScope *scope, kill *ast.Kill) (outScope *scope) {
	connID64 := b.getInt64Value(inScope, kill.ConnID, "Error parsing KILL, expected int literal")
	connID32 := uint32(connID64)
	if int64(connID32) != connID64 {
		err := sql.ErrUnsupportedFeature.New("int literal is not unsigned 32-bit.")
		b.handleErr(err)
	}
	if kill.Connection {
		outScope.node = plan.NewKill(plan.KillType_Connection, connID32)
	}
	outScope.node = plan.NewKill(plan.KillType_Query, connID32)
	return outScope
}

// getInt64Value returns the int64 literal value in the expression given, or an error with the errStr given if it
// cannot.
func (b *PlanBuilder) getInt64Value(inScope *scope, expr ast.Expr, errStr string) int64 {
	ie := b.getInt64Literal(inScope, expr, errStr)

	i, err := ie.Eval(b.ctx, nil)
	if err != nil {
		b.handleErr(err)
	}

	return i.(int64)
}

// getInt64Literal returns an int64 *expression.Literal for the value given, or an unsupported error with the string
// given if the expression doesn't represent an integer literal.
func (b *PlanBuilder) getInt64Literal(inScope *scope, expr ast.Expr, errStr string) *expression.Literal {
	e := b.buildScalar(inScope, expr)

	switch e := e.(type) {
	case *expression.Literal:
		if !types.IsInteger(e.Type()) {
			err := sql.ErrUnsupportedFeature.New(errStr)
			b.handleErr(err)
		}
	}
	nl, ok := e.(*expression.Literal)
	if !ok || !types.IsInteger(nl.Type()) {
		err := sql.ErrUnsupportedFeature.New(errStr)
		b.handleErr(err)
	} else {
		i64, _, err := types.Int64.Convert(nl.Value())
		if err != nil {
			err := sql.ErrUnsupportedFeature.New(errStr)
			b.handleErr(err)
		}
		return expression.NewLiteral(i64, types.Int64)
	}

	return nl
}

func (b *PlanBuilder) buildSignal(inScope *scope, s *ast.Signal) (outScope *scope) {
	outScope = inScope.push()
	// https://dev.mysql.com/doc/refman/8.0/en/signal.html#signal-condition-information-items
	signalInfo := make(map[plan.SignalConditionItemName]plan.SignalInfo)
	for _, info := range s.Info {
		si := plan.SignalInfo{}
		si.ConditionItemName = b.buildSignalConditionItemName(info.ConditionItemName)
		if _, ok := signalInfo[si.ConditionItemName]; ok {
			err := fmt.Errorf("duplicate signal condition item")
			b.handleErr(err)
		}

		if si.ConditionItemName == plan.SignalConditionItemName_MysqlErrno {
			switch v := info.Value.(type) {
			case *ast.SQLVal:
				number, err := strconv.ParseUint(string(v.Val), 10, 16)
				if err != nil || number == 0 {
					// We use our own error instead
					err := fmt.Errorf("invalid value '%s' for signal condition information item MYSQL_ERRNO", string(v.Val))
					b.handleErr(err)
				}
				si.IntValue = int64(number)
			default:
				err := fmt.Errorf("invalid value '%v' for signal condition information item MYSQL_ERRNO", info.Value)
				b.handleErr(err)
			}
		} else if si.ConditionItemName == plan.SignalConditionItemName_MessageText {
			switch v := info.Value.(type) {
			case *ast.SQLVal:
				val := string(v.Val)
				if len(val) > 128 {
					err := fmt.Errorf("signal condition information item MESSAGE_TEXT has max length of 128")
					b.handleErr(err)
				}
				si.StrValue = val
			case *ast.ColName:
				si.ExprVal = expression.NewUnresolvedColumn(v.Name.String())
			default:
				err := fmt.Errorf("invalid value '%v' for signal condition information item MESSAGE_TEXT", info.Value)
				b.handleErr(err)
			}
		} else {
			switch v := info.Value.(type) {
			case *ast.SQLVal:
				val := string(v.Val)
				if len(val) > 64 {
					err := fmt.Errorf("signal condition information item %s has max length of 64", strings.ToUpper(string(si.ConditionItemName)))
					b.handleErr(err)
				}
				si.StrValue = val
			default:
				err := fmt.Errorf("invalid value '%v' for signal condition information item '%s''", info.Value, strings.ToUpper(string(si.ConditionItemName)))
				b.handleErr(err)
			}
		}
		signalInfo[si.ConditionItemName] = si
	}

	if s.ConditionName != "" {
		outScope.node = plan.NewSignalName(strings.ToLower(s.ConditionName), signalInfo)
	} else {
		if len(s.SqlStateValue) != 5 {
			err := fmt.Errorf("SQLSTATE VALUE must be a string with length 5 consisting of only integers")
			b.handleErr(err)
		}
		if s.SqlStateValue[0:2] == "00" {
			err := fmt.Errorf("invalid SQLSTATE VALUE: '%s'", s.SqlStateValue)
			b.handleErr(err)
		}
		outScope.node = plan.NewSignal(s.SqlStateValue, signalInfo)
	}
	return outScope
}

func (b *PlanBuilder) buildSignalConditionItemName(name ast.SignalConditionItemName) plan.SignalConditionItemName {
	// We convert to our own plan equivalents to keep a separation between the parser and implementation
	switch name {
	case ast.SignalConditionItemName_ClassOrigin:
		return plan.SignalConditionItemName_ClassOrigin
	case ast.SignalConditionItemName_SubclassOrigin:
		return plan.SignalConditionItemName_SubclassOrigin
	case ast.SignalConditionItemName_MessageText:
		return plan.SignalConditionItemName_MessageText
	case ast.SignalConditionItemName_MysqlErrno:
		return plan.SignalConditionItemName_MysqlErrno
	case ast.SignalConditionItemName_ConstraintCatalog:
		return plan.SignalConditionItemName_ConstraintCatalog
	case ast.SignalConditionItemName_ConstraintSchema:
		return plan.SignalConditionItemName_ConstraintSchema
	case ast.SignalConditionItemName_ConstraintName:
		return plan.SignalConditionItemName_ConstraintName
	case ast.SignalConditionItemName_CatalogName:
		return plan.SignalConditionItemName_CatalogName
	case ast.SignalConditionItemName_SchemaName:
		return plan.SignalConditionItemName_SchemaName
	case ast.SignalConditionItemName_TableName:
		return plan.SignalConditionItemName_TableName
	case ast.SignalConditionItemName_ColumnName:
		return plan.SignalConditionItemName_ColumnName
	case ast.SignalConditionItemName_CursorName:
		return plan.SignalConditionItemName_CursorName
	default:
		err := fmt.Errorf("unknown signal condition item name: %s", string(name))
		b.handleErr(err)
	}
	return plan.SignalConditionItemName_Unknown
}
