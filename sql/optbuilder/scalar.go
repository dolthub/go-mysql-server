package optbuilder

import (
	"fmt"
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *PlanBuilder) buildComparison(inScope *scope, c *sqlparser.ComparisonExpr) sql.Expression {
	left := b.buildScalar(inScope, c.Left)

	right := b.buildScalar(inScope, c.Right)

	var escape sql.Expression = nil
	if c.Escape != nil {
		escape = b.buildScalar(inScope, c.Escape)
	}

	switch strings.ToLower(c.Operator) {
	case sqlparser.RegexpStr:
		return expression.NewRegexp(left, right)
	case sqlparser.NotRegexpStr:
		return expression.NewNot(expression.NewRegexp(left, right))
	case sqlparser.EqualStr:
		return expression.NewEquals(left, right)
	case sqlparser.LessThanStr:
		return expression.NewLessThan(left, right)
	case sqlparser.LessEqualStr:
		return expression.NewLessThanOrEqual(left, right)
	case sqlparser.GreaterThanStr:
		return expression.NewGreaterThan(left, right)
	case sqlparser.GreaterEqualStr:
		return expression.NewGreaterThanOrEqual(left, right)
	case sqlparser.NullSafeEqualStr:
		return expression.NewNullSafeEquals(left, right)
	case sqlparser.NotEqualStr:
		return expression.NewNot(
			expression.NewEquals(left, right),
		)
	case sqlparser.InStr:
		switch right.(type) {
		case expression.Tuple:
			return expression.NewInTuple(left, right)
		case *plan.Subquery:
			return plan.NewInSubquery(left, right)
		default:
			err := sql.ErrUnsupportedFeature.New(fmt.Sprintf("IN %T", right))
			b.handleErr(err)
		}
	case sqlparser.NotInStr:
		switch right.(type) {
		case expression.Tuple:
			return expression.NewNotInTuple(left, right)
		case *plan.Subquery:
			return plan.NewNotInSubquery(left, right)
		default:
			err := sql.ErrUnsupportedFeature.New(fmt.Sprintf("NOT IN %T", right))
			b.handleErr(err)
		}
	case sqlparser.LikeStr:
		return expression.NewLike(left, right, escape)
	case sqlparser.NotLikeStr:
		return expression.NewNot(expression.NewLike(left, right, escape))
	default:
		err := sql.ErrUnsupportedFeature.New(c.Operator)
		b.handleErr(err)
	}
	return nil
}

func (b *PlanBuilder) buildIsExprToExpression(inScope *scope, c *sqlparser.IsExpr) sql.Expression {
	e := b.buildScalar(inScope, c.Expr)
	switch strings.ToLower(c.Operator) {
	case sqlparser.IsNullStr:
		return expression.NewIsNull(e)
	case sqlparser.IsNotNullStr:
		return expression.NewNot(expression.NewIsNull(e))
	case sqlparser.IsTrueStr:
		return expression.NewIsTrue(e)
	case sqlparser.IsFalseStr:
		return expression.NewIsFalse(e)
	case sqlparser.IsNotTrueStr:
		return expression.NewNot(expression.NewIsTrue(e))
	case sqlparser.IsNotFalseStr:
		return expression.NewNot(expression.NewIsFalse(e))
	default:
		err := sql.ErrUnsupportedSyntax.New(sqlparser.String(c))
		b.handleErr(err)
	}
	return nil
}

func (b *PlanBuilder) binaryExprToExpression(inScope *scope, be *sqlparser.BinaryExpr) (sql.Expression, error) {
	switch strings.ToLower(be.Operator) {
	case
		sqlparser.PlusStr,
		sqlparser.MinusStr,
		sqlparser.MultStr,
		sqlparser.DivStr,
		sqlparser.ShiftLeftStr,
		sqlparser.ShiftRightStr,
		sqlparser.BitAndStr,
		sqlparser.BitOrStr,
		sqlparser.BitXorStr,
		sqlparser.IntDivStr,
		sqlparser.ModStr:

		l := b.buildScalar(inScope, be.Left)
		r := b.buildScalar(inScope, be.Right)

		_, lok := l.(*expression.Interval)
		_, rok := r.(*expression.Interval)
		if lok && be.Operator == "-" {
			return nil, sql.ErrUnsupportedSyntax.New("subtracting from an interval")
		} else if (lok || rok) && be.Operator != "+" && be.Operator != "-" {
			return nil, sql.ErrUnsupportedSyntax.New("only + and - can be used to add or subtract intervals from dates")
		} else if lok && rok {
			return nil, sql.ErrUnsupportedSyntax.New("intervals cannot be added or subtracted from other intervals")
		}

		switch strings.ToLower(be.Operator) {
		case sqlparser.DivStr:
			return expression.NewDiv(l, r), nil
		case sqlparser.ModStr:
			return expression.NewMod(l, r), nil
		case sqlparser.BitAndStr, sqlparser.BitOrStr, sqlparser.BitXorStr, sqlparser.ShiftRightStr, sqlparser.ShiftLeftStr:
			return expression.NewBitOp(l, r, be.Operator), nil
		case sqlparser.IntDivStr:
			return expression.NewIntDiv(l, r), nil
		default:
			return expression.NewArithmetic(l, r, be.Operator), nil
		}
	case
		sqlparser.JSONExtractOp,
		sqlparser.JSONUnquoteExtractOp:
		return nil, sql.ErrUnsupportedFeature.New(fmt.Sprintf("(%s) JSON operators not supported", be.Operator))

	default:
		return nil, sql.ErrUnsupportedFeature.New(be.Operator)
	}
}

func (b *PlanBuilder) caseExprToExpression(inScope *scope, e *sqlparser.CaseExpr) (sql.Expression, error) {
	var expr sql.Expression

	if e.Expr != nil {
		expr = b.buildScalar(inScope, e.Expr)
	}

	var branches []expression.CaseBranch
	for _, w := range e.Whens {
		var cond sql.Expression
		cond = b.buildScalar(inScope, w.Cond)

		var val sql.Expression
		val = b.buildScalar(inScope, w.Val)

		branches = append(branches, expression.CaseBranch{
			Cond:  cond,
			Value: val,
		})
	}

	var elseExpr sql.Expression
	if e.Else != nil {
		elseExpr = b.buildScalar(inScope, e.Else)
	}

	return expression.NewCase(expr, branches, elseExpr), nil
}

func (b *PlanBuilder) intervalExprToExpression(inScope *scope, e *sqlparser.IntervalExpr) (sql.Expression, error) {
	expr := b.buildScalar(inScope, e.Expr)

	return expression.NewInterval(expr, e.Unit), nil
}
