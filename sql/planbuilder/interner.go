package planbuilder

/*
import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type exprOp uint16

const (
	exprOpUnknown exprOp = iota
	exprOpcustomFunc
	exprOpAliasReference
	exprOpAlias
	exprOpUnaryMinus
	exprOpAutoIncrement
	exprOpBetween
	exprOpBinary
	exprOpBindVar
	exprOpBitOp
	exprOpNot
	exprOpCase
	exprOpCollatedExpression
	exprOpEquals
	exprOpNullSafeEquals
	exprOpRegexp
	exprOpGreaterThan
	exprOpLessThan
	exprOpGreaterThanOrEqual
	exprOpLessThanOrEqual
	exprOpConvert
	exprOpDefaultColumn
	exprOpDistinctExpression
	exprOpRand
	exprOpTime
	exprOpGetField
	exprOpInterval
	exprOpIsNull
	exprOpIsTrue
	exprOpLike
	exprOpAnd
	exprOpOr
	exprOpXor
	exprOpProcedureParam
	exprOpUnresolvedProcedureParam
	exprOpSetField
	exprOpStar
	exprOpUnresolvedColumn
	exprOpUnresolvedFunction
	exprOpSystemVar
	exprOpUserVar
	exprOpWrapper
	exprOpExistsSubquery
	exprOpInSubquery
	exprOpLiteral
	exprOpNamedLiteral
)

type interner struct{}

// does interner need state?
func (b *PlanBuilder) internExpr(e sql.Expression) uint64 {
	return 0
}

func (b *PlanBuilder) internBinary(e expression.BinaryExpression) uint64 {
	h := xxhash.New()
	var err error
	switch l := e.Left.(type) {
	case *expression.Literal:
		_, err = h.WriteString(fmt.Sprintf("%v", l.Value()))

	default:
		b.internExpr(l)
		_, err = h.WriteString(fmt.Sprintf("%v", b.internExpr(l)))
	}
	if err != nil {
		b.handleErr(err)
	}
	switch r := e.Right.(type) {
	case *expression.Literal:
		_, err = h.WriteString(fmt.Sprintf("%v", r.Value()))

	default:
		b.internExpr(r)
		_, err = h.WriteString(fmt.Sprintf("%v", b.internExpr(r)))
	}
	if err != nil {
		b.handleErr(err)
	}
	return h.Sum64()
}

func (b *PlanBuilder) internLiteral(e *expression.Literal) uint64 {
	h := xxhash.New()
	_, err := h.WriteString(fmt.Sprintf("%v", e.Value()))
	if err != nil {
		b.handleErr(err)
	}
	return h.Sum64()
}

*/

/*
"customFunc":               "*expression.customFunc",
"AliasReference":           "*expression.AliasReference",
"Alias":                    "*expression.Alias",
"UnaryMinus":               "*expression.UnaryMinus",
"AutoIncrement":            "*expression.AutoIncrement",
"Between":                  "*expression.Between",
"Binary":                   "*expression.Binary",
"BindVar":                  "*expression.BindVar",
"BitOp":                    "*expression.BitOp",
"Not":                      "*expression.Not",
"Case":                     "*expression.Case",
"CollatedExpression":       "*expression.CollatedExpression",
"Equals":                   "*expression.Equals",
"NullSafeEquals":           "*expression.NullSafeEquals",
"Regexp":                   "*expression.Regexp",
"GreaterThan":              "*expression.GreaterThan",
"LessThan":                 "*expression.LessThan",
"GreaterThanOrEqual":       "*expression.GreaterThanOrEqual",
"LessThanOrEqual":          "*expression.LessThanOrEqual",
"Convert":                  "*expression.Convert",
"DefaultColumn":            "*expression.DefaultColumn",
"DistinctExpression":       "*expression.DistinctExpression",
"Rand":                     "*expression.Rand",
"Time":                     "*expression.Time",
"GetField":                 "*expression.GetField",
"Interval":                 "*expression.Interval",
"IsNull":                   "*expression.IsNull",
"IsTrue":                   "*expression.IsTrue",
"Like":                     "*expression.Like",
"Literal":                  "expression.Literal",
"And":                      "*expression.And",
"Or":                       "*expression.Or",
"Xor":                      "*expression.Xor",
"NamedLiteral":             "expression.NamedLiteral",
"ProcedureParam":           "*expression.ProcedureParam",
"UnresolvedProcedureParam": "*expression.UnresolvedProcedureParam",
"SetField":                 "*expression.SetField",
"Star":                     "*expression.Star",
"UnresolvedColumn":         "*expression.UnresolvedColumn",
"UnresolvedFunction":       "*expression.UnresolvedFunction",
"SystemVar":                "*expression.SystemVar",
"UserVar":                  "*expression.UserVar",
"Wrapper":                  "*expression.Wrapper",
"colDefaultExpression":     "colDefaultExpression",
"ExistsSubquery":           "*expression.ExistsSubquery",
"InSubquery":               "*expression.InSubquery",
)
*/
