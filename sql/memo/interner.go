package memo

import (
	"fmt"
	"github.com/cespare/xxhash"
)

type ScalarExprId uint8

const (
	ScalarExprUnknown ScalarExprId = iota
	ScalarExprEqual
	ScalarExprNot
	ScalarExprColRef
	ScalarExprLiteral
	ScalarExprOr
	ScalarExprAnd
	ScalarExprInTuple
	ScalarExprLt
	ScalarExprLeq
	ScalarExprGt
	ScalarExprGeq
	ScalarExprNullSafeEq
	ScalarExprRegexp
	ScalarExprArithmetic
	ScalarExprBindvar
	ScalarExprIsNull
	ScalarExprTuple
	ScalarExprHidden
)

// TODO code gen
func internExpr(e ScalarExpr) uint64 {
	h := xxhash.New()
	switch e := e.(type) {
	case *Literal:
		h.Write([]byte(fmt.Sprintf("%d%v", e.ExprId(), e.Val)))
	case *ColRef:
		h.Write([]byte(fmt.Sprintf("%d%d", e.ExprId(), e.Col)))
	case *Equal:
		h.Write([]byte(fmt.Sprintf("%d%d%d", e.ExprId(), internExpr(e.Left.Scalar), internExpr(e.Right.Scalar))))
	case *NullSafeEq:
		h.Write([]byte(fmt.Sprintf("%d%d%d", e.ExprId(), internExpr(e.Left.Scalar), internExpr(e.Right.Scalar))))
	case *Gt:
		h.Write([]byte(fmt.Sprintf("%d%d%d", e.ExprId(), internExpr(e.Left.Scalar), internExpr(e.Right.Scalar))))
	case *Lt:
		h.Write([]byte(fmt.Sprintf("%d%d%d", e.ExprId(), internExpr(e.Left.Scalar), internExpr(e.Right.Scalar))))
	case *Geq:
		h.Write([]byte(fmt.Sprintf("%d%d%d", e.ExprId(), internExpr(e.Left.Scalar), internExpr(e.Right.Scalar))))
	case *Leq:
		h.Write([]byte(fmt.Sprintf("%d%d%d", e.ExprId(), internExpr(e.Left.Scalar), internExpr(e.Right.Scalar))))
	case *IsNull:
		h.Write([]byte(fmt.Sprintf("%d%d%d", e.ExprId(), internExpr(e.Child.Scalar))))
	case *Hidden:
		h.Write([]byte(fmt.Sprintf("%d%s", e.ExprId(), e.String())))
	case *Arithmetic:
		h.Write([]byte(fmt.Sprintf("%d%d%d", e.ExprId(), internExpr(e.Left.Scalar), internExpr(e.Right.Scalar))))
	case *Or:
		h.Write([]byte(fmt.Sprintf("%d%d%d", e.ExprId(), internExpr(e.Left.Scalar), internExpr(e.Right.Scalar))))
	case *And:
		h.Write([]byte(fmt.Sprintf("%d%d%d", e.ExprId(), internExpr(e.Left.Scalar), internExpr(e.Right.Scalar))))
	}
	return h.Sum64()
}
