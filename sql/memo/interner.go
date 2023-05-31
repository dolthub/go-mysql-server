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

func internExpr(e ScalarExpr) uint64 {
	h := xxhash.New()
	switch e := e.(type) {
	case *Literal:
		h.Write([]byte(fmt.Sprintf("%v", e.Val)))
	case *ColRef:
		h.Write([]byte(fmt.Sprintf("%d", e.Col)))
	case *Equal:
		h.Write([]byte(fmt.Sprintf("%d%d%d", e.ExprId(), internExpr(e.Left.Scalar), internExpr(e.Right.Scalar))))
	case *hidden:
		h.Write([]byte(fmt.Sprintf("%d%s", e.ExprId(), e.String())))
	}
	return h.Sum64()
}
