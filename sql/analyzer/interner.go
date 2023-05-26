package analyzer

import (
	"fmt"
	"github.com/cespare/xxhash"
)

type scalarExprId uint8

const (
	unknownScalar scalarExprId = iota
	equalExpr
	notExpr
	colRefExpr
	literalExpr
	orExpr
	andExpr
	inTupleExpr
	ltExpr
	leqExpr
	gtExpr
	geqExpr
	nullSafeEqExpr
	regexpExpr
)

func internExpr(e scalarExpr) uint64 {
	h := xxhash.New()
	switch e := e.(type) {
	case *literal:
		h.Write([]byte(fmt.Sprintf("%v", e.val)))
	case *colRef:
		h.Write([]byte(fmt.Sprintf("%d", e.col)))
	case *equal:
		h.Write([]byte(fmt.Sprintf("%d%d%d", e.exprId(), internExpr(e.left.scalar), internExpr(e.right.scalar))))
	}
	return h.Sum64()
}
