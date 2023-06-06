// Copyright 2023 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

// TODO code gen to make sure we are not missing
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
		h.Write([]byte(fmt.Sprintf("%d%d", e.ExprId(), internExpr(e.Child.Scalar))))
	case *Bindvar:
		h.Write([]byte(fmt.Sprintf("%d%s", e.ExprId(), e.Name)))
	case *Hidden:
		h.Write([]byte(fmt.Sprintf("%d%s", e.ExprId(), e.String())))
	case *Arithmetic:
		h.Write([]byte(fmt.Sprintf("%d%d%d", e.ExprId(), internExpr(e.Left.Scalar), internExpr(e.Right.Scalar))))
	case *Or:
		h.Write([]byte(fmt.Sprintf("%d%d%d", e.ExprId(), internExpr(e.Left.Scalar), internExpr(e.Right.Scalar))))
	case *And:
		h.Write([]byte(fmt.Sprintf("%d%d%d", e.ExprId(), internExpr(e.Left.Scalar), internExpr(e.Right.Scalar))))
	case *InTuple:
		h.Write([]byte(fmt.Sprintf("%d%d%d", e.ExprId(), internExpr(e.Left.Scalar), internExpr(e.Right.Scalar))))
	case *Tuple:

		h.Write([]byte(fmt.Sprintf("%d", e.ExprId())))
		for _, c := range e.Values {
			h.Write([]byte(fmt.Sprintf("%d", internExpr(c.Scalar))))
		}
	case *Regexp:
		h.Write([]byte(fmt.Sprintf("%d%d%d", e.ExprId(), internExpr(e.Left.Scalar), internExpr(e.Right.Scalar))))
	case *Not:
		h.Write([]byte(fmt.Sprintf("%d%d", e.ExprId(), internExpr(e.Child.Scalar))))
	default:
		return 0
	}
	return h.Sum64()
}
