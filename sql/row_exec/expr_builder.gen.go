// Copyright 2021 Dolthub, Inc.
//
// GENERATED
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

package row_exec

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *Builder) buildExprExec(n sql.Expression, row sql.Row) (sql.RowIter, error) {
	switch n := n.(type) {
	case *expression.SetField:
		return buildSetField(n, row)
	case *expression.Wrapper:
		return buildWrapper(n, row)
	case *expression.Case:
		return buildCase(n, row)
	case *expression.NullSafeEquals:
		return buildNullSafeEquals(n, row)
	case *expression.GreaterThanOrEqual:
		return buildGreaterThanOrEqual(n, row)
	case *expression.LessThanOrEqual:
		return buildLessThanOrEqual(n, row)
	case *expression.UnresolvedColumn:
		return buildUnresolvedColumn(n, row)
	case *expression.UnresolvedFunction:
		return buildUnresolvedFunction(n, row)
	case *plan.ExistsSubquery:
		return buildExistsSubquery(n, row)
	case *expression.LessThan:
		return buildLessThan(n, row)
	case *expression.And:
		return buildAnd(n, row)
	case *expression.Xor:
		return buildXor(n, row)
	case *expression.ProcedureParam:
		return buildProcedureParam(n, row)
	case *expression.DefaultColumn:
		return buildDefaultColumn(n, row)
	case expression.NamedLiteral:
		return buildNamedLiteral(n, row)
	case *expression.Star:
		return buildStar(n, row)
	case *expression.UnaryMinus:
		return buildUnaryMinus(n, row)
	case *expression.Binary:
		return buildBinary(n, row)
	case *expression.BindVar:
		return buildBindVar(n, row)
	case *expression.GreaterThan:
		return buildGreaterThan(n, row)
	case *expression.Alias:
		return buildAlias(n, row)
	case *expression.Between:
		return buildBetween(n, row)
	case *expression.Like:
		return buildLike(n, row)
	case *expression.Or:
		return buildOr(n, row)
	case *expression.IsTrue:
		return buildIsTrue(n, row)
	case *expression.AutoIncrement:
		return buildAutoIncrement(n, row)
	case *expression.GetField:
		return buildGetField(n, row)
	case *expression.Interval:
		return buildInterval(n, row)
	case *function.Rand:
		return buildRand(n, row)
	case *function.Time:
		return buildTime(n, row)
	case expression.Literal:
		return buildLiteral(n, row)
	case *expression.UserVar:
		return buildUserVar(n, row)
	case *expression.AliasReference:
		return buildAliasReference(n, row)
	case *expression.BitOp:
		return buildBitOp(n, row)
	case *expression.Not:
		return buildNot(n, row)
	case *expression.Equals:
		return buildEquals(n, row)
	case *expression.UnresolvedProcedureParam:
		return buildUnresolvedProcedureParam(n, row)
	case *expression.SystemVar:
		return buildSystemVar(n, row)
	case *plan.InSubquery:
		return buildInSubquery(n, row)
	case *expression.CollatedExpression:
		return buildCollatedExpression(n, row)
	case *expression.Convert:
		return buildConvert(n, row)
	case *expression.DistinctExpression:
		return buildDistinctExpression(n, row)
	case *expression.IsNull:
		return buildIsNull(n, row)
	case *expression.Regexp:
		return buildRegexp(n, row)
	default:
		return nil, fmt.Errorf("Unknown Expr type")
	}
}
