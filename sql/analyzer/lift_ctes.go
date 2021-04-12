// Copyright 2021 Dolthub, Inc.
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

package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// liftCommonTableExpressions lifts With nodes above Union and Distinct
// nodes.  Currently as parsed, we get Union(CTE(...), ...), and we can
// transform that to CTE(Union(..., ...)) to make the CTE visible across the
// Union.
//
// This will have surprising behavior in the case of something like:
//   (WITH t AS SELECT ... SELECT ...) UNION ...
// where the CTE will be visible on the second half of the UNION. We live with
// it for now.
func liftCommonTableExpressions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		if union, isUnion := n.(*plan.Union); isUnion {
			if cte, isCTE := union.Left().(*plan.With); isCTE {
				return plan.NewWith(plan.NewUnion(cte.Child, union.Right()), cte.CTEs), nil
			}
			l, err := liftCommonTableExpressions(ctx, a, union.Left(), scope)
			if err != nil {
				return nil, err
			}
			r, err := liftCommonTableExpressions(ctx, a, union.Right(), scope)
			if err != nil {
				return nil, err
			}
			if _, isCTE := l.(*plan.With); isCTE {
				return liftCommonTableExpressions(ctx, a, plan.NewUnion(l, r), scope)
			}
			return plan.NewUnion(l, r), nil
		}
		if distinct, isDistinct := n.(*plan.Distinct); isDistinct {
			if cte, isCTE := distinct.Child.(*plan.With); isCTE {
				return plan.NewWith(plan.NewDistinct(cte.Child), cte.CTEs), nil
			}
		}
		return n, nil
	})
}
