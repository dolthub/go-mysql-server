// Copyright 2020-2021 Dolthub, Inc.
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
	"reflect"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// resolveUnions resolves the left and right side of a union node in isolation.
func resolveUnions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if n.Resolved() {
		return n, transform.SameTree, nil
	}
	// Procedures explicitly handle unions
	if _, ok := n.(*plan.CreateProcedure); ok {
		return n, transform.SameTree, nil
	}

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		var u *plan.Union
		switch n := n.(type) {
		case *plan.Union:
			u = n
		default:
			return n, transform.SameTree, nil
		}
		subqueryCtx, cancelFunc := ctx.NewSubContext()
		defer cancelFunc()

		left, _, err := a.analyzeThroughBatch(subqueryCtx, u.Left(), scope, "default-rules", sel)
		if err != nil {
			return nil, transform.SameTree, err
		}

		right, _, err := a.analyzeThroughBatch(subqueryCtx, u.Right(), scope, "default-rules", sel)
		if err != nil {
			return nil, transform.SameTree, err
		}

		ret, err := n.WithChildren(StripPassthroughNodes(left), StripPassthroughNodes(right))
		if err != nil {
			return nil, transform.SameTree, err
		}
		return ret, transform.NewTree, nil
	})
}

func finalizeUnions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	// Procedures explicitly handle unions
	if _, ok := n.(*plan.CreateProcedure); ok {
		return n, transform.SameTree, nil
	}

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		var u *plan.Union
		switch n := n.(type) {
		case *plan.Union:
			u = n
		case *plan.RecursiveCte:
			// rCTEs behave like unions after default rules
			u = n.Union()
		default:
			return n, transform.SameTree, nil
		}
		subqueryCtx, cancelFunc := ctx.NewSubContext()
		defer cancelFunc()

		scope.SetJoin(false)
		// TODO we could detect tree modifications here, skip rebuilding
		left, _, err := a.analyzeStartingAtBatch(subqueryCtx, u.Left(), scope, "default-rules", NewFinalizeUnionSel(sel))
		if err != nil {
			return nil, transform.SameTree, err
		}

		scope.SetJoin(false)

		right, _, err := a.analyzeStartingAtBatch(subqueryCtx, u.Right(), scope, "default-rules", NewFinalizeUnionSel(sel))
		if err != nil {
			return nil, transform.SameTree, err
		}

		scope.SetJoin(false)

		newn, err := n.WithChildren(StripPassthroughNodes(left), StripPassthroughNodes(right))
		if err != nil {
			return nil, transform.SameTree, err
		}
		return newn, transform.NewTree, nil
	})
}

var (
	// ErrUnionSchemasDifferentLength is returned when the two sides of a
	// UNION do not have the same number of columns in their schemas.
	ErrUnionSchemasDifferentLength = errors.NewKind(
		"cannot union two queries whose schemas are different lengths; left has %d column(s) right has %d column(s).",
	)
)

// mergeUnionSchemas determines the narrowest possible shared schema types between the two sides of a union, and
// applies projections the two sides to convert column types as necessary.
func mergeUnionSchemas(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if !n.Resolved() {
		return n, transform.SameTree, nil
	}
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if u, ok := n.(*plan.Union); ok {
			ls, rs := u.Left().Schema(), u.Right().Schema()
			if len(ls) != len(rs) {
				return nil, transform.SameTree, ErrUnionSchemasDifferentLength.New(len(ls), len(rs))
			}
			les, res := make([]sql.Expression, len(ls)), make([]sql.Expression, len(rs))
			hasdiff := false
			for i := range ls {
				les[i] = expression.NewGetFieldWithTable(i, ls[i].Type, ls[i].Source, ls[i].Name, ls[i].Nullable)
				res[i] = expression.NewGetFieldWithTable(i, rs[i].Type, rs[i].Source, rs[i].Name, rs[i].Nullable)
				if reflect.DeepEqual(ls[i].Type, rs[i].Type) {
					continue
				}
				if types.IsDeferredType(ls[i].Type) || types.IsDeferredType(rs[i].Type) {
					continue
				}
				hasdiff = true

				// try to get optimal type to convert both into
				convertTo := getConvertToType(ls[i].Type, rs[i].Type)

				// TODO: Principled type coercion...
				les[i] = expression.NewConvert(les[i], convertTo)
				res[i] = expression.NewConvert(res[i], convertTo)

				// Preserve schema names across the conversion.
				les[i] = expression.NewAlias(ls[i].Name, les[i])
				res[i] = expression.NewAlias(rs[i].Name, res[i])
			}
			if hasdiff {
				n, err := u.WithChildren(
					plan.NewProject(les, u.Left()),
					plan.NewProject(res, u.Right()),
				)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return n, transform.NewTree, nil
			}
		}
		return n, transform.SameTree, nil
	})
}

// getConvertToType returns which type the both left and right values should be converted to.
// If neither sql.Type represent number, then converted to string. Otherwise, we try to get
// the appropriate type to avoid any precision loss.
func getConvertToType(l, r sql.Type) string {
	if !types.IsNumber(l) || !types.IsNumber(r) {
		return expression.ConvertToChar
	}

	if types.IsDecimal(l) || types.IsDecimal(r) {
		return expression.ConvertToDecimal
	}
	if types.IsUnsigned(l) && types.IsUnsigned(r) {
		return expression.ConvertToUnsigned
	}
	if types.IsSigned(l) && types.IsSigned(r) {
		return expression.ConvertToSigned
	}
	if types.IsInteger(l) && types.IsInteger(r) {
		return expression.ConvertToSigned
	}

	return expression.ConvertToChar
}
