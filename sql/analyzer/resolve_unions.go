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
)

// resolveUnions resolves the left and right side of a union node in isolation.
func resolveUnions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	if n.Resolved() {
		return n, nil
	}
	// Procedures explicitly handle unions
	if _, ok := n.(*plan.CreateProcedure); ok {
		return n, nil
	}

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.Union:
			subqueryCtx, cancelFunc := ctx.NewSubContext()
			defer cancelFunc()

			left, err := a.analyzeThroughBatch(subqueryCtx, n.Left(), scope, "default-rules")
			if err != nil {
				return nil, err
			}

			right, err := a.analyzeThroughBatch(subqueryCtx, n.Right(), scope, "default-rules")
			if err != nil {
				return nil, err
			}

			return n.WithChildren(stripQueryProcess(left), stripQueryProcess(right))
		default:
			return n, nil
		}
	})
}

func finalizeUnions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	// Procedures explicitly handle unions
	if _, ok := n.(*plan.CreateProcedure); ok {
		return n, nil
	}

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.Union:
			subqueryCtx, cancelFunc := ctx.NewSubContext()
			defer cancelFunc()

			left, err := a.analyzeStartingAtBatch(subqueryCtx, n.Left(), scope, "default-rules")
			if err != nil {
				return nil, err
			}

			right, err := a.analyzeStartingAtBatch(subqueryCtx, n.Right(), scope, "default-rules")
			if err != nil {
				return nil, err
			}

			return n.WithChildren(stripQueryProcess(left), stripQueryProcess(right))
		default:
			return n, nil
		}
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
func mergeUnionSchemas(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	if !n.Resolved() {
		return n, nil
	}
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		if u, ok := n.(*plan.Union); ok {
			ls, rs := u.Left().Schema(), u.Right().Schema()
			if len(ls) != len(rs) {
				return nil, ErrUnionSchemasDifferentLength.New(len(ls), len(rs))
			}
			les, res := make([]sql.Expression, len(ls)), make([]sql.Expression, len(rs))
			hasdiff := false
			for i := range ls {
				les[i] = expression.NewGetFieldWithTable(i, ls[i].Type, ls[i].Source, ls[i].Name, ls[i].Nullable)
				res[i] = expression.NewGetFieldWithTable(i, rs[i].Type, rs[i].Source, rs[i].Name, rs[i].Nullable)
				if reflect.DeepEqual(ls[i].Type, rs[i].Type) {
					continue
				}
				hasdiff = true

				// TODO: Principled type coercion...
				les[i] = expression.NewConvert(les[i], expression.ConvertToChar)
				res[i] = expression.NewConvert(res[i], expression.ConvertToChar)

				// Preserve schema names across the conversion.
				les[i] = expression.NewAlias(ls[i].Name, les[i])
				res[i] = expression.NewAlias(rs[i].Name, res[i])
			}
			if hasdiff {
				return u.WithChildren(
					plan.NewProject(les, u.Left()),
					plan.NewProject(res, u.Right()),
				)
			} else {
				return u, nil
			}
		}
		return n, nil
	})
}
