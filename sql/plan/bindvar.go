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

package plan

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// TODO: ApplyBindings is only ever called for testing purposes and the other functions in this file are either never
//  called or only called by each other. Consider deleting this file.

// ApplyBindings replaces all `BindVar` expressions in the given sql.Node with
// their corresponding sql.Expression entries in the provided |bindings| map.
// If a binding for a |BindVar| expression is not found in the map, no error is
// returned and the |BindVar| expression is left in place. There is no check on
// whether all entries in |bindings| are used at least once throughout the |n|
// but a map of all the used |bindings| are returned.
// sql.DeferredType instances will be resolved by the binding types.
func ApplyBindings(ctx *sql.Context, n sql.Node, bindings map[string]sql.Expression) (sql.Node, map[string]bool, error) {
	n, _, usedBindings, err := applyBindingsHelper(ctx, n, bindings)
	if err != nil {
		return nil, nil, err
	}
	return n, usedBindings, err
}

func fixBindings(ctx *sql.Context, expr sql.Expression, bindings map[string]sql.Expression) (sql.Expression, transform.TreeIdentity, map[string]bool, error) {
	usedBindings := map[string]bool{}
	switch e := expr.(type) {
	case *expression.BindVar:
		val, found := bindings[e.Name]
		if found {
			usedBindings[e.Name] = true
			return val, transform.NewTree, usedBindings, nil
		}
	case *expression.GetField:
		//TODO: aliases derived from arithmetic
		// expressions on BindVars should have types
		// re-evaluated
		t, ok := e.Type(ctx).(sql.DeferredType)
		if !ok {
			return expr, transform.SameTree, nil, nil
		}
		val, found := bindings[t.Name()]
		if !found {
			return expr, transform.SameTree, nil, nil
		}
		usedBindings[t.Name()] = true
		return expression.NewGetFieldWithTable(e.Index(), int(e.TableId()), val.Type(ctx).Promote(), e.Database(), e.Table(), e.Name(), val.IsNullable(ctx)), transform.NewTree, usedBindings, nil
	case *Subquery:
		// *Subquery is a sql.Expression with a sql.Node not reachable
		// by the visitor. Manually apply bindings to [Query] field.
		q, subUsedBindings, err := ApplyBindings(ctx, e.Query, bindings)
		if err != nil {
			return nil, transform.SameTree, nil, err
		}
		for binding := range subUsedBindings {
			usedBindings[binding] = true
		}
		return e.WithQuery(q), transform.NewTree, usedBindings, nil
	}
	return expr, transform.SameTree, nil, nil
}

func applyBindingsHelper(ctx *sql.Context, n sql.Node, bindings map[string]sql.Expression) (sql.Node, transform.TreeIdentity, map[string]bool, error) {
	usedBindings := map[string]bool{}
	fixBindingsTransform := func(ctx *sql.Context, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		newN, same, subUsedBindings, err := fixBindings(ctx, e, bindings)
		for binding := range subUsedBindings {
			usedBindings[binding] = true
		}
		return newN, same, err
	}
	newN, same, err := transform.NodeWithOpaque(ctx, n, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := node.(type) {
		case *JoinNode:
			// *plan.IndexedJoin cannot implement sql.Expressioner
			// because the column indexes get mis-ordered by FixFieldIndexesForExpressions.
			if n.Op.IsLookup() {
				cond, same, err := transform.Expr(ctx, n.Filter, fixBindingsTransform)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return NewJoin(n.left, n.right, n.Op, cond).WithScopeLen(n.ScopeLen), same, nil
			}
		case *InsertInto:
			// Manually apply bindings to [Source] because only [Destination]
			// is a proper child.
			newSource, same, subUsedBindings, err := applyBindingsHelper(ctx, n.Source, bindings)
			if err != nil {
				return nil, transform.SameTree, err
			}
			for binding := range subUsedBindings {
				usedBindings[binding] = true
			}
			if same {
				return transform.NodeExprs(ctx, n, fixBindingsTransform)
			}
			ne, _, err := transform.NodeExprs(ctx, n.WithSource(newSource), fixBindingsTransform)
			return ne, transform.NewTree, err
		case *DeferredFilteredTable:
			ft := n.Table.(sql.FilteredTable)
			var fixedFilters []sql.Expression
			for _, filter := range ft.Filters() {
				newFilter, _, err := transform.Expr(ctx, filter, func(ctx *sql.Context, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
					if bindVar, ok := e.(*expression.BindVar); ok {
						if val, found := bindings[bindVar.Name]; found {
							usedBindings[bindVar.Name] = true
							return val, transform.NewTree, nil
						}
					}
					return e, transform.SameTree, nil
				})
				if err != nil {
					return nil, transform.SameTree, err
				}
				fixedFilters = append(fixedFilters, newFilter)
			}

			newTbl := ft.WithFilters(nil, fixedFilters)
			n.ResolvedTable.Table = newTbl
			return n.ResolvedTable, transform.NewTree, nil
		}
		return transform.NodeExprs(ctx, node, fixBindingsTransform)
	})
	return newN, same, usedBindings, err
}

func HasEmptyTable(ctx *sql.Context, n sql.Node) bool {
	found := transform.InspectUp(ctx, n, func(ctx *sql.Context, n sql.Node) bool {
		_, ok := n.(*EmptyTable)
		return ok
	})
	if found {
		return true
	}
	ne, ok := n.(sql.Expressioner)
	if !ok {
		return false
	}
	for _, e := range ne.Expressions() {
		found := transform.InspectExpr(ctx, e, func(ctx *sql.Context, e sql.Expression) bool {
			sq, ok := e.(*Subquery)
			if ok {
				return HasEmptyTable(ctx, sq.Query)
			}
			return false
		})
		if found {
			return true
		}
	}
	return false
}
