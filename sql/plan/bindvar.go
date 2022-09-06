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

// ApplyBindings replaces all `BindVar` expressions in the given sql.Node with
// their corresponding sql.Expression entries in the provided |bindings| map.
// If a binding for a |BindVar| expression is not found in the map, no error is
// returned and the |BindVar| expression is left in place. There is no check on
// whether all entries in |bindings| are used at least once throughout the |n|.
// sql.DeferredType instances will be resolved by the binding types.
func ApplyBindings(n sql.Node, bindings map[string]sql.Expression) (sql.Node, error) {
	n, _, err := applyBindingsHelper(n, bindings)
	if err != nil {
		return nil, err
	}
	return n, err
}

func fixBindings(expr sql.Expression, bindings map[string]sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
	switch e := expr.(type) {
	case *expression.BindVar:
		val, found := bindings[e.Name]
		if found {
			return val, transform.NewTree, nil
		}
	case *expression.GetField:
		//TODO: aliases derived from arithmetic
		// expressions on BindVars should have types
		// re-evaluated
		t, ok := e.Type().(sql.DeferredType)
		if !ok {
			return expr, transform.SameTree, nil
		}
		val, found := bindings[t.Name()]
		if !found {
			return expr, transform.SameTree, nil
		}
		return expression.NewGetFieldWithTable(e.Index(), val.Type().Promote(), e.Table(), e.Name(), val.IsNullable()), transform.NewTree, nil
	case *Subquery:
		// *Subquery is a sql.Expression with a sql.Node not reachable
		// by the visitor. Manually apply bindings to [Query] field.
		q, err := ApplyBindings(e.Query, bindings)
		if err != nil {
			return nil, transform.SameTree, err
		}
		return e.WithQuery(q), transform.NewTree, nil
	}
	return expr, transform.SameTree, nil
}

func applyBindingsHelper(n sql.Node, bindings map[string]sql.Expression) (sql.Node, transform.TreeIdentity, error) {
	fixBindingsTransform := func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		return fixBindings(e, bindings)
	}
	return transform.NodeWithOpaque(n, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := node.(type) {
		case *IndexedJoin:
			// *plan.IndexedJoin cannot implement sql.Expressioner
			// because the column indexes get mis-ordered by FixFieldIndexesForExpressions.
			cond, same, err := transform.Expr(n.Cond, fixBindingsTransform)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return NewIndexedJoin(n.left, n.right, n.joinType, cond, n.scopeLen), same, nil
		case *InsertInto:
			// Manually apply bindings to [Source] because only [Destination]
			// is a proper child.
			newSource, same, err := applyBindingsHelper(n.Source, bindings)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if same {
				return transform.NodeExprs(n, fixBindingsTransform)
			}
			ne, _, err := transform.NodeExprs(n.WithSource(newSource), fixBindingsTransform)
			return ne, transform.NewTree, err
		default:
		}
		return transform.NodeExprs(node, fixBindingsTransform)
	})
}
