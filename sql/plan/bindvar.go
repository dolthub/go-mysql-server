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
)

// ApplyBindings replaces all `BindVar` expressions in the given sql.Node with
// their corresponding sql.Expression entries in the provided |bindings| map.
// If a binding for a |BindVar| expression is not found in the map, no error is
// returned and the |BindVar| expression is left in place. There is no check on
// whether all entries in |bindings| are used at least once throughout the |n|.
// sql.DeferredType instances will be resolved by the binding types.
func ApplyBindings(n sql.Node, bindings map[string]sql.Expression) (sql.Node, error) {
	fixBindings := func(_ sql.Node, expr sql.Expression) (sql.Expression, sql.TreeIdentity, error) {
		switch e := expr.(type) {
		case *expression.BindVar:
			val, found := bindings[e.Name]
			if found {
				return val, sql.NewTree, nil
			}
		case *Subquery:
			// *Subquery is a sql.Expression with a sql.Node not reachable
			// by the visitor. Manually apply bindings to [Query] field.
			q, err := ApplyBindings(e.Query, bindings)
			if err != nil {
				return nil, sql.SameTree, err
			}
			return e.WithQuery(q), sql.NewTree, nil
		}
		return expr, sql.SameTree, nil
	}

	n, _, err := TransformUpWithOpaque(n, func(node sql.Node) (sql.Node, sql.TreeIdentity, error) {
		switch n := node.(type) {
		case *InsertInto:
			// Manually apply bindings to [Source] because it is separated
			// from [Destination].
			newSource, err := ApplyBindings(n.Source, bindings)
			if err != nil {
				return nil, sql.SameTree, err
			}
			return n.WithSource(newSource), sql.NewTree, nil
		default:
			//return TransformExpressionsUp(node, fixBindings)
			return TransformUpHelper(node, func(n sql.Node) (sql.Node, sql.TreeIdentity, error) {
				return TransformExpressionsWithNode(n, fixBindings)
			})
		}
	})
	if err != nil {
		return nil, err
	}
	return n, err
}
