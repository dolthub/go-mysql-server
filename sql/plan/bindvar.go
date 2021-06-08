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
//
// This applies binding substitutions across *SubqueryAlias nodes, but will
// fail to apply bindings across other |sql.Opaque| nodes.
func ApplyBindings(ctx *sql.Context, n sql.Node, bindings map[string]sql.Expression) (sql.Node, error) {
	withSubqueries, err := TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *SubqueryAlias:
			child, err := ApplyBindings(ctx, n.Child, bindings)
			if err != nil {
				return nil, err
			}
			return n.WithChildren(child)
		case *InsertInto:
			source, err := ApplyBindings(ctx, n.Source, bindings)
			if err != nil {
				return nil, err
			}
			return n.WithSource(source), nil
		default:
			return n, nil
		}
	})
	if err != nil {
		return nil, err
	}
	return TransformExpressionsUp(ctx, withSubqueries, func(e sql.Expression) (sql.Expression, error) {
		if bv, ok := e.(*expression.BindVar); ok {
			val, found := bindings[bv.Name]
			if found {
				return val, nil
			}
		}
		return e, nil
	})
}
