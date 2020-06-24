// Copyright 2020 Liquidata, Inc.
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
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

// Grab-bag analyzer function to assign information schema info to any plan nodes that need it, like various SHOW *
// statements. The logic for each node is necessarily pretty custom.
func assignInfoSchema(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		if !n.Resolved() {
			return n, nil
		}

		if si, ok := n.(*plan.ShowIndexes); ok {
			ia, err := getIndexesForNode(ctx, a, si)
			if err != nil {
				return nil, err
			}

			var tableName string
			if rt, ok := si.Child.(*plan.ResolvedTable); ok {
				tableName = rt.Name()
			}

			si.IndexesToShow = ia.IndexesByTable(ctx, ctx.GetCurrentDatabase(), tableName)
		}

		return n, nil
	})
}
