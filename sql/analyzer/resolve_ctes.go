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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// resolveCommonTableExpressions operates on With nodes. It replaces any matching UnresolvedTable references in the
// tree with the subqueries defined in the CTEs.
func resolveCommonTableExpressions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	with, ok := n.(*plan.With)
	if !ok {
		return nil, nil
	}

	ctes := make(map[string]sql.Node)
	for _, cte := range with.CTEs {
		// TODO: column name aliases and schema validation
		ctes[strings.ToLower(cte.Subquery.Name())] = cte.Subquery
	}

	// Transform in two passes: the first to catch any uses of CTEs in subquery expressions
	child, err := plan.TransformExpressionsUp(with.Child, func(e sql.Expression) (sql.Expression, error) {
		sq, ok := e.(*plan.Subquery)
		if !ok {
			return e, nil
		}

		// TODO: needs some form of scope
		query, err := resolveCommonTableExpressions(ctx, a, sq.Query, scope)
		if err != nil {
			return nil, err
		}

		return sq.WithQuery(query), nil
	})
	if err != nil {
		return nil, err
	}

	// Second pass to catch any uses of CTEs as tables
	return plan.TransformUp(child, func(n sql.Node) (sql.Node, error) {
		t, ok := n.(*plan.UnresolvedTable)
		if !ok {
			return n, nil
		}

		lowerName := strings.ToLower(t.Name())
		if ctes[lowerName] != nil {
			return ctes[lowerName], nil
		}

		return n, nil
	})
}
