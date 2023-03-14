// Copyright 2023 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// preparedStatements handles all statements involved with prepared statements
func preparedStatements(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	identity := transform.SameTree
	if cacher, ok := n.(sql.PreparedDataCacher); ok {
		newNode, err := cacher.WithPreparedDataCache(a.PreparedDataCache)
		if err != nil {
			return nil, transform.SameTree, err
		}
		n = newNode
		identity = transform.NewTree
	}

	switch resolvedNode := n.(type) {
	case *plan.PrepareQuery:
		n = resolvedNode.WithDelayedAnalyzer(func(ctx *sql.Context, child sql.Node) (sql.Node, error) {
			return a.PrepareQuery(ctx, child, scope)
		})
		identity = transform.NewTree
	case *plan.ExecuteQuery:
		n = resolvedNode.WithDelayedAnalyzer(func(ctx *sql.Context, child sql.Node) (sql.Node, error) {
			analyzedNode, _, err := a.AnalyzePrepared(ctx, child, scope)
			return analyzedNode, err
		})
		identity = transform.NewTree
	}

	return n, identity, nil
}
