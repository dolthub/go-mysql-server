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

// beginTransaction begins a new transaction if one is not currently in progress, by wrapping any top-level node given
// in a StartTransaction node that will run before it.
func beginTransaction(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	// Don't wrap subqueries
	if scope != nil {
		return n, nil
	}

	// If there is a transaction already in progress, don't begin a new one
	if ctx.GetTransaction() != nil {
		return n, nil
	}

	switch n := n.(type) {
	case *plan.StartTransaction:
		return n, nil
	default:
		start, err := plan.NewStartTransaction("").WithChildren(n)
		if err != nil {
			return nil, err
		}

		return resolveDatabase(ctx, a, start, scope)
	}
}
