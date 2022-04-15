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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// resolveDescribeQuery resolves any DescribeQuery nodes by analyzing their child and assigning it back.
func resolveDescribeQuery(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	d, ok := n.(*plan.DescribeQuery)
	if !ok {
		return n, transform.SameTree, nil
	}

	q, _, err := a.analyzeWithSelector(ctx, d.Query(), scope, SelectAllBatches, sel)
	if err != nil {
		return nil, transform.SameTree, err
	}

	return d.WithQuery(StripPassthroughNodes(q)), transform.NewTree, nil
}
