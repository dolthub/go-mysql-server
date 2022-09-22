// Copyright 2022 Dolthub, Inc.
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

// reifyFullOuterJoin converts full join back to a Union of LeftJoin
// and RightJoin
func reifyFullOuterJoin(
	ctx *sql.Context,
	a *Analyzer,
	n sql.Node,
	scope *Scope,
	sel RuleSelector,
) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		foj, ok := n.(*plan.FullOuterJoin)
		if !ok {
			return n, transform.SameTree, nil
		}
		return plan.NewUnion(
			plan.NewLeftJoin(foj.Left(), foj.Right(), foj.Filter),
			plan.NewRightJoin(foj.Left(), foj.Right(), foj.Filter),
			true, nil, nil), transform.NewTree, nil
	})
}
