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
)

func clearWarnings(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (sql.Node, error) {
	children := node.Children()
	if len(children) == 0 {
		return node, nil
	}

	switch ch := children[0].(type) {
	case plan.ShowWarnings:
		return node, nil
	case *plan.Offset:
		clearWarnings(ctx, a, ch, scope)
		return node, nil
	case *plan.Limit:
		clearWarnings(ctx, a, ch, scope)
		return node, nil
	}

	ctx.ClearWarnings()
	return node, nil
}
