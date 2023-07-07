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

// applyEventScheduler configures AddEvent, UpdateEvent and RemoveEvent nodes with the
// EventScheduler that the Analyzer holds.
func applyEventScheduler(_ *sql.Context, a *Analyzer, n sql.Node, _ *plan.Scope, _ RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch nn := n.(type) {
		case sql.EventSchedulerStatement:
			return nn.WithEventScheduler(a.EventScheduler), transform.NewTree, nil
		default:
			return n, transform.SameTree, nil
		}
	})
}
