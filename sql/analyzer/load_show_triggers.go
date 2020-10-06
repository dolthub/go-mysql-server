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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func loadShowTriggers(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("loadShowTriggers")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch node := n.(type) {
		case *plan.ShowTriggers:
			newShowTriggers := *node
			db := newShowTriggers.Database()
			if triggerDb, ok := db.(sql.TriggerDatabase); ok {
				triggers, err := triggerDb.GetTriggers(ctx)
				if err != nil {
					return nil, err
				}
				triggerPlans := make([]*plan.CreateTrigger, len(triggers))
				for i, trigger := range triggers {
					parsedTrigger, err := parse.Parse(ctx, trigger.CreateStatement)
					if err != nil {
						return nil, err
					}
					triggerPlan, ok := parsedTrigger.(*plan.CreateTrigger)
					if !ok {
						return nil, sql.ErrTriggerCreateStatementInvalid.New(trigger.CreateStatement)
					}
					triggerPlans[i] = triggerPlan
				}
				newShowTriggers.Triggers = triggerPlans
			} else {
				newShowTriggers.Triggers = make([]*plan.CreateTrigger, 0)
			}
			return &newShowTriggers, nil
		default:
			return node, nil
		}
	})
}
