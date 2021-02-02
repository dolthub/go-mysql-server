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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// loadTriggers loads any triggers that are required for a plan node to operate properly (except for nodes dealing with
// trigger execution).
func loadTriggers(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("loadTriggers")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch node := n.(type) {
		case *plan.ShowTriggers:
			newShowTriggers := *node
			loadedTriggers, err := loadTriggersFromDb(ctx, newShowTriggers.Database())
			if err != nil {
				return nil, err
			}
			if len(loadedTriggers) != 0 {
				newShowTriggers.Triggers = loadedTriggers
			} else {
				newShowTriggers.Triggers = make([]*plan.CreateTrigger, 0)
			}
			return &newShowTriggers, nil
		case *plan.DropTrigger:
			loadedTriggers, err := loadTriggersFromDb(ctx, node.Database())
			if err != nil {
				return nil, err
			}
			lowercasedTriggerName := strings.ToLower(node.TriggerName)
			for _, trigger := range loadedTriggers {
				if strings.ToLower(trigger.TriggerName) == lowercasedTriggerName {
					node.TriggerName = trigger.TriggerName
				} else if trigger.TriggerOrder != nil &&
					strings.ToLower(trigger.TriggerOrder.OtherTriggerName) == lowercasedTriggerName {
					return nil, sql.ErrTriggerCannotBeDropped.New(node.TriggerName, trigger.TriggerName)
				}
			}
			return node, nil
		case *plan.DropTable:
			loadedTriggers, err := loadTriggersFromDb(ctx, node.Database())
			if err != nil {
				return nil, err
			}
			lowercasedNames := make(map[string]struct{})
			for _, tableName := range node.TableNames() {
				lowercasedNames[strings.ToLower(tableName)] = struct{}{}
			}
			var triggersForTable []string
			for _, trigger := range loadedTriggers {
				if _, ok := lowercasedNames[strings.ToLower(trigger.Table.(*plan.UnresolvedTable).Name())]; ok {
					triggersForTable = append(triggersForTable, trigger.TriggerName)
				}
			}
			return node.WithTriggers(triggersForTable), nil
		default:
			return node, nil
		}
	})
}

func loadTriggersFromDb(ctx *sql.Context, db sql.Database) ([]*plan.CreateTrigger, error) {
	var loadedTriggers []*plan.CreateTrigger
	if triggerDb, ok := db.(sql.TriggerDatabase); ok {
		triggers, err := triggerDb.GetTriggers(ctx)
		if err != nil {
			return nil, err
		}
		for _, trigger := range triggers {
			parsedTrigger, err := parse.Parse(ctx, trigger.CreateStatement)
			if err != nil {
				return nil, err
			}
			triggerPlan, ok := parsedTrigger.(*plan.CreateTrigger)
			if !ok {
				return nil, sql.ErrTriggerCreateStatementInvalid.New(trigger.CreateStatement)
			}
			loadedTriggers = append(loadedTriggers, triggerPlan)
		}
	}
	return loadedTriggers, nil
}
