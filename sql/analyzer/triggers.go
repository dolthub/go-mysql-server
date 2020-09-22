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
	"github.com/liquidata-inc/vitess/go/vt/sqlparser"
	"strings"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/parse"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

type triggerColumnRef struct {
	*expression.UnresolvedColumn
}

func (r *triggerColumnRef) Resolved() bool {
	return true
}

func (r *triggerColumnRef) Type() sql.Type {
	return sql.Boolean
}

func resolveNewAndOldReferences(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	switch n.(type) {
	case *plan.CreateTrigger:
		// For create triggers, we just want to verify that the trigger is correctly defined before creating it. If it
		// is, we replace the UnresolvedColumn expressions with placeholder expressions that say they are Resolved().
		// TODO: validate columns better
		// TODO: this might work badly for databases with tables named new and old. Needs tests.
		return plan.TransformExpressionsUp(n, func(e sql.Expression) (sql.Expression, error) {
			switch e := e.(type) {
			case *expression.UnresolvedColumn:
				if e.Table() == "new" || e.Table() == "old" {
					return &triggerColumnRef{e}, nil
				}
			case *deferredColumn:
				if e.Table() == "new" || e.Table() == "old" {
					return &triggerColumnRef{e.UnresolvedColumn}, nil
				}
			}
			return e, nil
		})
	}

	return n, nil
}

func applyTriggers(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	var affectedTables []string
	var triggerEvent plan.TriggerEvent
	plan.Inspect(n, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.InsertInto:
			affectedTables = append(affectedTables, findTableName(n))
			triggerEvent = plan.InsertTrigger
		case *plan.Update:
			affectedTables = append(affectedTables, findTableName(n))
			triggerEvent = plan.UpdateTrigger
		case *plan.DeleteFrom:
			affectedTables = append(affectedTables, findTableName(n))
			triggerEvent = plan.DeleteTrigger
		}
		return true
	})

	if len(affectedTables) == 0 {
		return n, nil
	}

	// TODO: database should be dependent on the table being inserted / updated, but we don't have that info available
	//  from the table object yet.
	db := ctx.GetCurrentDatabase()
	database, err := a.Catalog.Database(db)
	if err != nil {
		return nil, err
	}

	var affectedTriggers []*plan.CreateTrigger
	if tdb, ok := database.(sql.TriggerDatabase); ok {
		triggers, err := tdb.GetTriggers(ctx)
		if err != nil {
			return nil, err
		}

		for _, trigger := range triggers {
			parsedTrigger, err := parse.Parse(ctx, trigger.CreateStatement)
			if err != nil {
				return nil, err
			}

			ct, ok := parsedTrigger.(*plan.CreateTrigger)
			if !ok {
				return nil, sql.ErrTriggerCreateStatementInvalid.New(trigger.CreateStatement)
			}

			triggerTable := findTableName(ct.Table)
			if stringContains(affectedTables, triggerTable) && triggerEventsMatch(triggerEvent, ct.TriggerEvent) {
				// TODO: ordering of multiple triggers
				affectedTriggers = append(affectedTriggers, ct)
			}
		}
	}

	if len(affectedTriggers) == 0 {
		return n, nil
	}

	// TODO: multiple triggers
	trigger := affectedTriggers[0]

	var triggerLogic sql.Node

	// For the reference to the row in the trigger table, we use the scope mechanism. This is a little strange because
	// scopes for subqueries work with the child schemas of a scope node, but we don't have such a node here. Instead we
	// fabricate one with the right properties (its child schema matches the table schema, with the right aliased name)
	switch triggerEvent {
	case plan.InsertTrigger:
		scopeNode := plan.NewProject(
			[]sql.Expression{expression.NewStar()},
			plan.NewTableAlias("new", getResolvedTable(n)),
		)
		triggerLogic, err = a.Analyze(ctx, trigger.Body, ((*Scope)(nil)).newScope(scopeNode))
	case plan.UpdateTrigger:
		scopeNode := plan.NewProject(
			[]sql.Expression{expression.NewStar()},
			plan.NewCrossJoin(
				plan.NewTableAlias("old", getResolvedTable(n)),
				plan.NewTableAlias("new", getResolvedTable(n)),
			),
		)
		triggerLogic, err = a.Analyze(ctx, trigger.Body, ((*Scope)(nil)).newScope(scopeNode))
	case plan.DeleteTrigger:
		scopeNode := plan.NewProject(
			[]sql.Expression{expression.NewStar()},
			plan.NewTableAlias("old", getResolvedTable(n)),
		)
		triggerLogic, err = a.Analyze(ctx, trigger.Body, ((*Scope)(nil)).newScope(scopeNode))
	}

	if err != nil {
		return nil, err
	}

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.InsertInto:
			if trigger.TriggerTime == sqlparser.BeforeStr {
				triggerExecutor := plan.NewTriggerExecutor(n.Right, triggerLogic, plan.InsertTrigger, plan.TriggerTime(trigger.TriggerTime), sql.TriggerDefinition{
					Name:            trigger.TriggerName,
					CreateStatement: trigger.CreateTriggerString,
				})
				return n.WithChildren(n.Left, triggerExecutor)
			} else {
				return plan.NewTriggerExecutor(n.Right, triggerLogic, plan.InsertTrigger, plan.TriggerTime(trigger.TriggerTime), sql.TriggerDefinition{
					Name:            trigger.TriggerName,
					CreateStatement: trigger.CreateTriggerString,
				}), nil
			}
		case *plan.Update:
			if trigger.TriggerTime == sqlparser.BeforeStr {
				triggerExecutor := plan.NewTriggerExecutor(n.Child, triggerLogic, plan.UpdateTrigger, plan.TriggerTime(trigger.TriggerTime), sql.TriggerDefinition{
					Name:            trigger.TriggerName,
					CreateStatement: trigger.CreateTriggerString,
				})
				return n.WithChildren(triggerExecutor)
			} else {
				return plan.NewTriggerExecutor(n, triggerLogic, plan.UpdateTrigger, plan.TriggerTime(trigger.TriggerTime), sql.TriggerDefinition{
					Name:            trigger.TriggerName,
					CreateStatement: trigger.CreateTriggerString,
				}), nil
			}
		case *plan.DeleteFrom:
			if trigger.TriggerTime == sqlparser.BeforeStr {
				triggerExecutor := plan.NewTriggerExecutor(n.Child, triggerLogic, plan.DeleteTrigger, plan.TriggerTime(trigger.TriggerTime), sql.TriggerDefinition{
					Name:            trigger.TriggerName,
					CreateStatement: trigger.CreateTriggerString,
				})
				return n.WithChildren(triggerExecutor)
			} else {
				return plan.NewTriggerExecutor(n, triggerLogic, plan.DeleteTrigger, plan.TriggerTime(trigger.TriggerTime), sql.TriggerDefinition{
					Name:            trigger.TriggerName,
					CreateStatement: trigger.CreateTriggerString,
				}), nil
			}
		}

		return n, nil
	})
}

func triggerEventsMatch(event plan.TriggerEvent, event2 string) bool {
	return strings.ToLower((string)(event)) == strings.ToLower(event2)
}
