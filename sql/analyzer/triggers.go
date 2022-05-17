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

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/grant_tables"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// validateCreateTrigger handles CreateTrigger nodes, resolving references to "old" and "new" table references in
// the trigger body. Also validates that these old and new references are being used appropriately -- they are only
// valid for certain kinds of triggers and certain statements.
func validateCreateTrigger(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	ct, ok := node.(*plan.CreateTrigger)
	if !ok {
		return node, transform.SameTree, nil
	}

	// We just want to verify that the trigger is correctly defined before creating it. If it is, we replace the
	// UnresolvedColumn expressions with placeholder expressions that say they are Resolved().
	// TODO: this might work badly for databases with tables named new and old. Needs tests.
	var err error
	transform.InspectExpressions(ct.Body, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.UnresolvedColumn:
			if strings.ToLower(e.Table()) == "new" {
				if ct.TriggerEvent == sqlparser.DeleteStr {
					err = sql.ErrInvalidUseOfOldNew.New("new", ct.TriggerEvent)
				}
			}
			if strings.ToLower(e.Table()) == "old" {
				if ct.TriggerEvent == sqlparser.InsertStr {
					err = sql.ErrInvalidUseOfOldNew.New("old", ct.TriggerEvent)
				}
			}
		case *deferredColumn:
			if strings.ToLower(e.Table()) == "new" {
				if ct.TriggerEvent == sqlparser.DeleteStr {
					err = sql.ErrInvalidUseOfOldNew.New("new", ct.TriggerEvent)
				}
			}
			if strings.ToLower(e.Table()) == "old" {
				if ct.TriggerEvent == sqlparser.InsertStr {
					err = sql.ErrInvalidUseOfOldNew.New("old", ct.TriggerEvent)
				}
			}
		}
		return true
	})

	if err != nil {
		return nil, transform.SameTree, err
	}

	// Check to see if the plan sets a value for "old" rows, or if an AFTER trigger assigns to NEW. Both are illegal.
	transform.InspectExpressionsWithNode(ct.Body, func(n sql.Node, e sql.Expression) bool {
		if _, ok := n.(*plan.Set); !ok {
			return true
		}

		switch e := e.(type) {
		case *expression.SetField:
			switch left := e.Left.(type) {
			case column:
				if strings.ToLower(left.Table()) == "old" {
					err = sql.ErrInvalidUpdateOfOldRow.New()
				}
				if ct.TriggerTime == sqlparser.AfterStr && strings.ToLower(left.Table()) == "new" {
					err = sql.ErrInvalidUpdateInAfterTrigger.New()
				}
			}
		}

		return true
	})

	if err != nil {
		return nil, transform.SameTree, err
	}

	trigTable := getResolvedTable(ct.Table)
	sch := trigTable.Schema()
	colsList := make(map[string]struct{})
	for _, c := range sch {
		colsList[c.Name] = struct{}{}
	}

	// Check to see if the columns with "new" and "old" table reference are valid columns from the trigger table.
	transform.InspectExpressions(ct.Body, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.UnresolvedColumn:
			if strings.ToLower(e.Table()) == "old" || strings.ToLower(e.Table()) == "new" {
				if _, ok := colsList[e.Name()]; !ok {
					err = sql.ErrUnknownColumn.New(e.Name(), e.Table())
				}
			}
		case *deferredColumn:
			if strings.ToLower(e.Table()) == "old" || strings.ToLower(e.Table()) == "new" {
				if _, ok := colsList[e.Name()]; !ok {
					err = sql.ErrUnknownColumn.New(e.Name(), e.Table())
				}
			}
		}
		return true
	})

	if err != nil {
		return nil, transform.SameTree, err
	}
	return node, transform.NewTree, nil
}

func applyTriggers(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	// Skip this step for CreateTrigger statements
	if _, ok := n.(*plan.CreateTrigger); ok {
		return n, transform.SameTree, nil
	}

	var affectedTables []string
	var triggerEvent plan.TriggerEvent
	db := ctx.GetCurrentDatabase()
	transform.Inspect(n, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.InsertInto:
			affectedTables = append(affectedTables, getTableName(n))
			triggerEvent = plan.InsertTrigger
			if n.Database() != nil && n.Database().Name() != "" {
				db = n.Database().Name()
			}
		case *plan.Update:
			affectedTables = append(affectedTables, getTableName(n))
			triggerEvent = plan.UpdateTrigger
			if n.Database() != "" {
				db = n.Database()
			}
		case *plan.DeleteFrom:
			affectedTables = append(affectedTables, getTableName(n))
			triggerEvent = plan.DeleteTrigger
			if n.Database() != "" {
				db = n.Database()
			}
		}
		return true
	})

	if len(affectedTables) == 0 {
		return n, transform.SameTree, nil
	}

	// TODO: database should be dependent on the table being inserted / updated, but we don't have that info available
	//  from the table object yet.
	database, err := a.Catalog.Database(ctx, db)
	if err != nil {
		return nil, transform.SameTree, err
	}

	var affectedTriggers []*plan.CreateTrigger
	if tdb, ok := database.(sql.TriggerDatabase); ok {
		triggers, err := tdb.GetTriggers(ctx)
		if err != nil {
			return nil, transform.SameTree, err
		}

		for _, trigger := range triggers {
			parsedTrigger, err := parse.Parse(ctx, trigger.CreateStatement)
			if err != nil {
				return nil, transform.SameTree, err
			}

			ct, ok := parsedTrigger.(*plan.CreateTrigger)
			if !ok {
				return nil, transform.SameTree, sql.ErrTriggerCreateStatementInvalid.New(trigger.CreateStatement)
			}

			triggerTable := getTableName(ct.Table)
			if stringContains(affectedTables, triggerTable) && triggerEventsMatch(triggerEvent, ct.TriggerEvent) {
				if block, ok := ct.Body.(*plan.BeginEndBlock); ok {
					ct.Body = plan.NewTriggerBeginEndBlock(block)
				}
				affectedTriggers = append(affectedTriggers, ct)
			}
		}
	}

	if len(affectedTriggers) == 0 {
		return n, transform.SameTree, nil
	}

	triggers := orderTriggersAndReverseAfter(affectedTriggers)
	originalNode := n
	same := transform.SameTree
	allSame := transform.SameTree
	for _, trigger := range triggers {
		err = validateNoCircularUpdates(trigger, originalNode, scope)
		if err != nil {
			return nil, transform.SameTree, err
		}

		n, same, err = applyTrigger(ctx, a, originalNode, n, scope, trigger)
		if err != nil {
			return nil, transform.SameTree, err
		}
		allSame = same && allSame
	}

	return n, allSame, nil
}

// applyTrigger applies the trigger given to the node given, returning the resulting node
func applyTrigger(ctx *sql.Context, a *Analyzer, originalNode, n sql.Node, scope *Scope, trigger *plan.CreateTrigger) (sql.Node, transform.TreeIdentity, error) {
	triggerLogic, err := getTriggerLogic(ctx, a, originalNode, scope, trigger)
	if err != nil {
		return nil, transform.SameTree, err
	}

	return transform.NodeWithCtx(n, nil, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		// Don't double-apply trigger executors to the bodies of triggers. To avoid this, don't apply the trigger if the
		// parent is a trigger body.
		// TODO: this won't work for BEGIN END blocks, stored procedures, etc. For those, we need to examine all ancestors,
		//  not just the immediate parent. Alternately, we could do something like not walk all children of some node types
		//  (probably better).
		if _, ok := c.Parent.(*plan.TriggerExecutor); ok {
			if c.ChildNum == 1 { // Right child is the trigger execution logic
				return c.Node, transform.SameTree, nil
			}
		}

		switch n := c.Node.(type) {
		case *plan.InsertInto:
			if trigger.TriggerTime == sqlparser.BeforeStr {
				triggerExecutor := plan.NewTriggerExecutor(n.Source, triggerLogic, plan.InsertTrigger, plan.TriggerTime(trigger.TriggerTime), sql.TriggerDefinition{
					Name:            trigger.TriggerName,
					CreateStatement: trigger.CreateTriggerString,
				})
				return n.WithSource(triggerExecutor), transform.NewTree, nil
			} else {
				return plan.NewTriggerExecutor(n, triggerLogic, plan.InsertTrigger, plan.TriggerTime(trigger.TriggerTime), sql.TriggerDefinition{
					Name:            trigger.TriggerName,
					CreateStatement: trigger.CreateTriggerString,
				}), transform.NewTree, nil
			}
		case *plan.Update:
			if trigger.TriggerTime == sqlparser.BeforeStr {
				triggerExecutor := plan.NewTriggerExecutor(n.Child, triggerLogic, plan.UpdateTrigger, plan.TriggerTime(trigger.TriggerTime), sql.TriggerDefinition{
					Name:            trigger.TriggerName,
					CreateStatement: trigger.CreateTriggerString,
				})
				node, err := n.WithChildren(triggerExecutor)
				return node, transform.NewTree, err
			} else {
				return plan.NewTriggerExecutor(n, triggerLogic, plan.UpdateTrigger, plan.TriggerTime(trigger.TriggerTime), sql.TriggerDefinition{
					Name:            trigger.TriggerName,
					CreateStatement: trigger.CreateTriggerString,
				}), transform.NewTree, nil
			}
		case *plan.DeleteFrom:
			if trigger.TriggerTime == sqlparser.BeforeStr {
				triggerExecutor := plan.NewTriggerExecutor(n.Child, triggerLogic, plan.DeleteTrigger, plan.TriggerTime(trigger.TriggerTime), sql.TriggerDefinition{
					Name:            trigger.TriggerName,
					CreateStatement: trigger.CreateTriggerString,
				})
				node, err := n.WithChildren(triggerExecutor)
				return node, transform.NewTree, err
			} else {
				return plan.NewTriggerExecutor(n, triggerLogic, plan.DeleteTrigger, plan.TriggerTime(trigger.TriggerTime), sql.TriggerDefinition{
					Name:            trigger.TriggerName,
					CreateStatement: trigger.CreateTriggerString,
				}), transform.NewTree, nil
			}
		}

		return c.Node, transform.SameTree, nil
	})
}

// getTriggerLogic analyzes and returns the Node representing the trigger body for the trigger given, applied to the
// plan node given, which must be an insert, update, or delete.
func getTriggerLogic(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, trigger *plan.CreateTrigger) (sql.Node, error) {
	// For the reference to the row in the trigger table, we use the scope mechanism. This is a little strange because
	// scopes for subqueries work with the child schemas of a scope node, but we don't have such a node here. Instead we
	// fabricate one with the right properties (its child schema matches the table schema, with the right aliased name)
	var triggerLogic sql.Node
	var err error
	switch trigger.TriggerEvent {
	case sqlparser.InsertStr:
		scopeNode := plan.NewProject(
			[]sql.Expression{expression.NewStar()},
			plan.NewTableAlias("new", getResolvedTable(n)),
		)
		triggerLogic, err = a.Analyze(ctx, trigger.Body, (*Scope)(nil).newScope(scopeNode).withMemos(scope.memo(n).MemoNodes()))
	case sqlparser.UpdateStr:
		scopeNode := plan.NewProject(
			[]sql.Expression{expression.NewStar()},
			plan.NewCrossJoin(
				plan.NewTableAlias("old", getResolvedTable(n)),
				plan.NewTableAlias("new", getResolvedTable(n)),
			),
		)
		triggerLogic, err = a.Analyze(ctx, trigger.Body, (*Scope)(nil).newScope(scopeNode).withMemos(scope.memo(n).MemoNodes()))
	case sqlparser.DeleteStr:
		scopeNode := plan.NewProject(
			[]sql.Expression{expression.NewStar()},
			plan.NewTableAlias("old", getResolvedTable(n)),
		)
		triggerLogic, err = a.Analyze(ctx, trigger.Body, (*Scope)(nil).newScope(scopeNode).withMemos(scope.memo(n).MemoNodes()))
	}

	return StripPassthroughNodes(triggerLogic), err
}

// validateNoCircularUpdates returns an error if the trigger logic attempts to update the table that invoked it (or any
// table being updated in an outer scope of this analysis)
func validateNoCircularUpdates(trigger *plan.CreateTrigger, n sql.Node, scope *Scope) error {
	var circularRef error
	transform.Inspect(trigger.Body, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.Update, *plan.InsertInto, *plan.DeleteFrom:
			for _, n := range append([]sql.Node{n}, scope.MemoNodes()...) {
				invokingTableName := getUnaliasedTableName(n)
				updatedTable := getUnaliasedTableName(node)
				// TODO: need to compare DB as well
				if updatedTable == invokingTableName {
					circularRef = sql.ErrTriggerTableInUse.New(updatedTable)
					return false
				}
			}
		}
		return true
	})

	return circularRef
}

func orderTriggersAndReverseAfter(triggers []*plan.CreateTrigger) []*plan.CreateTrigger {
	beforeTriggers, afterTriggers := plan.OrderTriggers(triggers)

	// Reverse the order of after triggers. This is because we always apply them to the Insert / Update / Delete node
	// that initiated the trigger, so after triggers, which wrap the Insert, need be applied in reverse order for them to
	// run in the correct order.
	for left, right := 0, len(afterTriggers)-1; left < right; left, right = left+1, right-1 {
		afterTriggers[left], afterTriggers[right] = afterTriggers[right], afterTriggers[left]
	}

	return append(beforeTriggers, afterTriggers...)
}

func triggerEventsMatch(event plan.TriggerEvent, event2 string) bool {
	return strings.ToLower((string)(event)) == strings.ToLower(event2)
}

// wrapWritesWithRollback wraps the entire tree iff it contains a trigger, allowing rollback when a trigger errors
func wrapWritesWithRollback(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	// Check if tree contains a TriggerExecutor
	containsTrigger := false
	transform.Inspect(n, func(n sql.Node) bool {
		// After Triggers wrap nodes
		if _, ok := n.(*plan.TriggerExecutor); ok {
			containsTrigger = true
			return false // done, don't bother to recurse
		}

		// Before Triggers on Inserts are inside Source
		if n, ok := n.(*plan.InsertInto); ok {
			if _, ok := n.Source.(*plan.TriggerExecutor); ok {
				containsTrigger = true
				return false
			}
		}

		// Before Triggers on Delete and Update should be in children
		return true
	})

	// No TriggerExecutor, so return same tree
	if !containsTrigger {
		return n, transform.SameTree, nil
	}

	// No database set, find it through tree
	dbName := ctx.GetCurrentDatabase()
	if dbName == "" {
		transform.Inspect(n, func(n sql.Node) bool {
			switch n := n.(type) {
			case *plan.InsertInto:
				if n.Database() != nil && n.Database().Name() != "" {
					dbName = n.Database().Name()
				}
			case *plan.Update:
				if n.Database() != "" {
					dbName = n.Database()
				}
			case *plan.DeleteFrom:
				if n.Database() != "" {
					dbName = n.Database()
				}
			}
			return true
		})
	}

	// Get current database
	currDb, err := a.Catalog.Database(ctx, dbName)
	if err != nil {
		return nil, transform.SameTree, err
	}

	// Extract from privilegedDatabase
	if privilegedDatabase, ok := currDb.(grant_tables.PrivilegedDatabase); ok {
		currDb = privilegedDatabase.Unwrap()
	}

	// Not a TransactionDatabase, do nothing
	tdb, ok := currDb.(sql.TransactionDatabase)
	if !ok {
		return n, transform.SameTree, err
	}

	// Wrap tree with new node
	return plan.NewTriggerRollback(n, tdb), transform.NewTree, nil
}
