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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// declarationScope holds the scope of DECLARE statements relative to their depth in the plan tree.
type declarationScope struct {
	parent     *declarationScope
	conditions map[string]*plan.DeclareCondition
}

// newDeclarationScope returns a *declarationScope.
func newDeclarationScope(parent *declarationScope) *declarationScope {
	return &declarationScope{
		parent:     parent,
		conditions: make(map[string]*plan.DeclareCondition),
	}
}

// AddCondition adds a condition to the scope at the given depth. Returns an error if a condition with the name already
// exists.
func (d *declarationScope) AddCondition(condition *plan.DeclareCondition) error {
	name := strings.ToLower(condition.Name)
	if _, ok := d.conditions[name]; ok {
		return sql.ErrDeclareConditionDuplicate.New(condition.Name)
	}
	d.conditions[name] = condition
	return nil
}

// GetCondition returns the condition from the scope. If the condition is not found in the current scope, then walks
// up the parent until it is found. Returns a bool regarding whether it was found.
func (d *declarationScope) GetCondition(name string) *plan.DeclareCondition {
	return d.getCondition(strings.ToLower(name))
}

// getCondition is the recursive implementation of GetCondition.
func (d *declarationScope) getCondition(name string) *plan.DeclareCondition {
	if d == nil {
		return nil
	}
	if dc, ok := d.conditions[name]; ok {
		return dc
	}
	return d.parent.getCondition(name)
}

// resolveDeclarations handles all Declare nodes, ensuring correct node order and assigning variables and conditions to
// their appropriate references.
func resolveDeclarations(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (sql.Node, error) {
	return resolveDeclarationsInner(ctx, a, node, newDeclarationScope(nil))
}

func resolveDeclarationsInner(ctx *sql.Context, a *Analyzer, node sql.Node, scope *declarationScope) (sql.Node, error) {
	children := node.Children()
	if len(children) == 0 {
		return node, nil
	}
	newChildren := make([]sql.Node, len(children))
	// First pass checks for order and assigns to scope
	isBeginEnd := false
	switch node.(type) {
	case *plan.BeginEndBlock, *plan.TriggerBeginEndBlock:
		isBeginEnd = true
	}
	if isBeginEnd {
		// Documentation on the ordering of DECLARE statements.
		// BEGIN/END is treated specially for scope regarding DECLARE statements.
		// https://dev.mysql.com/doc/refman/8.0/en/declare.html
		lastStatementDeclare := true
		for _, child := range children {
			switch child := child.(type) {
			case *plan.DeclareCondition:
				if !lastStatementDeclare {
					return nil, sql.ErrDeclareOrderInvalid.New()
				}
				if err := scope.AddCondition(child); err != nil {
					return nil, err
				}
			default:
				lastStatementDeclare = false
			}
		}
	} else {
		for _, child := range children {
			switch child.(type) {
			case *plan.DeclareCondition:
				return nil, sql.ErrDeclareOrderInvalid.New()
			}
		}
	}
	for i, child := range children {
		var newChild sql.Node
		var err error
		switch child := child.(type) {
		case *plan.Procedure, *plan.Block, *plan.IfElseBlock, *plan.IfConditional:
			newChild, err = resolveDeclarationsInner(ctx, a, child, scope)
		case *plan.BeginEndBlock, *plan.TriggerBeginEndBlock:
			newChild, err = resolveDeclarationsInner(ctx, a, child, newDeclarationScope(scope))
		case *plan.SignalName:
			condition := scope.GetCondition(child.Name)
			if condition == nil {
				return nil, sql.ErrDeclareConditionNotFound.New(child.Name)
			}
			if condition.SqlStateValue == "" {
				return nil, sql.ErrSignalOnlySqlState.New()
			}
			newChild = plan.NewSignal(condition.SqlStateValue, child.Signal.Info)
		default:
			newChild = child
		}
		if err != nil {
			return nil, err
		}
		newChildren[i] = newChild
	}
	return node.WithChildren(newChildren...)
}
