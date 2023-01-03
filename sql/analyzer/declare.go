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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// declarePosition is used to state the last declaration so that they're properly ordered
type declarePosition byte

const (
	declarePosition_VariablesConditions declarePosition = iota // Variable or Condition
	declarePosition_Cursors                                    // Cursor
	declarePosition_Handlers                                   // Handler
	declarePosition_Body                                       // No more declarations should be seen
)

type declarationVariable struct {
	id   int
	name string
	typ  sql.Type
}

type declarationCursor struct {
	id   int
	name string
}

// declarationScopeValidation is used to validate DECLARE statements by ensuring that all scope-related references are
// valid.
type declarationScopeValidation struct {
	parent     *declarationScopeValidation
	conditions map[string]*plan.DeclareCondition
	variables  map[string]struct{}
	cursors    map[string]struct{}
	labels     map[string]bool

	//TODO: implement proper handler support
	handler    *plan.DeclareHandler
	handlerIds []int
}

// newDeclarationScopeValidation returns a *declarationScopeValidation.
func newDeclarationScopeValidation() *declarationScopeValidation {
	return &declarationScopeValidation{
		parent:     nil,
		conditions: make(map[string]*plan.DeclareCondition),
		variables:  make(map[string]struct{}),
		cursors:    make(map[string]struct{}),
		labels:     make(map[string]bool),
	}
}

// AddCondition adds a condition to the scope at the given scope's depth. Returns an error if a condition with the name already
// exists.
func (d *declarationScopeValidation) AddCondition(condition *plan.DeclareCondition) error {
	name := strings.ToLower(condition.Name)
	if _, ok := d.conditions[name]; ok {
		return sql.ErrDeclareConditionDuplicate.New(condition.Name)
	}
	d.conditions[name] = condition
	return nil
}

// AddVariable adds a variable to the current scope. Returns an error if a variable with the same name already exists.
func (d *declarationScopeValidation) AddVariable(name string) error {
	lowerName := strings.ToLower(name)
	if _, ok := d.variables[lowerName]; ok {
		return sql.ErrDeclareVariableDuplicate.New(name)
	}
	d.variables[lowerName] = struct{}{}
	return nil
}

// AddCursor adds a cursor to the current scope. Returns an error if a cursor with the same name already exists.
func (d *declarationScopeValidation) AddCursor(name string) error {
	lowerName := strings.ToLower(name)
	if _, ok := d.cursors[lowerName]; ok {
		return sql.ErrDeclareCursorDuplicate.New(name)
	}
	d.cursors[lowerName] = struct{}{}
	return nil
}

// AddHandler adds a handler to the current scope. Returns an error if a duplicate handler exists.
func (d *declarationScopeValidation) AddHandler(handler *plan.DeclareHandler) error {
	if d.handler != nil {
		return sql.ErrDeclareHandlerDuplicate.New()
	}
	d.handler = handler
	return nil
}

// AddLabel adds a label to the current scope. Returns an error if a label with the same name already exists.
func (d *declarationScopeValidation) AddLabel(label string, isLoop bool) error {
	// Empty labels are not added since they cannot be referenced
	if len(label) == 0 {
		return nil
	}
	lowercaseLabel := strings.ToLower(label)
	if _, ok := d.labels[lowercaseLabel]; ok {
		return sql.ErrLoopRedefinition.New(label)
	}
	d.labels[lowercaseLabel] = isLoop
	return nil
}

// GetCondition returns the condition from the scope. If the condition is not found in the current scope, then walks up
// the parent until it is found. Returns nil if not found.
func (d *declarationScopeValidation) GetCondition(name string) *plan.DeclareCondition {
	return d.getCondition(strings.ToLower(name))
}

// getCondition is the recursive implementation of GetCondition.
func (d *declarationScopeValidation) getCondition(name string) *plan.DeclareCondition {
	if d == nil {
		return nil
	}
	if dc, ok := d.conditions[name]; ok {
		return dc
	}
	return d.parent.getCondition(name)
}

// HasVariable returns true if the given variable is reachable.
func (d *declarationScopeValidation) HasVariable(name string) bool {
	return d.hasVariable(strings.ToLower(name))
}

// hasVariable is the recursive implementation of HasVariable.
func (d *declarationScopeValidation) hasVariable(name string) bool {
	if d == nil {
		return false
	}
	if _, ok := d.variables[name]; ok {
		return true
	}
	return d.parent.hasVariable(name)
}

// HasCursor returns true if the given cursor is reachable.
func (d *declarationScopeValidation) HasCursor(name string) bool {
	return d.hasCursor(strings.ToLower(name))
}

// hasCursor is the recursive implementation of HasCursor.
func (d *declarationScopeValidation) hasCursor(name string) bool {
	if d == nil {
		return false
	}
	if _, ok := d.cursors[name]; ok {
		return true
	}
	return d.parent.hasCursor(name)
}

// HasLabel returns true if the given label is reachable. Also returns whether the label represents a loop.
func (d *declarationScopeValidation) HasLabel(label string) (exists bool, isLoop bool) {
	return d.hasLabel(strings.ToLower(label))
}

// hasLabel is the recursive implementation of HasLabel.
func (d *declarationScopeValidation) hasLabel(label string) (exists bool, isLoop bool) {
	if d == nil {
		return false, false
	}
	if isLoop, ok := d.labels[label]; ok {
		return true, isLoop
	}
	return d.parent.hasLabel(label)
}

// RemoveLabel removes a label from the current scope.
func (d *declarationScopeValidation) RemoveLabel(label string) error {
	// Ignore trying to remove empty labels
	if len(label) == 0 {
		return nil
	}
	lowercaseLabel := strings.ToLower(label)
	if _, ok := d.labels[lowercaseLabel]; !ok {
		// This should never be hit
		return fmt.Errorf("label '%s' could not be found for removal", label)
	}
	delete(d.labels, lowercaseLabel)
	return nil
}

// Child returns a new *declarationScopeValidation that represents an inner scope.
func (d *declarationScopeValidation) Child() *declarationScopeValidation {
	return &declarationScopeValidation{
		parent:     d,
		conditions: make(map[string]*plan.DeclareCondition),
		variables:  make(map[string]struct{}),
		cursors:    make(map[string]struct{}),
		labels:     make(map[string]bool),
	}
}

// resolveDeclarations handles all Declare nodes, ensuring correct node order and assigning variables and conditions to
// their appropriate references.
func resolveDeclarations(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	// First scope houses the parameters
	scopeValidation := newDeclarationScopeValidation()
	proc, ok := node.(*plan.Procedure)
	if !ok {
		if cp, innerOk := node.(*plan.CreateProcedure); innerOk {
			proc = cp.Procedure
			ok = true
		}
	}
	if !ok {
		if _, ok = node.(*plan.TriggerBeginEndBlock); !ok {
			return node, transform.SameTree, nil
		}
	} else {
		for _, param := range proc.Params {
			if err := scopeValidation.AddVariable(param.Name); err != nil {
				return nil, transform.SameTree, err
			}
		}
	}
	// Second scope houses the first set of declared variables
	resolvedNode, identity, err := resolveDeclarationsInner(ctx, a, node, scopeValidation.Child(), sel)
	if err != nil {
		return nil, identity, err
	}
	return resolvedNode, identity, nil
}

func resolveDeclarationsInner(ctx *sql.Context, a *Analyzer, node sql.Node, scope *declarationScopeValidation, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	children := node.Children()
	if len(children) == 0 {
		return node, transform.SameTree, nil
	}
	// First pass checks for order and assigns to scope
	_, representsScope := node.(plan.RepresentsScope)
	if representsScope {
		// Documentation on the ordering of DECLARE statements.
		// BEGIN/END is treated specially for scope regarding DECLARE statements.
		// https://dev.mysql.com/doc/refman/8.0/en/declare.html
		lastPosition := declarePosition_VariablesConditions
		for _, child := range children {
			switch child := child.(type) {
			case *plan.DeclareCondition:
				if lastPosition > declarePosition_VariablesConditions {
					return nil, transform.SameTree, sql.ErrDeclareConditionOrderInvalid.New()
				}
				lastPosition = declarePosition_VariablesConditions
				if err := scope.AddCondition(child); err != nil {
					return nil, transform.SameTree, err
				}
			case *plan.DeclareCursor:
				if lastPosition > declarePosition_Cursors {
					return nil, transform.SameTree, sql.ErrDeclareCursorOrderInvalid.New()
				}
				lastPosition = declarePosition_Cursors
				if err := scope.AddCursor(child.Name); err != nil {
					return nil, transform.SameTree, err
				}
			case *plan.DeclareHandler:
				if lastPosition > declarePosition_Handlers {
					return nil, transform.SameTree, sql.ErrDeclareHandlerOrderInvalid.New()
				}
				lastPosition = declarePosition_Handlers
				if err := scope.AddHandler(child); err != nil {
					return nil, transform.SameTree, err
				}
			case *plan.DeclareVariables:
				if lastPosition > declarePosition_VariablesConditions {
					return nil, transform.SameTree, sql.ErrDeclareVariableOrderInvalid.New()
				}
				lastPosition = declarePosition_VariablesConditions
				for _, varName := range child.Names {
					if err := scope.AddVariable(varName); err != nil {
						return nil, transform.SameTree, err
					}
				}
			default:
				lastPosition = declarePosition_Body
			}
		}
	} else {
		for _, child := range children {
			switch child.(type) {
			case *plan.DeclareCondition:
				return nil, transform.SameTree, sql.ErrDeclareConditionOrderInvalid.New()
			case *plan.DeclareCursor:
				return nil, transform.SameTree, sql.ErrDeclareCursorOrderInvalid.New()
			case *plan.DeclareHandler:
				return nil, transform.SameTree, sql.ErrDeclareHandlerOrderInvalid.New()
			case *plan.DeclareVariables:
				return nil, transform.SameTree, sql.ErrDeclareVariableOrderInvalid.New()
			}
		}
	}

	var (
		child       sql.Node
		newChild    sql.Node
		err         error
		newChildren []sql.Node
		same        transform.TreeIdentity
	)

	for i := 0; i < len(children); i++ {
		same = transform.SameTree
		child = children[i]
		if scopeBlock, ok := child.(plan.RepresentsScope); ok {
			childScope := scope.Child()
			if labelBlock, ok := child.(plan.RepresentsLabeledBlock); ok {
				if err = childScope.AddLabel(labelBlock.GetBlockLabel(ctx), labelBlock.RepresentsLoop()); err != nil {
					return nil, transform.SameTree, err
				}
			}
			if newChild, same, err = resolveProcedureChild(ctx, a, scopeBlock, childScope, sel); err != nil {
				return nil, transform.SameTree, err
			}
		} else if labelBlock, ok := child.(plan.RepresentsLabeledBlock); ok {
			if err = scope.AddLabel(labelBlock.GetBlockLabel(ctx), labelBlock.RepresentsLoop()); err != nil {
				return nil, transform.SameTree, err
			}
			if newChild, same, err = resolveProcedureChild(ctx, a, labelBlock, scope, sel); err != nil {
				return nil, transform.SameTree, err
			}
			if err = scope.RemoveLabel(labelBlock.GetBlockLabel(ctx)); err != nil {
				return nil, transform.SameTree, err
			}
		} else {
			switch c := child.(type) {
			case *plan.Leave:
				if exists, _ := scope.HasLabel(c.Label); !exists {
					return nil, transform.SameTree, sql.ErrLoopLabelNotFound.New("LEAVE", c.Label)
				}
			case *plan.Iterate:
				//TODO: if this is within a DECLARE ... HANDLER block, then this should be unable to get outer labels
				if exists, isLoop := scope.HasLabel(c.Label); !exists || !isLoop {
					return nil, transform.SameTree, sql.ErrLoopLabelNotFound.New("ITERATE", c.Label)
				}
			case *plan.SignalName:
				condition := scope.GetCondition(c.Name)
				if condition == nil {
					return nil, transform.SameTree, sql.ErrDeclareConditionNotFound.New(c.Name)
				}
				if condition.SqlStateValue == "" {
					return nil, transform.SameTree, sql.ErrSignalOnlySqlState.New()
				}
				newChild = plan.NewSignal(condition.SqlStateValue, c.Signal.Info)
				same = transform.NewTree
			case *plan.Open:
				if !scope.HasCursor(c.Name) {
					return nil, transform.SameTree, sql.ErrCursorNotFound.New(c.Name)
				}
			case *plan.Close:
				if !scope.HasCursor(c.Name) {
					return nil, transform.SameTree, sql.ErrCursorNotFound.New(c.Name)
				}
			case *plan.Fetch:
				if !scope.HasCursor(c.Name) {
					return nil, transform.SameTree, sql.ErrCursorNotFound.New(c.Name)
				}
				if newChild, same, err = resolveProcedureVariables(ctx, scope, c); err != nil {
					return nil, transform.SameTree, err
				}
			case plan.DisjointedChildrenNode:
				disjointedChildGroups := c.DisjointedChildren()
				newDisjointedChildGroups := make([][]sql.Node, len(disjointedChildGroups))
				for groupIdx, disjointedChildGroup := range disjointedChildGroups {
					newDisjointedChildGroups[groupIdx] = make([]sql.Node, len(disjointedChildGroup))
					for childIdx, disjointedChild := range disjointedChildGroup {
						var childIdentity transform.TreeIdentity
						if newDisjointedChildGroups[groupIdx][childIdx], childIdentity, err = resolveProcedureChild(ctx, a, disjointedChild, scope, sel); err != nil {
							return nil, transform.SameTree, err
						} else if childIdentity == transform.NewTree {
							same = childIdentity
						}
					}
				}
				if same == transform.NewTree {
					if newChild, err = c.WithDisjointedChildren(newDisjointedChildGroups); err != nil {
						return nil, transform.SameTree, err
					}
				}
			default:
				if newChild, same, err = resolveProcedureChild(ctx, a, c, scope, sel); err != nil {
					return nil, transform.SameTree, err
				}
			}
		}
		if !same {
			if newChildren == nil {
				newChildren = make([]sql.Node, len(children))
				copy(newChildren, children)
			}
			newChildren[i] = newChild
		}
	}

	if len(newChildren) > 0 {
		node, err = node.WithChildren(newChildren...)
		if err != nil {
			return nil, transform.SameTree, err
		}
		return node, transform.NewTree, nil
	}

	return node, transform.SameTree, nil
}

// resolveProcedureChild resolves the expressions that are directly on a child node, along with expressions in the node's children.
func resolveProcedureChild(ctx *sql.Context, a *Analyzer, node sql.Node, scope *declarationScopeValidation, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	var newChild sql.Node
	var identity1 transform.TreeIdentity
	var identity2 transform.TreeIdentity
	var err error
	newChild, identity1, err = resolveProcedureVariables(ctx, scope, node)
	if err != nil {
		return nil, transform.SameTree, err
	}
	newChild, identity2, err = resolveDeclarationsInner(ctx, a, newChild, scope, sel)
	if err != nil {
		return nil, transform.SameTree, err
	}
	if identity1 == transform.NewTree || identity2 == transform.NewTree {
		return newChild, transform.NewTree, nil
	}
	return node, transform.SameTree, nil
}

// resolveProcedureVariables resolves all named parameters and declared variables in a node.
func resolveProcedureVariables(ctx *sql.Context, scope *declarationScopeValidation, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeExprsWithOpaque(n, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		case *expression.UnresolvedColumn:
			if strings.ToLower(e.Table()) == "" {
				if scope.HasVariable(e.Name()) {
					return expression.NewProcedureParam(e.Name()), transform.NewTree, nil
				}
			}
			return e, transform.SameTree, nil
		case *deferredColumn:
			if strings.ToLower(e.Table()) == "" {
				if scope.HasVariable(e.Name()) {
					return expression.NewProcedureParam(e.Name()), transform.NewTree, nil
				}
			}
			return e, transform.SameTree, nil
		case *expression.UnresolvedProcedureParam:
			if scope.HasVariable(e.Name()) {
				return expression.NewProcedureParam(e.Name()), transform.NewTree, nil
			}
			return e, transform.SameTree, nil
		case sql.ExpressionWithNodes:
			children := e.NodeChildren()
			var newChildren []sql.Node
			for i, child := range children {
				newChild, same, err := resolveProcedureVariables(ctx, scope, child)
				if err != nil {
					return nil, transform.SameTree, err
				}
				if same == transform.NewTree {
					if newChildren == nil {
						newChildren = make([]sql.Node, len(children))
						copy(newChildren, children)
					}
					newChildren[i] = newChild
				}
			}
			if len(newChildren) > 0 {
				newExpr, err := e.WithNodeChildren(newChildren...)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return newExpr, transform.NewTree, nil
			}
			return e, transform.SameTree, nil
		default:
			return e, transform.SameTree, nil
		}
	})
}
