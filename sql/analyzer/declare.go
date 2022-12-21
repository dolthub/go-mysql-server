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

// declarationScope holds the scope of DECLARE statements relative to their depth in the plan tree.
type declarationScope struct {
	nextVarId  *int
	nextCurId  *int
	nextHandId *int
	parent     *declarationScope
	conditions map[string]*plan.DeclareCondition
	variables  map[string]*declarationVariable
	cursors    map[string]*declarationCursor
	loops      map[string]struct{}

	//TODO: implement proper handler support
	handler    *plan.DeclareHandler
	handlerIds []int
}

// newDeclarationScope returns a *declarationScope.
func newDeclarationScope(parent *declarationScope) *declarationScope {
	var nextVarId *int
	var nextCurId *int
	var nextHandId *int
	if parent != nil {
		nextVarId = parent.nextVarId
		nextCurId = parent.nextCurId
		nextHandId = parent.nextHandId
	} else {
		nextVarId = new(int)
		nextCurId = new(int)
		nextHandId = new(int)
	}
	return &declarationScope{
		nextVarId:  nextVarId,
		nextCurId:  nextCurId,
		nextHandId: nextHandId,
		parent:     parent,
		conditions: make(map[string]*plan.DeclareCondition),
		variables:  make(map[string]*declarationVariable),
		cursors:    make(map[string]*declarationCursor),
		loops:      make(map[string]struct{}),
	}
}

// AddCondition adds a condition to the scope at the given scope's depth. Returns an error if a condition with the name already
// exists.
func (d *declarationScope) AddCondition(condition *plan.DeclareCondition) error {
	name := strings.ToLower(condition.Name)
	if _, ok := d.conditions[name]; ok {
		return sql.ErrDeclareConditionDuplicate.New(condition.Name)
	}
	d.conditions[name] = condition
	return nil
}

// AddVariable adds a variable to the scope at the given scope's depth. Returns the added variable's ID, or an error if
// a variable with the name already exists.
func (d *declarationScope) AddVariable(name string, typ sql.Type) (int, error) {
	lowerName := strings.ToLower(name)
	if _, ok := d.variables[lowerName]; ok {
		return 0, sql.ErrDeclareVariableDuplicate.New(name)
	}
	d.variables[lowerName] = &declarationVariable{
		id:   *d.nextVarId,
		name: name,
		typ:  typ,
	}
	*d.nextVarId++
	return *d.nextVarId - 1, nil
}

// AddCursor adds a cursor to the scope at the given scope's depth. Returns the added cursor's ID, or an error if a
// cursor with the name already exists.
func (d *declarationScope) AddCursor(name string) (int, error) {
	lowerName := strings.ToLower(name)
	if _, ok := d.cursors[lowerName]; ok {
		return 0, sql.ErrDeclareCursorDuplicate.New(name)
	}
	d.cursors[lowerName] = &declarationCursor{
		id:   *d.nextCurId,
		name: name,
	}
	*d.nextCurId++
	return *d.nextCurId - 1, nil
}

// AddHandler adds the given handler to the scope. Errors if a duplicate handler exists.
func (d *declarationScope) AddHandler(handler *plan.DeclareHandler) (int, error) {
	if d.handler != nil {
		return 0, sql.ErrDeclareHandlerDuplicate.New()
	}
	d.handler = handler
	d.handlerIds = append(d.handlerIds, *d.nextHandId)
	*d.nextHandId++
	return *d.nextHandId - 1, nil
}

// AddLoop adds a loop with the given label to the current scope.
func (d *declarationScope) AddLoop(label string) error {
	lowercaseLabel := strings.ToLower(label)
	if _, ok := d.loops[lowercaseLabel]; ok {
		return sql.ErrLoopRedefinition.New(label)
	}
	d.loops[lowercaseLabel] = struct{}{}
	return nil
}

// GetCondition returns the condition from the scope. If the condition is not found in the current scope, then walks up
// the parent until it is found. Returns nil if not found.
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

// GetVariable returns the variable from the scope. If the variable is not found in the current scope, then walks up the
// parent until it is found. Returns nil if not found.
func (d *declarationScope) GetVariable(name string) *declarationVariable {
	return d.getVariable(strings.ToLower(name))
}

// getVariable is the recursive implementation of GetVariable.
func (d *declarationScope) getVariable(name string) *declarationVariable {
	if d == nil {
		return nil
	}
	if dv, ok := d.variables[name]; ok {
		return dv
	}
	return d.parent.getVariable(name)
}

// GetCursor returns the cursor from the scope. If the cursor is not found in the current scope, then walks up the
// parent until it is found. Returns nil if not found.
func (d *declarationScope) GetCursor(name string) *declarationCursor {
	return d.getCursor(strings.ToLower(name))
}

// getCursor is the recursive implementation of GetCursor.
func (d *declarationScope) getCursor(name string) *declarationCursor {
	if d == nil {
		return nil
	}
	if dc, ok := d.cursors[name]; ok {
		return dc
	}
	return d.parent.getCursor(name)
}

// GetAllReachableHandlerIds returns all *plan.DeclareHandler IDs that are reachable from the current scope. Handlers are sorted by
// lowest in the scope's stack.
func (d *declarationScope) GetAllReachableHandlerIds() []int {
	return d.getAllReachableHandlerIds()
}

// getAllReachableHandlerIds is the recursive implementation of GetAllReachableHandlerIds.
func (d *declarationScope) getAllReachableHandlerIds() []int {
	if d == nil {
		return nil
	}
	return append(d.parent.getAllReachableHandlerIds(), d.handlerIds...)
}

// GetCurrentScopeHandlerIds returns all *plan.DeclareHandler IDs that are declared on the current scope.
func (d *declarationScope) GetCurrentScopeHandlerIds() []int {
	return append([]int{}, d.handlerIds...)
}

// HasLoop returns whether the loop exists in the current scope only. Loops that are in scopes above the current one may
// not be queried.
func (d *declarationScope) HasLoop(label string) bool {
	_, ok := d.loops[strings.ToLower(label)]
	return ok
}

// RemoveLoop removes a loop with the given label from the current scope.
func (d *declarationScope) RemoveLoop(label string) error {
	lowercaseLabel := strings.ToLower(label)
	if _, ok := d.loops[lowercaseLabel]; !ok {
		// This should never be hit
		return fmt.Errorf("loop '%s' could not be found in the current scope for removal", label)
	}
	delete(d.loops, lowercaseLabel)
	return nil
}

// VariableCount returns the total number of parameters and variables.
func (d *declarationScope) VariableCount() int {
	return *d.nextVarId
}

// CursorCount returns the total number of cursors.
func (d *declarationScope) CursorCount() int {
	return *d.nextCurId
}

// HandlerCount returns the total number of handlers.
func (d *declarationScope) HandlerCount() int {
	return *d.nextHandId
}

// resolveDeclarations handles all Declare nodes, ensuring correct node order and assigning variables and conditions to
// their appropriate references.
func resolveDeclarations(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	// First scope houses the parameters
	declScope := newDeclarationScope(nil)
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
			if _, err := declScope.AddVariable(param.Name, param.Type); err != nil {
				return nil, transform.SameTree, err
			}
		}
	}
	// Second scope houses the first set of declared variables
	resolvedNode, identity, err := resolveDeclarationsInner(ctx, a, node, newDeclarationScope(declScope), sel)
	if err != nil {
		return nil, identity, err
	}
	if resolvedProc, ok := resolvedNode.(*plan.Procedure); ok {
		resolvedProc.VariableCount = declScope.VariableCount()
		resolvedProc.CursorCount = declScope.CursorCount()
		resolvedProc.HandlerCount = declScope.HandlerCount()
	} else if resolvedCProc, ok := resolvedNode.(*plan.CreateProcedure); ok {
		resolvedCProc.VariableCount = declScope.VariableCount()
		resolvedCProc.CursorCount = declScope.CursorCount()
		resolvedCProc.HandlerCount = declScope.HandlerCount()
	}
	return resolvedNode, identity, nil
}

func resolveDeclarationsInner(ctx *sql.Context, a *Analyzer, node sql.Node, scope *declarationScope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	same := transform.SameTree
	var newChildren []sql.Node

	children := node.Children()
	if len(children) == 0 {
		return node, transform.SameTree, nil
	}
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
		lastPosition := declarePosition_VariablesConditions
		for i, child := range children {
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
				id, err := scope.AddCursor(child.Name)
				if err != nil {
					return nil, transform.SameTree, err
				}
				same = false
				if newChildren == nil {
					newChildren = make([]sql.Node, len(children))
					copy(newChildren, children)
				}
				newChildren[i] = child.WithId(id)
			case *plan.DeclareHandler:
				if lastPosition > declarePosition_Handlers {
					return nil, transform.SameTree, sql.ErrDeclareHandlerOrderInvalid.New()
				}
				lastPosition = declarePosition_Handlers
				id, err := scope.AddHandler(child)
				if err != nil {
					return nil, transform.SameTree, err
				}
				same = false
				if newChildren == nil {
					newChildren = make([]sql.Node, len(children))
					copy(newChildren, children)
				}
				newChildren[i] = child.WithId(id)
			case *plan.DeclareVariables:
				if lastPosition > declarePosition_VariablesConditions {
					return nil, transform.SameTree, sql.ErrDeclareVariableOrderInvalid.New()
				}
				lastPosition = declarePosition_VariablesConditions
				var err error
				ids := make([]int, len(child.Names))
				for nameIdx, name := range child.Names {
					if ids[nameIdx], err = scope.AddVariable(name, child.Type); err != nil {
						return nil, transform.SameTree, err
					}
				}
				same = false
				if newChildren == nil {
					newChildren = make([]sql.Node, len(children))
					copy(newChildren, children)
				}
				newChildren[i], err = child.WithIds(ctx, ids)
				if err != nil {
					return nil, transform.SameTree, err
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
	if len(newChildren) > 0 {
		children = newChildren
	}

	var (
		child    sql.Node
		newChild sql.Node
		err      error
	)

	for i := 0; i < len(children); i++ {
		child = children[i]
		switch c := child.(type) {
		case *plan.Procedure, *plan.Block, *plan.IfElseBlock:
			newChild, same, err = resolveDeclarationsInner(ctx, a, child, scope, sel)
			if err != nil {
				return nil, transform.SameTree, err
			}
		case *plan.BeginEndBlock:
			blockScope := newDeclarationScope(scope)
			newChild, same, err = resolveDeclarationsInner(ctx, a, child, blockScope, sel)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if handlerIds := blockScope.GetCurrentScopeHandlerIds(); len(handlerIds) > 0 {
				newChild = newChild.(*plan.BeginEndBlock).WithHandlerIds(handlerIds)
				same = transform.NewTree
			}
		case *plan.TriggerBeginEndBlock:
			newChild, same, err = resolveDeclarationsInner(ctx, a, child, newDeclarationScope(scope), sel)
			if err != nil {
				return nil, transform.SameTree, err
			}
		case *plan.IfConditional:
			var identity1 transform.TreeIdentity
			var identity2 transform.TreeIdentity
			newChild, identity1, err = resolveDeclarationsInner(ctx, a, child, scope, sel)
			if err != nil {
				return nil, transform.SameTree, err
			}
			newChild, identity2, err = resolveProcedureVariables(ctx, scope, newChild)
			if err != nil {
				return nil, transform.SameTree, err
			}
			same = identity1 && identity2
		case *plan.Loop:
			err = scope.AddLoop(c.Label)
			if err != nil {
				return nil, transform.SameTree, err
			}
			newChild, same, err = resolveDeclarationsInner(ctx, a, child, scope, sel)
			if err != nil {
				return nil, transform.SameTree, err
			}
			err = scope.RemoveLoop(c.Label)
			if err != nil {
				return nil, transform.SameTree, err
			}
		case *plan.Leave:
			if !scope.HasLoop(c.Label) {
				return nil, transform.SameTree, sql.ErrLoopLabelNotFound.New("LEAVE", c.Label)
			}
			same = transform.SameTree
		case *plan.Iterate:
			if !scope.HasLoop(c.Label) {
				return nil, transform.SameTree, sql.ErrLoopLabelNotFound.New("ITERATE", c.Label)
			}
			same = transform.SameTree
		case *plan.SignalName:
			condition := scope.GetCondition(c.Name)
			if condition == nil {
				return nil, transform.SameTree, sql.ErrDeclareConditionNotFound.New(c.Name)
			}
			if condition.SqlStateValue == "" {
				return nil, transform.SameTree, sql.ErrSignalOnlySqlState.New()
			}
			newChild = plan.NewSignal(condition.SqlStateValue, c.Signal.Info)
			same = false
		case *plan.Open:
			cursor := scope.GetCursor(c.Name)
			if cursor == nil {
				return nil, transform.SameTree, sql.ErrCursorNotFound.New(c.Name)
			}
			newChild = c.WithId(cursor.id)
			same = false
		case *plan.Close:
			cursor := scope.GetCursor(c.Name)
			if cursor == nil {
				return nil, transform.SameTree, sql.ErrCursorNotFound.New(c.Name)
			}
			newChild = c.WithId(cursor.id)
			same = false
		case *plan.Fetch:
			cursor := scope.GetCursor(c.Name)
			if cursor == nil {
				return nil, transform.SameTree, sql.ErrCursorNotFound.New(c.Name)
			}
			newChild, _, err = resolveProcedureVariables(ctx, scope, c.WithId(cursor.id).WithHandlerIds(scope.GetAllReachableHandlerIds()))
			if err != nil {
				return nil, transform.SameTree, err
			}
			same = false
		case *plan.InsertInto:
			newSource, sourceIdentity, err := resolveProcedureChild(ctx, a, c.Source, scope, sel)
			if err != nil {
				return nil, transform.SameTree, err
			}
			newChild, same, err = resolveProcedureChild(ctx, a, c, scope, sel)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if sourceIdentity == transform.NewTree {
				newChild = newChild.(*plan.InsertInto).WithSource(newSource)
				same = transform.NewTree
			}
		case *plan.Union:
			// todo(max): IndexedJoins might be missed here
			newLeft, identityL, err := resolveProcedureVariables(ctx, scope, c.Left())
			if err != nil {
				return nil, transform.SameTree, err
			}
			newRight, identityR, err := resolveProcedureVariables(ctx, scope, c.Right())
			if err != nil {
				return nil, transform.SameTree, err
			}
			if identityL == transform.NewTree || identityR == transform.NewTree {
				same = false
				newChild, err = c.WithChildren(newLeft, newRight)
				if err != nil {
					return nil, transform.SameTree, err
				}
			}
		default:
			newChild, same, err = resolveProcedureChild(ctx, a, c, scope, sel)
			if err != nil {
				return nil, transform.SameTree, err
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
func resolveProcedureChild(ctx *sql.Context, a *Analyzer, node sql.Node, scope *declarationScope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
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
func resolveProcedureVariables(ctx *sql.Context, scope *declarationScope, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
	exprFunc := func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		case *expression.UnresolvedColumn:
			if strings.ToLower(e.Table()) == "" {
				if scopedVar := scope.GetVariable(strings.ToLower(e.Name())); scopedVar != nil {
					return expression.NewProcedureParam(scopedVar.id, scopedVar.name), transform.NewTree, nil
				}
			}
			return e, transform.SameTree, nil
		case *deferredColumn:
			if strings.ToLower(e.Table()) == "" {
				if scopedVar := scope.GetVariable(strings.ToLower(e.Name())); scopedVar != nil {
					return expression.NewProcedureParam(scopedVar.id, scopedVar.name), transform.NewTree, nil
				}
			}
			return e, transform.SameTree, nil
		case *expression.UnresolvedProcedureParam:
			if scopedVar := scope.GetVariable(strings.ToLower(e.Name())); scopedVar != nil {
				return expression.NewProcedureParam(scopedVar.id, scopedVar.name), transform.NewTree, nil
			}
			return e, transform.SameTree, nil
		case *plan.Subquery: // Subqueries have an internal Query node that we need to check as well.
			newQuery, same, err := resolveProcedureVariables(ctx, scope, e.Query)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if same {
				return e, transform.SameTree, nil
			}
			ne := *e
			ne.Query = newQuery
			return &ne, transform.NewTree, nil
		default:
			return e, transform.SameTree, nil
		}
	}

	if w, ok := n.(*plan.With); ok {
		cteSame := transform.SameTree
		for i, cte := range w.CTEs {
			newChild, same, err := resolveProcedureVariables(ctx, scope, cte.Subquery.Child)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if same {
				continue
			}
			cteSame = transform.NewTree
			newSubquery, err := cte.Subquery.WithChildren(newChild)
			if err != nil {
				return nil, transform.SameTree, err
			}
			w.CTEs[i].Subquery = newSubquery.(*plan.SubqueryAlias)
		}
		newW, same, err := transform.NodeExprs(w, exprFunc)
		if err != nil {
			return nil, transform.SameTree, err
		}

		return newW, cteSame && same, nil
	}

	return transform.NodeExprs(n, exprFunc)
}
