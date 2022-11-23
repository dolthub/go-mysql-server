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

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// loadStoredProcedures loads non-built-in stored procedures for all databases on relevant calls.
func loadStoredProcedures(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (*Scope, error) {
	if scope.proceduresPopulating() {
		return scope, nil
	}
	referencesProcedures := hasProcedureCall(n)
	if !referencesProcedures {
		return scope, nil
	}
	scope = scope.withProcedureCache(NewProcedureCache())
	scope.procedures.IsPopulating = true
	defer func() {
		scope.procedures.IsPopulating = false
	}()

	allDatabases := a.Catalog.AllDatabases(ctx)
	for _, database := range allDatabases {
		if pdb, ok := database.(sql.StoredProcedureDatabase); ok {
			procedures, err := pdb.GetStoredProcedures(ctx)
			if err != nil {
				return nil, err
			}

			for _, procedure := range procedures {
				parsedProcedure, err := parse.Parse(ctx, procedure.CreateStatement)
				if err != nil {
					return nil, err
				}
				cp, ok := parsedProcedure.(*plan.CreateProcedure)
				if !ok {
					return nil, sql.ErrProcedureCreateStatementInvalid.New(procedure.CreateStatement)
				}

				var procToRegister *plan.Procedure
				analyzedProc, err := analyzeCreateProcedure(ctx, a, cp, scope, sel)
				if err != nil {
					procToRegister = cp.Procedure
					procToRegister.ValidationError = err
				} else {
					procToRegister = analyzedProc
				}

				err = scope.procedures.Register(database.Name(), procToRegister)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return scope, nil
}

// analyzeCreateProcedure checks the plan.CreateProcedure and returns a valid plan.Procedure or an error
func analyzeCreateProcedure(ctx *sql.Context, a *Analyzer, cp *plan.CreateProcedure, scope *Scope, sel RuleSelector) (*plan.Procedure, error) {
	paramNames, err := validateStoredProcedure(ctx, cp.Procedure)
	if err != nil {
		return nil, err
	}
	var analyzedNode sql.Node
	analyzedNode, _, err = resolveDeclarations(ctx, a, cp.Procedure, scope, sel)
	if err != nil {
		return nil, err
	}
	analyzedNode, _, err = resolveProcedureParams(ctx, paramNames, analyzedNode)
	if err != nil {
		return nil, err
	}
	analyzedNode, _, err = analyzeProcedureBodies(ctx, a, analyzedNode, false, scope, sel)
	if err != nil {
		return nil, err
	}
	analyzedProc, ok := analyzedNode.(*plan.Procedure)
	if !ok {
		return nil, fmt.Errorf("analyzed node %T and expected *plan.Procedure", analyzedNode)
	}
	return analyzedProc, nil
}

func hasProcedureCall(n sql.Node) bool {
	referencesProcedures := false
	transform.Inspect(n, func(n sql.Node) bool {
		if _, ok := n.(*plan.Call); ok {
			referencesProcedures = true
			return false
		} else if rt, ok := n.(*plan.ResolvedTable); ok {
			_, rOk := rt.Table.(RoutineTable)
			if rOk {
				referencesProcedures = true
				return false
			}
		}
		return true
	})
	return referencesProcedures
}

// analyzeProcedureBodies analyzes each statement in a procedure's body individually, as the analyzer is designed to
// inspect single statements rather than a collection of statements, which is usually the body of a stored procedure.
func analyzeProcedureBodies(ctx *sql.Context, a *Analyzer, node sql.Node, skipCall bool, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	children := node.Children()
	newChildren := make([]sql.Node, len(children))
	var err error
	procSel := NewSkipPruneRuleSelector(sel)
	for i, child := range children {
		var newChild sql.Node
		switch child := child.(type) {
		// Anything that may represent a collection of statements should go here
		case *plan.Procedure, *plan.BeginEndBlock, *plan.Block, *plan.IfElseBlock, *plan.IfConditional:
			newChild, _, err = analyzeProcedureBodies(ctx, a, child, skipCall, scope, sel)
		case *plan.Call:
			if skipCall {
				newChild = child
			} else {
				newChild, _, err = a.analyzeWithSelector(ctx, child, scope, SelectAllBatches, procSel)
			}
		default:
			newChild, _, err = a.analyzeWithSelector(ctx, child, scope, SelectAllBatches, procSel)
		}
		if err != nil {
			return nil, transform.SameTree, err
		}
		newChildren[i] = StripPassthroughNodes(newChild)
	}
	node, err = node.WithChildren(newChildren...)
	if err != nil {
		return nil, transform.SameTree, err
	}
	return node, transform.NewTree, nil
}

// validateCreateProcedure handles CreateProcedure nodes, resolving references to the parameters, along with ensuring
// that all logic contained within the stored procedure body is valid.
func validateCreateProcedure(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	cp, ok := node.(*plan.CreateProcedure)
	if !ok {
		return node, transform.SameTree, nil
	}

	paramNames, err := validateStoredProcedure(ctx, cp.Procedure)
	if err != nil {
		return nil, transform.SameTree, err
	}
	proc, _, err := resolveProcedureParams(ctx, paramNames, cp.Procedure)
	if err != nil {
		return nil, transform.SameTree, err
	}
	newProc, _, err := analyzeProcedureBodies(ctx, a, proc, true, nil, sel)
	if err != nil {
		return nil, transform.SameTree, err
	}

	node, err = cp.WithChildren(StripPassthroughNodes(newProc))
	if err != nil {
		return nil, transform.SameTree, err
	}
	return node, transform.NewTree, nil
}

// validateStoredProcedure handles Procedure nodes, resolving references to the parameters, along with ensuring
// that all logic contained within the stored procedure body is valid.
func validateStoredProcedure(_ *sql.Context, proc *plan.Procedure) (map[string]struct{}, error) {
	//TODO: handle declared variables here as well
	paramNames := make(map[string]struct{})
	for _, param := range proc.Params {
		paramName := strings.ToLower(param.Name)
		if _, ok := paramNames[paramName]; ok {
			return nil, sql.ErrProcedureDuplicateParameterName.New(param.Name, proc.Name)
		}
		paramNames[paramName] = struct{}{}
	}

	// For now, we don't support creating any of the following within stored procedures.
	// These will be removed in the future, but cause issues with the current execution plan.
	var err error
	spUnsupportedErr := errors.NewKind("creating %s in stored procedures is currently unsupported " +
		"and will be added in a future release")
	transform.Inspect(proc, func(n sql.Node) bool {
		switch n.(type) {
		case *plan.CreateTable:
			err = spUnsupportedErr.New("tables")
		case *plan.CreateTrigger:
			err = spUnsupportedErr.New("triggers")
		case *plan.CreateProcedure:
			err = spUnsupportedErr.New("procedures")
		case *plan.CreateDB:
			err = spUnsupportedErr.New("databases")
		case *plan.CreateForeignKey:
			err = spUnsupportedErr.New("foreign keys")
		case *plan.CreateIndex:
			err = spUnsupportedErr.New("indexes")
		case *plan.CreateView:
			err = spUnsupportedErr.New("views")
		default:
			return true
		}
		return false
	})
	if err != nil {
		return nil, err
	}

	transform.Inspect(proc, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.Call:
			if proc.Name == strings.ToLower(n.Name) {
				err = sql.ErrProcedureRecursiveCall.New(proc.Name)
			}
		case *plan.LockTables: // Blocked in vitess, but this is for safety
			err = sql.ErrProcedureInvalidBodyStatement.New("LOCK TABLES")
		case *plan.UnlockTables: // Blocked in vitess, but this is for safety
			err = sql.ErrProcedureInvalidBodyStatement.New("UNLOCK TABLES")
		case *plan.Use: // Blocked in vitess, but this is for safety
			err = sql.ErrProcedureInvalidBodyStatement.New("USE")
		case *plan.LoadData:
			err = sql.ErrProcedureInvalidBodyStatement.New("LOAD DATA")
		default:
			return true
		}
		return false
	})
	if err != nil {
		return nil, err
	}

	return paramNames, nil
}

// resolveProcedureParams resolves all named parameters and declared variables in a stored procedure.
func resolveProcedureParams(ctx *sql.Context, paramNames map[string]struct{}, proc sql.Node) (sql.Node, transform.TreeIdentity, error) {
	newProcNode, _, err := resolveProcedureParamsTransform(ctx, paramNames, proc)
	if err != nil {
		return nil, transform.SameTree, err
	}
	// Some nodes do not expose all of their children, so we need to handle them here.
	newProc, _, err := transform.Node(newProcNode, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := node.(type) {
		case *plan.InsertInto:
			newSource, same, err := resolveProcedureParamsTransform(ctx, paramNames, n.Source)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if same {
				return n, transform.SameTree, nil
			}
			return n.WithSource(newSource), transform.NewTree, nil
		case *plan.Union:
			// todo(max): IndexedJoins might be missed here
			newLeft, sameL, err := resolveProcedureParamsTransform(ctx, paramNames, n.Left())
			if err != nil {
				return nil, transform.SameTree, err
			}
			newRight, sameR, err := resolveProcedureParamsTransform(ctx, paramNames, n.Right())
			if err != nil {
				return nil, transform.SameTree, err
			}
			if sameL && sameR {
				return n, transform.SameTree, nil
			}
			node, err = n.WithChildren(newLeft, newRight)
			return node, transform.NewTree, err
		default:
			return n, transform.SameTree, nil
		}
	})
	if err != nil {
		return nil, transform.SameTree, err
	}
	newProc, ok := newProc.(*plan.Procedure)
	if !ok {
		return nil, transform.SameTree, fmt.Errorf("expected `*plan.Procedure` but got `%T`", newProcNode)
	}
	return newProc, transform.NewTree, nil
}

// resolveProcedureParamsTransform resolves all named parameters and declared variables in a node.
// In cases where an expression contains nodes, this will also walk those nodes.
func resolveProcedureParamsTransform(ctx *sql.Context, paramNames map[string]struct{}, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeExprs(n, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		case *expression.UnresolvedColumn:
			if strings.ToLower(e.Table()) == "" {
				if _, ok := paramNames[strings.ToLower(e.Name())]; ok {
					return expression.NewProcedureParam(e.Name()), transform.NewTree, nil
				}
			}
			return e, transform.SameTree, nil
		case *deferredColumn:
			if strings.ToLower(e.Table()) == "" {
				if _, ok := paramNames[strings.ToLower(e.Name())]; ok {
					return expression.NewProcedureParam(e.Name()), transform.NewTree, nil
				}
			}
			return e, transform.SameTree, nil
		case *plan.Subquery: // Subqueries have an internal Query node that we need to check as well.
			newQuery, same, err := resolveProcedureParamsTransform(ctx, paramNames, e.Query)
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
	})
}

// applyProcedures applies the relevant stored procedures to the node given (if necessary).
func applyProcedures(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if scope.proceduresPopulating() {
		return n, transform.SameTree, nil
	}
	if _, ok := n.(*plan.CreateProcedure); ok {
		return n, transform.SameTree, nil
	}

	hasProcedureCall := hasProcedureCall(n)
	_, isShowCreateProcedure := n.(*plan.ShowCreateProcedure)
	if !hasProcedureCall && !isShowCreateProcedure {
		return n, transform.SameTree, nil
	}

	scope, err := loadStoredProcedures(ctx, a, n, scope, sel)
	if err != nil {
		return nil, false, err
	}

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.Call:
			return applyProceduresCall(ctx, a, n, scope, sel)
		case *plan.ShowCreateProcedure:
			procedures, err := a.Catalog.ExternalStoredProcedures(ctx, n.ProcedureName)
			if err != nil {
				return n, transform.SameTree, err
			}
			if len(procedures) == 0 {
				// Not finding an external stored procedure is not an error, since we'll also later
				// search for a user-defined stored procedure with this name.
				return n, transform.SameTree, nil
			}
			return n.WithExternalStoredProcedure(procedures[0]), transform.NewTree, nil
		default:
			return n, transform.SameTree, nil
		}
	})
}

// applyProceduresCall applies the relevant stored procedure to the given *plan.Call.
func applyProceduresCall(ctx *sql.Context, a *Analyzer, call *plan.Call, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	pRef := expression.NewProcedureParamReference()
	call = call.WithParamReference(pRef)

	dbName := ctx.GetCurrentDatabase()
	if call.Database() != nil {
		dbName = call.Database().Name()
	}

	esp, err := a.Catalog.ExternalStoredProcedure(ctx, call.Name, len(call.Params))
	if err != nil {
		return nil, transform.SameTree, err
	}

	var procedure *plan.Procedure
	if esp != nil {
		externalProcedure, err := resolveExternalStoredProcedure(ctx, *esp)
		if err != nil {
			return nil, false, err
		}

		procedure = externalProcedure
	} else {
		procedure = scope.procedures.Get(dbName, call.Name, len(call.Params))
	}

	if procedure == nil {
		err := sql.ErrStoredProcedureDoesNotExist.New(call.Name)
		if dbName == "" {
			return nil, transform.SameTree, fmt.Errorf("%w; this might be because no database is selected", err)
		}
		return nil, transform.SameTree, err
	}

	if procedure.ValidationError != nil {
		return nil, transform.SameTree, procedure.ValidationError
	}

	if procedure.HasVariadicParameter() {
		procedure = procedure.ExtendVariadic(ctx, len(call.Params))
	}

	var procParamTransformFunc transform.ExprFunc
	procParamTransformFunc = func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch expr := e.(type) {
		case *expression.ProcedureParam:
			return expr.WithParamReference(pRef), transform.NewTree, nil
		case *plan.Subquery: // Subqueries have an internal Query node that we need to check as well.
			newQuery, same, err := transform.NodeExprs(expr.Query, procParamTransformFunc)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if same {
				return expr, transform.SameTree, nil
			}
			ne := *expr
			ne.Query = newQuery
			return &ne, transform.NewTree, nil
		default:
			return e, transform.SameTree, nil
		}
	}
	transformedProcedure, _, err := transform.NodeExprs(procedure, procParamTransformFunc)
	if err != nil {
		return nil, transform.SameTree, err
	}
	// Some nodes do not expose all of their children, so we need to handle them here.
	transformedProcedure, _, err = transform.Node(transformedProcedure, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := node.(type) {
		case *plan.InsertInto:
			newSource, same, err := transform.Node(n.Source, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
				return transform.NodeExprs(n, procParamTransformFunc)
			})
			if err != nil {
				return nil, transform.SameTree, err
			}
			if same {
				return n, transform.SameTree, nil
			}
			return n.WithSource(newSource), transform.NewTree, nil
		case *plan.Union:
			newLeft, sameL, err := transform.NodeExprs(n.Left(), procParamTransformFunc)
			if err != nil {
				return nil, transform.SameTree, err
			}
			newRight, sameR, err := transform.NodeExprs(n.Right(), procParamTransformFunc)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if sameL && sameR {
				return n, transform.SameTree, nil
			}
			node, err := n.WithChildren(newLeft, newRight)
			return node, transform.NewTree, err
		default:
			return n, transform.SameTree, nil
		}
	})
	if err != nil {
		return nil, transform.SameTree, err
	}

	transformedProcedure, _, err = transform.Node(transformedProcedure, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		rt, ok := node.(*plan.ResolvedTable)
		if !ok {
			return node, transform.SameTree, nil
		}
		return plan.NewProcedureResolvedTable(rt), transform.NewTree, nil
	})

	transformedProcedure, _, err = applyProcedures(ctx, a, transformedProcedure, scope, sel)
	if err != nil {
		return nil, transform.SameTree, err
	}

	var ok bool
	procedure, ok = transformedProcedure.(*plan.Procedure)
	if !ok {
		return nil, transform.SameTree, fmt.Errorf("expected `*plan.Procedure` but got `%T`", transformedProcedure)
	}

	if len(procedure.Params) != len(call.Params) {
		return nil, transform.SameTree, sql.ErrCallIncorrectParameterCount.New(procedure.Name, len(procedure.Params), len(call.Params))
	}

	call = call.WithProcedure(procedure)
	return call, transform.NewTree, nil
}
