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

// loadStoredProcedures loads stored procedures for all databases on relevant calls.
func loadStoredProcedures(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, sql.TreeIdentity, error) {
	if a.ProcedureCache.IsPopulating {
		return n, sql.SameTree, nil
	}
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
	if !referencesProcedures {
		return n, sql.SameTree, nil
	}
	a.ProcedureCache = NewProcedureCache()
	a.ProcedureCache.IsPopulating = true
	defer func() {
		a.ProcedureCache.IsPopulating = false
	}()

	for _, database := range a.Catalog.AllDatabases(ctx) {
		if pdb, ok := database.(sql.StoredProcedureDatabase); ok {
			procedures, err := pdb.GetStoredProcedures(ctx)
			if err != nil {
				return nil, sql.SameTree, err
			}

			for _, procedure := range procedures {
				parsedProcedure, err := parse.Parse(ctx, procedure.CreateStatement)
				if err != nil {
					return nil, sql.SameTree, err
				}
				cp, ok := parsedProcedure.(*plan.CreateProcedure)
				if !ok {
					return nil, sql.SameTree, sql.ErrProcedureCreateStatementInvalid.New(procedure.CreateStatement)
				}

				paramNames, err := validateStoredProcedure(ctx, cp.Procedure)
				if err != nil {
					return nil, sql.SameTree, err
				}
				analyzedNode, _, err := resolveDeclarations(ctx, a, cp.Procedure, scope)
				if err != nil {
					return nil, sql.SameTree, err
				}
				analyzedNode, _, err = resolveProcedureParams(ctx, paramNames, analyzedNode)
				if err != nil {
					return nil, sql.SameTree, err
				}
				analyzedNode, _, err = analyzeProcedureBodies(ctx, a, analyzedNode, false, scope)
				if err != nil {
					return nil, sql.SameTree, err
				}
				analyzedProc, ok := analyzedNode.(*plan.Procedure)
				if !ok {
					return nil, sql.SameTree, fmt.Errorf("analyzed node %T and expected *plan.Procedure", analyzedNode)
				}

				a.ProcedureCache.Register(database.Name(), analyzedProc)
			}
		}
	}
	return n, sql.SameTree, nil
}

// analyzeProcedureBodies analyzes each statement in a procedure's body individually, as the analyzer is designed to
// inspect single statements rather than a collection of statements, which is usually the body of a stored procedure.
func analyzeProcedureBodies(ctx *sql.Context, a *Analyzer, node sql.Node, skipCall bool, scope *Scope) (sql.Node, sql.TreeIdentity, error) {
	children := node.Children()
	newChildren := make([]sql.Node, len(children))
	var err error
	for i, child := range children {
		var newChild sql.Node
		switch child := child.(type) {
		// Anything that may represent a collection of statements should go here
		case *plan.Procedure, *plan.BeginEndBlock, *plan.Block, *plan.IfElseBlock, *plan.IfConditional:
			newChild, _, err = analyzeProcedureBodies(ctx, a, child, skipCall, scope)
		case *plan.Call:
			if skipCall {
				newChild = child
			} else {
				newChild, err = a.Analyze(ctx, child, scope)
			}
		default:
			newChild, err = a.Analyze(ctx, child, scope)
		}
		if err != nil {
			return nil, sql.SameTree, err
		}
		newChildren[i] = StripPassthroughNodes(newChild)
	}
	node, err = node.WithChildren(newChildren...)
	if err != nil {
		return nil, sql.SameTree, err
	}
	return node, sql.NewTree, nil
}

// validateCreateProcedure handles CreateProcedure nodes, resolving references to the parameters, along with ensuring
// that all logic contained within the stored procedure body is valid.
func validateCreateProcedure(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (sql.Node, sql.TreeIdentity, error) {
	cp, ok := node.(*plan.CreateProcedure)
	if !ok {
		return node, sql.SameTree, nil
	}

	paramNames, err := validateStoredProcedure(ctx, cp.Procedure)
	if err != nil {
		return nil, sql.SameTree, err
	}
	proc, _, err := resolveProcedureParams(ctx, paramNames, cp.Procedure)
	if err != nil {
		return nil, sql.SameTree, err
	}
	newProc, _, err := analyzeProcedureBodies(ctx, a, proc, true, nil)
	if err != nil {
		return nil, sql.SameTree, err
	}

	node, err = cp.WithChildren(StripPassthroughNodes(newProc))
	if err != nil {
		return nil, sql.SameTree, err
	}
	return node, sql.NewTree, nil
}

// validateStoredProcedure handles Procedure nodes, resolving references to the parameters, along with ensuring
// that all logic contained within the stored procedure body is valid.
func validateStoredProcedure(ctx *sql.Context, proc *plan.Procedure) (map[string]struct{}, error) {
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

// resolveProcedureParams resolves all of the named parameters and declared variables inside of a stored procedure.
func resolveProcedureParams(ctx *sql.Context, paramNames map[string]struct{}, proc sql.Node) (sql.Node, sql.TreeIdentity, error) {
	newProcNode, _, err := resolveProcedureParamsTransform(ctx, paramNames, proc)
	if err != nil {
		return nil, sql.SameTree, err
	}
	// Some nodes do not expose all of their children, so we need to handle them here.
	newProc, _, err := transform.Node(newProcNode, func(node sql.Node) (sql.Node, sql.TreeIdentity, error) {
		switch n := node.(type) {
		case *plan.InsertInto:
			newSource, same, err := resolveProcedureParamsTransform(ctx, paramNames, n.Source)
			if err != nil {
				return nil, sql.SameTree, err
			}
			if same {
				return n, sql.SameTree, nil
			}
			return n.WithSource(newSource), sql.NewTree, nil
		case *plan.Union:
			//TODO unions aren't opaque, is this necessary?
			// IndexedJoins might be missed
			newLeft, sameL, err := resolveProcedureParamsTransform(ctx, paramNames, n.Left())
			if err != nil {
				return nil, sql.SameTree, err
			}
			newRight, sameR, err := resolveProcedureParamsTransform(ctx, paramNames, n.Right())
			if err != nil {
				return nil, sql.SameTree, err
			}
			if sameL && sameR {
				return n, sql.SameTree, nil
			}
			node, err = n.WithChildren(newLeft, newRight)
			return node, sql.NewTree, err
		default:
			return n, sql.SameTree, nil
		}
	})
	if err != nil {
		return nil, sql.SameTree, err
	}
	newProc, ok := newProc.(*plan.Procedure)
	if !ok {
		return nil, sql.SameTree, fmt.Errorf("expected `*plan.Procedure` but got `%T`", newProcNode)
	}
	return newProc, sql.NewTree, nil
}

// resolveProcedureParamsTransform resolves all of the named parameters and declared variables inside of a node.
// In cases where an expression contains nodes, this will also walk those nodes.
func resolveProcedureParamsTransform(ctx *sql.Context, paramNames map[string]struct{}, n sql.Node) (sql.Node, sql.TreeIdentity, error) {
	return transform.NodeExprs(n, func(e sql.Expression) (sql.Expression, sql.TreeIdentity, error) {
		switch e := e.(type) {
		case *expression.UnresolvedColumn:
			if strings.ToLower(e.Table()) == "" {
				if _, ok := paramNames[strings.ToLower(e.Name())]; ok {
					return expression.NewProcedureParam(e.Name()), sql.NewTree, nil
				}
			}
			return e, sql.SameTree, nil
		case *deferredColumn:
			if strings.ToLower(e.Table()) == "" {
				if _, ok := paramNames[strings.ToLower(e.Name())]; ok {
					return expression.NewProcedureParam(e.Name()), sql.NewTree, nil
				}
			}
			return e, sql.SameTree, nil
		case *plan.Subquery: // Subqueries have an internal Query node that we need to check as well.
			newQuery, same, err := resolveProcedureParamsTransform(ctx, paramNames, e.Query)
			if err != nil {
				return nil, sql.SameTree, err
			}
			if same {
				return e, sql.SameTree, nil
			}
			ne := *e
			ne.Query = newQuery
			return &ne, sql.NewTree, nil
		default:
			return e, sql.SameTree, nil
		}
	})
}

// applyProcedures applies the relevant stored procedures to the node given (if necessary).
func applyProcedures(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, sql.TreeIdentity, error) {
	if a.ProcedureCache.IsPopulating {
		return n, sql.SameTree, nil
	}
	if _, ok := n.(*plan.CreateProcedure); ok {
		return n, sql.SameTree, nil
	}
	return transform.Node(n, func(n sql.Node) (sql.Node, sql.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.Call:
			return applyProceduresCall(ctx, a, n, scope)
		default:
			return n, sql.SameTree, nil
		}
	})
}

// applyProceduresCall applies the relevant stored procedure to the given *plan.Call.
func applyProceduresCall(ctx *sql.Context, a *Analyzer, call *plan.Call, scope *Scope) (sql.Node, sql.TreeIdentity, error) {
	pRef := expression.NewProcedureParamReference()
	call = call.WithParamReference(pRef)

	procedure := a.ProcedureCache.Get(ctx.GetCurrentDatabase(), call.Name)
	if procedure == nil {
		return nil, sql.SameTree, sql.ErrStoredProcedureDoesNotExist.New(call.Name)
	}

	var procParamTransformFunc sql.TransformExprFunc
	procParamTransformFunc = func(e sql.Expression) (sql.Expression, sql.TreeIdentity, error) {
		switch expr := e.(type) {
		case *expression.ProcedureParam:
			return expr.WithParamReference(pRef), sql.NewTree, nil
		case *plan.Subquery: // Subqueries have an internal Query node that we need to check as well.
			newQuery, same, err := transform.NodeExprs(expr.Query, procParamTransformFunc)
			if err != nil {
				return nil, sql.SameTree, err
			}
			if same {
				return expr, sql.SameTree, nil
			}
			ne := *expr
			ne.Query = newQuery
			return &ne, sql.NewTree, nil
		default:
			return e, sql.SameTree, nil
		}
	}
	transformedProcedure, _, err := transform.NodeExprs(procedure, procParamTransformFunc)
	if err != nil {
		return nil, sql.SameTree, err
	}
	// Some nodes do not expose all of their children, so we need to handle them here.
	transformedProcedure, _, err = transform.Node(transformedProcedure, func(node sql.Node) (sql.Node, sql.TreeIdentity, error) {
		switch n := node.(type) {
		case *plan.InsertInto:
			newSource, same, err := transform.Node(n.Source, func(n sql.Node) (sql.Node, sql.TreeIdentity, error) {
				return transform.NodeExprs(n, procParamTransformFunc)
			})
			if err != nil {
				return nil, sql.SameTree, err
			}
			if same {
				return n, sql.SameTree, nil
			}
			return n.WithSource(newSource), sql.NewTree, nil
		case *plan.Union:
			newLeft, sameL, err := transform.NodeExprs(n.Left(), procParamTransformFunc)
			if err != nil {
				return nil, sql.SameTree, err
			}
			newRight, sameR, err := transform.NodeExprs(n.Right(), procParamTransformFunc)
			if err != nil {
				return nil, sql.SameTree, err
			}
			if sameL && sameR {
				return n, sql.SameTree, nil
			}
			node, err := n.WithChildren(newLeft, newRight)
			return node, sql.NewTree, err
		default:
			return n, sql.SameTree, nil
		}
	})
	if err != nil {
		return nil, sql.SameTree, err
	}

	transformedProcedure, _, err = transform.Node(transformedProcedure, func(node sql.Node) (sql.Node, sql.TreeIdentity, error) {
		rt, ok := node.(*plan.ResolvedTable)
		if !ok {
			return node, sql.SameTree, nil
		}
		return plan.NewProcedureResolvedTable(rt), sql.NewTree, nil
	})
	transformedProcedure, _, err = applyProcedures(ctx, a, transformedProcedure, scope)
	if err != nil {
		return nil, sql.SameTree, err
	}

	var ok bool
	procedure, ok = transformedProcedure.(*plan.Procedure)
	if !ok {
		return nil, sql.SameTree, fmt.Errorf("expected `*plan.Procedure` but got `%T`", transformedProcedure)
	}

	if len(procedure.Params) != len(call.Params) {
		return nil, sql.SameTree, sql.ErrCallIncorrectParameterCount.New(procedure.Name, len(procedure.Params), len(call.Params))
	}

	call = call.WithProcedure(procedure)
	return call, sql.NewTree, nil
}
