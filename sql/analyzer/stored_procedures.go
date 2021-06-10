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
)

// loadStoredProcedures loads stored procedures for all databases on relevant calls.
func loadStoredProcedures(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	if a.ProcedureCache.IsPopulating {
		return n, nil
	}
	referencesProcedures := false
	plan.Inspect(n, func(n sql.Node) bool {
		if _, ok := n.(*plan.Call); ok {
			referencesProcedures = true
			return false
		} else if _, ok := n.(*plan.ShowProcedureStatus); ok {
			referencesProcedures = true
			return false
		}
		return true
	})
	if !referencesProcedures {
		return n, nil
	}
	a.ProcedureCache = NewProcedureCache()
	a.ProcedureCache.IsPopulating = true
	defer func() {
		a.ProcedureCache.IsPopulating = false
	}()

	for _, database := range a.Catalog.AllDatabases() {
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

				paramNames, err := validateStoredProcedure(ctx, cp.Procedure)
				if err != nil {
					return nil, err
				}
				analyzedNode, err := resolveDeclarations(ctx, a, cp.Procedure, scope)
				if err != nil {
					return nil, err
				}
				analyzedNode, err = resolveProcedureParams(ctx, paramNames, analyzedNode)
				if err != nil {
					return nil, err
				}
				analyzedNode, err = analyzeProcedureBodies(ctx, a, analyzedNode, false, scope)
				if err != nil {
					return nil, err
				}
				analyzedProc, ok := analyzedNode.(*plan.Procedure)
				if !ok {
					return nil, fmt.Errorf("analyzed node %T and expected *plan.Procedure", analyzedNode)
				}

				a.ProcedureCache.Register(database.Name(), analyzedProc)
			}
		}
	}
	return n, nil
}

// analyzeProcedureBodies analyzes each statement in a procedure's body individually, as the analyzer is designed to
// inspect single statements rather than a collection of statements, which is usually the body of a stored procedure.
func analyzeProcedureBodies(ctx *sql.Context, a *Analyzer, node sql.Node, skipCall bool, scope *Scope) (sql.Node, error) {
	children := node.Children()
	newChildren := make([]sql.Node, len(children))
	var err error
	for i, child := range children {
		var newChild sql.Node
		switch child := child.(type) {
		// Anything that may represent a collection of statements should go here
		case *plan.Procedure, *plan.BeginEndBlock, *plan.Block, *plan.IfElseBlock, *plan.IfConditional:
			newChild, err = analyzeProcedureBodies(ctx, a, child, skipCall, scope)
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
			return nil, err
		}
		newChildren[i] = stripQueryProcess(newChild)
	}
	return node.WithChildren(newChildren...)
}

// validateCreateProcedure handles CreateProcedure nodes, resolving references to the parameters, along with ensuring
// that all logic contained within the stored procedure body is valid.
func validateCreateProcedure(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (sql.Node, error) {
	cp, ok := node.(*plan.CreateProcedure)
	if !ok {
		return node, nil
	}

	paramNames, err := validateStoredProcedure(ctx, cp.Procedure)
	if err != nil {
		return nil, err
	}
	proc, err := resolveProcedureParams(ctx, paramNames, cp.Procedure)
	if err != nil {
		return nil, err
	}
	newProc, err := analyzeProcedureBodies(ctx, a, proc, true, nil)
	if err != nil {
		return nil, err
	}

	return cp.WithChildren(stripQueryProcess(newProc))
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
	plan.Inspect(proc, func(n sql.Node) bool {
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

	plan.Inspect(proc, func(n sql.Node) bool {
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
func resolveProcedureParams(ctx *sql.Context, paramNames map[string]struct{}, proc sql.Node) (sql.Node, error) {
	newProcNode, err := resolveProcedureParamsTransform(ctx, paramNames, proc)
	if err != nil {
		return nil, err
	}
	// Some nodes do not expose all of their children, so we need to handle them here.
	newProcNode, err = plan.TransformUp(newProcNode, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.InsertInto:
			newSource, err := resolveProcedureParamsTransform(ctx, paramNames, n.Source)
			if err != nil {
				return nil, err
			}
			return n.WithSource(newSource), nil
		case *plan.Union:
			newLeft, err := resolveProcedureParamsTransform(ctx, paramNames, n.Left())
			if err != nil {
				return nil, err
			}
			newRight, err := resolveProcedureParamsTransform(ctx, paramNames, n.Right())
			if err != nil {
				return nil, err
			}
			return n.WithChildren(newLeft, newRight)
		default:
			return n, nil
		}
	})
	if err != nil {
		return nil, err
	}
	newProc, ok := newProcNode.(*plan.Procedure)
	if !ok {
		return nil, fmt.Errorf("expected `*plan.Procedure` but got `%T`", newProcNode)
	}
	return newProc, nil
}

// resolveProcedureParamsTransform resolves all of the named parameters and declared variables inside of a node.
// In cases where an expression contains nodes, this will also walk those nodes.
func resolveProcedureParamsTransform(ctx *sql.Context, paramNames map[string]struct{}, n sql.Node) (sql.Node, error) {
	return plan.TransformExpressionsUp(ctx, n, func(e sql.Expression) (sql.Expression, error) {
		switch e := e.(type) {
		case *expression.UnresolvedColumn:
			if strings.ToLower(e.Table()) == "" {
				if _, ok := paramNames[strings.ToLower(e.Name())]; ok {
					return expression.NewProcedureParam(e.Name()), nil
				}
			}
			return e, nil
		case *deferredColumn:
			if strings.ToLower(e.Table()) == "" {
				if _, ok := paramNames[strings.ToLower(e.Name())]; ok {
					return expression.NewProcedureParam(e.Name()), nil
				}
			}
			return e, nil
		case *plan.Subquery: // Subqueries have an internal Query node that we need to check as well.
			newQuery, err := resolveProcedureParamsTransform(ctx, paramNames, e.Query)
			if err != nil {
				return nil, err
			}
			ne := *e
			ne.Query = newQuery
			return &ne, nil
		default:
			return e, nil
		}
	})
}

// applyProcedures applies the relevant stored procedures to the node given (if necessary).
func applyProcedures(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	if a.ProcedureCache.IsPopulating {
		return n, nil
	}
	if _, ok := n.(*plan.CreateProcedure); ok {
		return n, nil
	}
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.Call:
			return applyProceduresCall(ctx, a, n, scope)
		case *plan.ShowProcedureStatus:
			return applyProceduresShowProcedure(ctx, a, n, scope)
		default:
			return n, nil
		}
	})
}

// applyProceduresCall applies the relevant stored procedure to the given *plan.Call.
func applyProceduresCall(ctx *sql.Context, a *Analyzer, call *plan.Call, scope *Scope) (sql.Node, error) {
	pRef := expression.NewProcedureParamReference()
	call = call.WithParamReference(pRef)

	procedure := a.ProcedureCache.Get(ctx.GetCurrentDatabase(), call.Name)
	if procedure == nil {
		return nil, sql.ErrStoredProcedureDoesNotExist.New(call.Name)
	}

	var procParamTransformFunc sql.TransformExprFunc
	procParamTransformFunc = func(e sql.Expression) (sql.Expression, error) {
		switch expr := e.(type) {
		case *expression.ProcedureParam:
			return expr.WithParamReference(pRef), nil
		case *plan.Subquery: // Subqueries have an internal Query node that we need to check as well.
			newQuery, err := plan.TransformExpressionsUp(ctx, expr.Query, procParamTransformFunc)
			if err != nil {
				return nil, err
			}
			ne := *expr
			ne.Query = newQuery
			return &ne, nil
		default:
			return e, nil
		}
	}
	transformedProcedure, err := plan.TransformExpressionsUp(ctx, procedure, procParamTransformFunc)
	if err != nil {
		return nil, err
	}
	// Some nodes do not expose all of their children, so we need to handle them here.
	transformedProcedure, err = plan.TransformUp(transformedProcedure, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.InsertInto:
			newSource, err := plan.TransformExpressionsUp(ctx, n.Source, procParamTransformFunc)
			if err != nil {
				return nil, err
			}
			return n.WithSource(newSource), nil
		case *plan.Union:
			newLeft, err := plan.TransformExpressionsUp(ctx, n.Left(), procParamTransformFunc)
			if err != nil {
				return nil, err
			}
			newRight, err := plan.TransformExpressionsUp(ctx, n.Right(), procParamTransformFunc)
			if err != nil {
				return nil, err
			}
			return n.WithChildren(newLeft, newRight)
		default:
			return n, nil
		}
	})
	if err != nil {
		return nil, err
	}

	transformedProcedure, err = plan.TransformUpWithParent(transformedProcedure, func(n sql.Node, parent sql.Node, childNum int) (sql.Node, error) {
		rt, ok := n.(*plan.ResolvedTable)
		if !ok {
			return n, nil
		}
		return plan.NewProcedureResolvedTable(rt), nil
	})
	transformedProcedure, err = applyProcedures(ctx, a, transformedProcedure, scope)
	if err != nil {
		return nil, err
	}

	var ok bool
	procedure, ok = transformedProcedure.(*plan.Procedure)
	if !ok {
		return nil, fmt.Errorf("expected `*plan.Procedure` but got `%T`", transformedProcedure)
	}

	if len(procedure.Params) != len(call.Params) {
		return nil, sql.ErrCallIncorrectParameterCount.New(procedure.Name, len(procedure.Params), len(call.Params))
	}

	call = call.WithProcedure(procedure)
	return call, nil
}

// applyProceduresShowProcedure applies all of the stored procedures to the given *plan.ShowProcedureStatus.
func applyProceduresShowProcedure(ctx *sql.Context, a *Analyzer, n *plan.ShowProcedureStatus, scope *Scope) (sql.Node, error) {
	n.Procedures = a.ProcedureCache.AllForDatabase(ctx.GetCurrentDatabase())
	return n, nil
}
