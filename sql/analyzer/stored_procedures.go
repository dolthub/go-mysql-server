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

	"github.com/dolthub/go-mysql-server/sql/parse"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// loadStoredProcedures loads stored procedures for all databases on relevant calls.
func loadStoredProcedures(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	if ctx.ProcedureCache.IsPopulating() {
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
	ctx.ProcedureCache = sql.NewProcedureCache(true)

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

				analyzedNode, err := a.Analyze(ctx, cp, nil)
				if err != nil {
					return nil, err
				}
				analyzedNode = stripQueryProcess(analyzedNode)
				analyzedCp, ok := analyzedNode.(*plan.CreateProcedure)
				if !ok {
					return nil, fmt.Errorf("analyzed node %T and expected *plan.CreateProcedure", analyzedNode)
				}

				ctx.ProcedureCache.Register(database.Name(), analyzedCp.Procedure)
			}
		}
	}
	return n, nil
}

// validateStoredProcedure handles CreateProcedure nodes, resolving references to the parameters, along with ensuring
// that all logic contained within the stored procedure body is valid.
func validateStoredProcedure(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (sql.Node, error) {
	cp, ok := node.(*plan.CreateProcedure)
	if !ok {
		return node, nil
	}

	paramNames := make(map[string]struct{})
	for _, param := range cp.Params {
		paramName := strings.ToLower(param.Name)
		if _, ok := paramNames[paramName]; ok {
			return nil, fmt.Errorf("duplicate parameter name `%s` on stored procedure `%s`", param.Name, cp.Name)
		}
		paramNames[paramName] = struct{}{}
	}

	// For now, we don't support creating any of the following within stored procedures.
	// These will be removed in the future, but cause issues with the current execution plan.
	var err error
	spUnsupportedErr := errors.NewKind("creating %s in stored procedures is currently unsupported " +
		"and will be added in a future release")
	plan.Inspect(cp.Body, func(n sql.Node) bool {
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

	body, err := plan.TransformExpressionsUp(cp.Body, func(e sql.Expression) (sql.Expression, error) {
		switch e := e.(type) {
		case *expression.UnresolvedColumn:
			if strings.ToLower(e.Table()) == "" {
				if _, ok := paramNames[strings.ToLower(e.Name())]; !ok {
					return nil, fmt.Errorf("unknown parameter name `%s` on stored procedure `%s`", e.Name(), cp.Name)
				}
				return expression.NewProcedureParam(e.Name()), nil
			}
			return e, nil
		case *deferredColumn:
			if strings.ToLower(e.Table()) == "" {
				if _, ok := paramNames[strings.ToLower(e.Name())]; !ok {
					return nil, fmt.Errorf("unknown parameter name `%s` on stored procedure `%s`", e.Name(), cp.Name)
				}
				return expression.NewProcedureParam(e.Name()), nil
			}
			return e, nil
		default:
			return e, nil
		}
	})
	if err != nil {
		return nil, err
	}

	//TODO: check the procedure body and verify that only valid statements are contained within
	//TODO: check the procedure body and ensure that no recursive procedure calls are made

	analyzedBody, err := a.Analyze(ctx, body, scope)
	if err != nil {
		return nil, err
	}

	return cp.WithChildren(stripQueryProcess(analyzedBody))
}

func applyProcedures(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
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

func applyProceduresCall(ctx *sql.Context, a *Analyzer, call *plan.Call, scope *Scope) (sql.Node, error) {
	pRef := expression.NewProcedureParamReference()
	call = call.WithParamReference(pRef)

	procedure := ctx.ProcedureCache.Get(ctx.GetCurrentDatabase(), call.Name)
	if procedure == nil {
		return nil, sql.ErrStoredProcedureDoesNotExist.New(call.Name)
	}
	procedureBody, err := plan.TransformExpressionsUp(procedure.Body, func(e sql.Expression) (sql.Expression, error) {
		switch expr := e.(type) {
		case *expression.ProcedureParam:
			return expr.WithParamReference(pRef), nil
		default:
			return e, nil
		}
	})
	if err != nil {
		return nil, err
	}
	procedureBody, err = applyProcedures(ctx, a, procedureBody, scope)
	if err != nil {
		return nil, err
	}

	procedure.Body = procedureBody
	call = call.WithProcedure(procedure)
	return call, nil
}

func applyProceduresShowProcedure(ctx *sql.Context, a *Analyzer, n *plan.ShowProcedureStatus, scope *Scope) (sql.Node, error) {
	n.Procedures = ctx.ProcedureCache.AllForDatabase(ctx.GetCurrentDatabase())
	return n, nil
}
