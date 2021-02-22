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
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

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

	body, err := plan.TransformExpressions(cp.Body, func(e sql.Expression) (sql.Expression, error) {
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
	//TODO: this should only apply to either CALL statements or statements that may contain CALL statements
	call, ok := n.(*plan.Call)
	if !ok {
		return n, nil
	}

	callName := strings.ToLower(call.Name)
	db := ctx.GetCurrentDatabase()
	database, err := a.Catalog.Database(db)
	if err != nil {
		return nil, err
	}

	pRef := &expression.ProcedureParamReference{
		NameToParam: make(map[string]interface{}),
	}
	call = call.WithParamReference(pRef)

	if pdb, ok := database.(sql.StoredProcedureDatabase); ok {
		procedures, err := pdb.GetStoredProcedures(ctx)
		if err != nil {
			return nil, err
		}

		for _, procedure := range procedures {
			procName := strings.ToLower(procedure.Name)
			if procName != callName {
				continue
			}

			parsedProcedure, err := parse.Parse(ctx, procedure.CreateStatement)
			if err != nil {
				return nil, err
			}
			cp, ok := parsedProcedure.(*plan.CreateProcedure)
			if !ok {
				return nil, sql.ErrProcedureCreateStatementInvalid.New(procedure.CreateStatement)
			}

			analyzedNode, err := a.Analyze(ctx, cp, scope)
			if err != nil {
				return nil, err
			}
			analyzedNode = stripQueryProcess(analyzedNode)
			analyzedCp, ok := analyzedNode.(*plan.CreateProcedure)
			if !ok {
				return nil, fmt.Errorf("analyzed node %T and expected *plan.CreateProcedure", analyzedNode)
			}

			procedureBody, err := plan.TransformExpressions(analyzedCp.Body, func(e sql.Expression) (sql.Expression, error) {
				switch expr := e.(type) {
				case *expression.ProcedureParam:
					return expr.WithParamReference(pRef), nil
				default:
					return e, nil
				}
			})
			analyzedCp.Body = procedureBody
			call = call.WithProcedure(analyzedCp)
		}
	}
	if !call.HasProcedure() {
		return nil, sql.ErrStoredProcedureDoesNotExist.New(call.Name)
	}
	return call, nil
}
