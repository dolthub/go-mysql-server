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

				procToRegister.CreatedAt = procedure.CreatedAt
				procToRegister.ModifiedAt = procedure.ModifiedAt

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
	err := validateStoredProcedure(ctx, cp.Procedure)
	if err != nil {
		return nil, err
	}
	var analyzedNode sql.Node
	analyzedNode, _, err = resolveDeclarations(ctx, a, cp.Procedure, scope, sel)
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
	procSel := NewProcRuleSelector(sel)
	for i, child := range children {
		var newChild sql.Node
		switch child := child.(type) {
		case plan.RepresentsBlock:
			// Many analyzer rules only check the top-level node, so we have to recursively analyze each child
			newChild, _, err = analyzeProcedureBodies(ctx, a, child, skipCall, scope, sel)
			if err != nil {
				return nil, transform.SameTree, err
			}
			// Blocks may have expressions declared directly on them, so we explicitly check the block node for variables
			newChild, _, err = a.analyzeWithSelector(ctx, newChild, scope, SelectAllBatches, func(id RuleId) bool {
				return id == resolveVariablesId
			})
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

// validateCreateProcedure handles CreateProcedure nodes, ensuring that all nodes in Procedure are supported.
func validateCreateProcedure(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	cp, ok := node.(*plan.CreateProcedure)
	if !ok {
		return node, transform.SameTree, nil
	}

	err := validateStoredProcedure(ctx, cp.Procedure)
	if err != nil {
		return nil, transform.SameTree, err
	}

	return node, transform.SameTree, nil
}

// resolveCreateProcedure handles CreateProcedure nodes, resolving references to the parameters, along with ensuring
// that all logic contained within the stored procedure body is valid.
func resolveCreateProcedure(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	cp, ok := node.(*plan.CreateProcedure)
	if !ok {
		return node, transform.SameTree, nil
	}

	proc, _, err := resolveDeclarations(ctx, a, cp.Procedure, scope, sel)
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
func validateStoredProcedure(_ *sql.Context, proc *plan.Procedure) error {
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
		return err
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
		return err
	}

	return nil
}

// applyProcedures applies the relevant stored procedures to the node given (if necessary).
func applyProcedures(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if _, ok := n.(*plan.CreateProcedure); ok {
		return n, transform.SameTree, nil
	}

	hasProcedureCall := hasProcedureCall(n)
	_, isShowCreateProcedure := n.(*plan.ShowCreateProcedure)
	if !hasProcedureCall && !isShowCreateProcedure {
		return n, transform.SameTree, nil
	}

	call, newIdentity, err := transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		call, ok := n.(*plan.Call)
		if !ok {
			return n, transform.SameTree, nil
		}
		if scope.IsEmpty() {
			scope = scope.withProcedureCache(NewProcedureCache())
		}
		if call.AsOf() != nil && !scope.enforceReadOnly {
			scope.enforceReadOnly = true
			defer func() {
				scope.enforceReadOnly = false
			}()
		}

		esp, err := a.Catalog.ExternalStoredProcedure(ctx, call.Name, len(call.Params))
		if err != nil {
			return nil, transform.SameTree, err
		}
		if esp != nil {
			externalProcedure, err := resolveExternalStoredProcedure(ctx, *esp)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return call.WithProcedure(externalProcedure), transform.NewTree, nil
		}

		if spdb, ok := call.Database().(sql.StoredProcedureDatabase); ok {
			procedure, ok, err := spdb.GetStoredProcedure(ctx, call.Name)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if !ok {
				err := sql.ErrStoredProcedureDoesNotExist.New(call.Name)
				if call.Database().Name() == "" {
					return nil, transform.SameTree, fmt.Errorf("%w; this might be because no database is selected", err)
				}
				return nil, transform.SameTree, err
			}
			parsedProcedure, err := parse.Parse(ctx, procedure.CreateStatement)
			if err != nil {
				return nil, transform.SameTree, err
			}
			parsedProcedure, _, err = transform.Node(parsedProcedure, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
				versionable, ok := n.(plan.Versionable)
				if !ok {
					return n, transform.SameTree, nil
				}
				tree := transform.SameTree
				if newCall, ok := versionable.(*plan.Call); ok {
					if newCall.Database() == nil || newCall.Database().Name() == "" {
						newNode, err := newCall.WithDatabase(spdb)
						if err != nil {
							return nil, transform.SameTree, err
						}
						versionable = newNode.(plan.Versionable)
						tree = transform.NewTree
					}
				}
				if versionable.AsOf() == nil {
					newNode, err := versionable.WithAsOf(call.AsOf())
					if err != nil {
						return nil, transform.SameTree, err
					}
					versionable = newNode.(plan.Versionable)
					tree = transform.NewTree
				}
				return versionable, tree, nil
			})
			if err != nil {
				return nil, transform.SameTree, err
			}
			cp, ok := parsedProcedure.(*plan.CreateProcedure)
			if !ok {
				return nil, transform.SameTree, sql.ErrProcedureCreateStatementInvalid.New(procedure.CreateStatement)
			}
			analyzedProc, err := analyzeCreateProcedure(ctx, a, cp, scope, sel)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return call.WithProcedure(analyzedProc), transform.NewTree, nil
		} else {
			return nil, transform.SameTree, sql.ErrStoredProceduresNotSupported.New(call.Database().Name())
		}
	})
	if err != nil {
		return nil, transform.SameTree, err
	}
	n = call

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
			return n, newIdentity, nil
		}
	})
}

// applyProceduresCall applies the relevant stored procedure to the given *plan.Call.
func applyProceduresCall(ctx *sql.Context, a *Analyzer, call *plan.Call, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	var procedure *plan.Procedure
	if call.Procedure == nil {
		dbName := ctx.GetCurrentDatabase()
		if call.Database() != nil {
			dbName = call.Database().Name()
		}

		esp, err := a.Catalog.ExternalStoredProcedure(ctx, call.Name, len(call.Params))
		if err != nil {
			return nil, transform.SameTree, err
		}

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
	} else {
		procedure = call.Procedure
	}

	if procedure.HasVariadicParameter() {
		procedure = procedure.ExtendVariadic(ctx, len(call.Params))
	}
	pRef := expression.NewProcedureReference()
	call = call.WithParamReference(pRef)

	var procParamTransformFunc transform.ExprFunc
	procParamTransformFunc = func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch expr := e.(type) {
		case *expression.ProcedureParam:
			return expr.WithParamReference(pRef), transform.NewTree, nil
		case sql.ExpressionWithNodes:
			children := expr.NodeChildren()
			var newChildren []sql.Node
			for i, child := range children {
				newChild, same, err := transform.NodeExprsWithOpaque(child, procParamTransformFunc)
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
				newExpr, err := expr.WithNodeChildren(newChildren...)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return newExpr, transform.NewTree, nil
			}
			return e, transform.SameTree, nil
		default:
			return e, transform.SameTree, nil
		}
	}
	transformedProcedure, _, err := transform.NodeExprsWithOpaque(procedure, procParamTransformFunc)
	if err != nil {
		return nil, transform.SameTree, err
	}
	// Some nodes do not expose all of their children, so we need to handle them here.
	transformedProcedure, _, err = transform.NodeWithOpaque(transformedProcedure, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := node.(type) {
		case plan.DisjointedChildrenNode:
			same := transform.SameTree
			disjointedChildGroups := n.DisjointedChildren()
			newDisjointedChildGroups := make([][]sql.Node, len(disjointedChildGroups))
			for groupIdx, disjointedChildGroup := range disjointedChildGroups {
				newDisjointedChildGroups[groupIdx] = make([]sql.Node, len(disjointedChildGroup))
				for childIdx, disjointedChild := range disjointedChildGroup {
					var childIdentity transform.TreeIdentity
					if newDisjointedChildGroups[groupIdx][childIdx], childIdentity, err = transform.NodeExprsWithOpaque(disjointedChild, procParamTransformFunc); err != nil {
						return nil, transform.SameTree, err
					} else if childIdentity == transform.NewTree {
						same = childIdentity
					}
				}
			}
			if same == transform.NewTree {
				if newChild, err := n.WithDisjointedChildren(newDisjointedChildGroups); err != nil {
					return nil, transform.SameTree, err
				} else {
					return newChild, transform.NewTree, nil
				}
			}
			return n, transform.SameTree, nil
		case expression.ProcedureReferencable:
			return n.WithParamReference(pRef), transform.NewTree, nil
		default:
			return transform.NodeExprsWithOpaque(n, procParamTransformFunc)
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
