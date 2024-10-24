// Copyright 2023 Dolthub, Inc.
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

package planbuilder

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// factory functions should apply all optimizations to an expression
// that are always costing/simplification wins. Each function will be a series
// of optimizations local to this specific node.
//
// TODO: split this into a factory object/package when we start memoizing the plan
// TODO: switch statement for each type
// TODO: logging when optimizations triggered

type factory struct {
	ctx   *sql.Context
	debug bool
}

func (f *factory) log(s string) {
	if f.debug {
		f.ctx.GetLogger().Info(s)
	}
}

func (f *factory) buildProject(p *plan.Project, subquery bool) (sql.Node, error) {
	{
		// todo generalize this. proj->proj with subquery expression alias
		// references are one problem.
		if sqa, _ := p.Child.(*plan.SubqueryAlias); sqa != nil && p.Schema().Equals(sqa.Schema()) {
			f.log("eliminated projection")
			return sqa, nil
		}
	}

	{
		// project->project=>project
		if p2, _ := p.Child.(*plan.Project); p2 != nil {
			if !subquery {
				// it is important to bisect subquery expression alias inputs
				// into a separate projection with current exec impl
				adjGraph := make(map[sql.ColumnId]sql.Expression, 0)
				for _, e := range p2.Projections {
					// inner projections track/collapse alias refs
					_, err := aliasTrackAndReplace(adjGraph, e)
					if err != nil {
						return nil, err
					}
				}

				var newP []sql.Expression
				for _, e := range p.Projections {
					//outer projections are the ones we want, with aliases replaced
					newE, err := aliasTrackAndReplace(adjGraph, e)
					if err != nil {
						return nil, err
					}
					newP = append(newP, newE)
				}
				return plan.NewProject(newP, p2.Child), nil
			}
		}
	}
	return p, nil
}

func containsSubqueryExpr(exprs []sql.Expression) bool {
	for _, e := range exprs {
		subqFound := transform.InspectExpr(e, func(e sql.Expression) bool {
			_, ok := e.(*plan.Subquery)
			return ok
		})
		if subqFound {
			return true
		}
	}
	return false
}

func aliasTrackAndReplace(adj map[sql.ColumnId]sql.Expression, e sql.Expression) (sql.Expression, error) {
	var id sql.ColumnId
	if ide, ok := e.(sql.IdExpression); ok {
		id = ide.Id()
	}
	newE, _, err := transform.Expr(e, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		case *expression.GetField:
			if a, _ := adj[sql.ColumnId(e.Index())]; a != nil {
				if _, ok := a.(*expression.Alias); ok {
					// prefer outer-most field reference, is case-sensitive
					return a, transform.NewTree, nil
				}
			}
		default:
		}
		return e, transform.SameTree, nil
	})
	if err != nil {
		return nil, err
	}
	if id > 0 {
		adj[id] = newE
	}
	return newE, nil
}

func (f *factory) buildConvert(expr sql.Expression, castToType string, typeLength, typeScale int) (sql.Expression, error) {
	n := expression.NewConvertWithLengthAndScale(expr, castToType, typeLength, typeScale)
	{
		// deduplicate redundant convert
		if expr.Type().Equals(n.Type()) {
			f.log("eliminated convert")
			return expr, nil
		}
	}
	return n, nil
}

func (f *factory) buildJoin(l, r sql.Node, op plan.JoinType, cond sql.Expression) (sql.Node, error) {
	{
		// fold empty joins
		if _, empty := l.(*plan.EmptyTable); empty {
			f.log("folded empty table join")
			return plan.NewEmptyTableWithSchema(append(l.Schema(), r.Schema()...)), nil
		}
		if _, empty := r.(*plan.EmptyTable); empty && !op.IsLeftOuter() {
			f.log("folded empty table join")
			return plan.NewEmptyTableWithSchema(append(l.Schema(), r.Schema()...)), nil
		}
	}

	{
		// transpose right joins
		if op.IsRightOuter() {
			f.log("transposed right join")
			return f.buildJoin(r, l, plan.JoinTypeLeftOuter, cond)
		}
		if op == plan.JoinTypeLateralRight {
			f.log("transposed right join")
			return f.buildJoin(r, l, plan.JoinTypeLateralLeft, cond)
		}
	}
	return plan.NewJoin(l, r, op, cond), nil
}

func (f *factory) buildTableAlias(name string, child sql.Node) (plan.TableIdNode, error) {
	{
		// deduplicate tableAlias->tableAlias and tableAlias->subqueryAlias
		switch n := child.(type) {
		case *plan.TableAlias:
			return n.WithName(name).(plan.TableIdNode), nil
		case *plan.SubqueryAlias:
			return n.WithName(name).(plan.TableIdNode), nil
		case plan.TableIdNode:
			return plan.NewTableAlias(name, child).WithId(n.Id()).WithColumns(n.Columns()), nil
		default:
			return plan.NewTableAlias(name, child), nil
		}
	}
}

// buildDistinct will wrap the child node in a distinct node depending on the Sort nodes and Projections there.
// if the sort fields are a subset of the projection fields
//
//	sort(project(table)) -> sort(distinct(project(table)))
//
// else
//
//	sort(project(table)) -> distinct(sort(project(table)))
func (f *factory) buildDistinct(child sql.Node) sql.Node {
	if proj, isProj := child.(*plan.Project); isProj {
		// TODO: if projection columns are just primary key, distinct is no-op
		// TODO: distinct literals are just one row
		if sort, isSort := proj.Child.(*plan.Sort); isSort {
			projMap := make(map[string]struct{})
			for _, p := range proj.Projections {
				projMap[strings.ToLower(p.String())] = struct{}{}
			}
			hasDiff := false
			for _, s := range sort.SortFields {
				if _, ok := projMap[strings.ToLower(s.Column.String())]; !ok {
					hasDiff = true
					break
				}
			}
			if !hasDiff {
				proj.Child = sort.Child
				sort.Child = plan.NewDistinct(proj)
				return sort
			}
		}
	}
	return plan.NewDistinct(child)
}
