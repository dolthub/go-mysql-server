package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/fixidx"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// fixupAuxiliaryExprs calls FixUpExpressions on Sort and Project nodes
// to compensate for the new name resolution expression overloading GetField
// indexes.
func fixupAuxiliaryExprs(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeWithOpaque(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		default:
			ret, same1, err := fixidx.FixFieldIndexesForExpressions(a.LogFn(), n, ctx, scope)
			if err != nil {
				return n, transform.SameTree, err
			}
			// walk subquery expressions
			ret, same2, err := transform.OneNodeExpressions(ret, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				switch e := e.(type) {
				case *plan.Subquery:
					newQ, same, err := fixupAuxiliaryExprs(ctx, a, e.Query, scope.NewScopeFromSubqueryExpression(ret), sel)
					if same || err != nil {
						return e, transform.SameTree, err
					}
					return plan.NewSubquery(newQ, e.QueryString), transform.NewTree, nil
				default:
				}
				return e, transform.SameTree, nil
			})
			if same1 && same2 || err != nil {
				return n, transform.SameTree, nil
			}
			return ret, transform.NewTree, nil
		case *plan.ShowVariables:
			if n.Filter != nil {
				newF, same, err := fixidx.FixFieldIndexes(scope, a.LogFn(), n.Schema(), n.Filter)
				if same || err != nil {
					return n, transform.SameTree, err
				}
				n.Filter = newF
				return n, transform.NewTree, nil
			}
			return n, transform.SameTree, nil
		//case *plan.Set:
		//	exprs, same, err := fixidx.FixFieldIndexesOnExpressions(scope, a.LogFn(), nil, n.Exprs...)
		//	if err != nil || same {
		//		return n, transform.SameTree, err
		//	}
		//	return plan.NewSet(exprs), transform.NewTree, nil
		case *plan.CreateTable:
			allSame := transform.SameTree
			if len(n.ChDefs) > 0 {
				for i, ch := range n.ChDefs {
					newExpr, same, err := fixidx.FixFieldIndexes(scope, a.LogFn(), n.CreateSchema.Schema, ch.Expr)
					if err != nil {
						return n, transform.SameTree, err
					}
					allSame = allSame && same
					n.ChDefs[i].Expr = newExpr
				}
			}
			return n, allSame, nil
		case *plan.Update:
			allSame := transform.SameTree
			if len(n.Checks) > 0 {
				for i, ch := range n.Checks {
					newExpr, same, err := fixidx.FixFieldIndexes(scope, a.LogFn(), n.Schema(), ch.Expr)
					if err != nil {
						return n, transform.SameTree, err
					}
					allSame = allSame && same
					n.Checks[i].Expr = newExpr
				}
			}
			return n, allSame, nil
		case *plan.InsertInto:
			newN, same1, err := fixidx.FixFieldIndexesForExpressions(a.LogFn(), n, ctx, scope)
			if err != nil {
				return n, transform.SameTree, err
			}
			ins := newN.(*plan.InsertInto)
			newSource, same2, err := fixupAuxiliaryExprs(ctx, a, ins.Source, scope, sel)
			if err != nil || (same1 && same2) {
				return n, transform.SameTree, err
			}
			ins.Source = newSource
			return ins, transform.NewTree, nil
			//case *plan.InsertInto:
			//	newN, _, err := fixidx.FixFieldIndexesForExpressions(a.LogFn(), n, scope)
			//	if err != nil {
			//		return n, transform.SameTree, err
			//	}
			//	newIns := newN.(*plan.InsertInto)
			//	newIns.OnDupExprs, _, err = fixidx.FixFieldIndexesOnExpressions(scope, a.LogFn(), n.Destination.Schema(), n.OnDupExprs...)
			//	if err != nil {
			//		return n, transform.SameTree, err
			//	}
			//	return newIns, transform.NewTree, nil
		}
	})
}

type aliasScope struct {
	aliases map[string]sql.Expression
	parent  *aliasScope
}

func (a *aliasScope) push() *aliasScope {
	ret := &aliasScope{
		parent: a,
	}
	return ret
}

func (a *aliasScope) addRef(alias *expression.Alias) {
	if a.aliases == nil {
		a.aliases = make(map[string]sql.Expression)
	}
	a.aliases[alias.Name()] = alias.Child
}

func (a *aliasScope) isOuterRef(name string) (bool, sql.Expression) {
	if a.aliases != nil {
		if a, ok := a.aliases[name]; ok {
			return false, a
		}
	}
	if a.parent == nil {
		return false, nil
	}
	_, found := a.parent.isOuterRef(name)
	if found != nil {
		return true, found
	}
	return false, nil
}

// inlineSubqueryAliasRefs matches the pattern:
// SELECT expr as <alias>, (SELECT <alias> ...) ...
// and performs a variable replacement:
// SELECT expr as <alias>, (SELECT expr ...) ...
// Outer alias references can occur anywhere in subquery expressions,
// as written this is a fairly unflexible rule.
// TODO: extend subquery search to WHERE filters and other scalar expressions
// TODO: convert subquery expressions to lateral joins to avoid this hack
func inlineSubqueryAliasRefs(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	aliasScope := &aliasScope{}
	return inlineSubqueryAliasRefsHelper(aliasScope, n), transform.NewTree, nil
}

func inlineSubqueryAliasRefsHelper(scope *aliasScope, n sql.Node) sql.Node {
	ret := n
	switch n := n.(type) {
	case *plan.Project:
		var newProj []sql.Expression
		for i, e := range n.Projections {
			e, same, err := transform.Expr(e, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				switch e := e.(type) {
				case *expression.AliasReference:
				case *expression.Alias:
					// new def
					if !e.Unreferencable() {
						scope.addRef(e)
					}
				case *expression.GetField:
					// is an alias ref?
					// check if in parent scope
					if inOuter, alias := scope.isOuterRef(strings.ToLower(e.Name())); e.Table() == "" && alias != nil && inOuter {
						return alias, transform.NewTree, nil
					}
				case *plan.Subquery:
					subqScope := scope.push()
					newQ := inlineSubqueryAliasRefsHelper(subqScope, e.Query)
					ret := *e
					ret.Query = newQ
					return &ret, transform.NewTree, nil
				default:
				}
				return e, transform.SameTree, nil
			})
			if err != nil {
				panic(err)
			}
			if !same {
				if newProj == nil {
					newProj = make([]sql.Expression, len(n.Projections))
					copy(newProj, n.Projections)
				}
				newProj[i] = e
			}
		}
		if newProj != nil {
			ret = plan.NewProject(newProj, n.Child)
		}
	default:
	}

	newChildren := make([]sql.Node, len(n.Children()))
	for i, c := range ret.Children() {
		newChildren[i] = inlineSubqueryAliasRefsHelper(scope, c)
	}
	ret, err := ret.WithChildren(newChildren...)
	if err != nil {
		panic(err)
	}
	return ret
}
