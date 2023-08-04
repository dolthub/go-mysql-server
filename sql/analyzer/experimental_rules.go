package analyzer

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/fixidx"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// fixupAuxiliaryExprs calls FixUpExpressions on Sort and Project nodes
// to compensate for the new name resolution expression overloading GetField
// indexes.
func fixupAuxiliaryExprs(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeWithOpaque(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if _, ok := n.(*plan.Union); ok {
			print("")
		}
		switch n := n.(type) {
		default:
			ret, same1, err := fixidx.FixFieldIndexesForExpressions(ctx, a.LogFn(), n, scope)
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
		case *plan.Fetch:
			return n, transform.SameTree, nil
		case *plan.JSONTable:
			newExprs, same, err := fixidx.FixFieldIndexesOnExpressions(scope, a.LogFn(), nil, n.Expressions()...)
			if err != nil {
				return n, transform.SameTree, nil
			}
			if same {
				return n, transform.SameTree, nil
			}
			newJt, err := n.WithExpressions(newExprs...)
			return newJt, transform.NewTree, err
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
			newN, same1, err := fixidx.FixFieldIndexesForExpressions(ctx, a.LogFn(), n, scope)
			if err != nil {
				return n, transform.SameTree, err
			}
			ins := newN.(*plan.InsertInto)
			newSource, same2, err := fixupAuxiliaryExprs(ctx, a, ins.Source, scope, sel)
			if err != nil || (same1 && same2) {
				return n, transform.SameTree, err
			}
			ins.Source = newSource

			rightSchema := make(sql.Schema, len(n.Destination.Schema())*2)
			// schema = [oldrow][newrow]
			for i, c := range n.Destination.Schema() {
				rightSchema[i] = c
				if _, ok := n.Source.(*plan.Values); !ok && len(n.Destination.Schema()) == len(n.Source.Schema()) {
					rightSchema[len(n.Destination.Schema())+i] = n.Source.Schema()[i]
				} else {
					newC := c.Copy()
					newC.Source = "__new_ins"
					rightSchema[len(n.Destination.Schema())+i] = newC
				}
			}

			var newOnDups []sql.Expression
			for i, e := range n.OnDupExprs {
				set, ok := e.(*expression.SetField)
				if !ok {
					return n, transform.SameTree, fmt.Errorf("on duplicate update expressions should be *expression.SetField; found %T", e)
				}
				left, same1, err := fixidx.FixFieldIndexes(scope, a.LogFn(), n.Destination.Schema(), set.Left)
				if err != nil {
					return n, transform.SameTree, err
				}

				right, same2, err := fixidx.FixFieldIndexes(scope, a.LogFn(), rightSchema, set.Right)
				if err != nil {
					return n, transform.SameTree, err
				}
				if same1 && same2 {
					continue
				}
				if newOnDups == nil {
					newOnDups = make([]sql.Expression, len(n.OnDupExprs))
					copy(newOnDups, n.OnDupExprs)
				}
				newOnDups[i] = expression.NewSetField(left, right)
			}
			if newOnDups != nil {
				ins.OnDupExprs = newOnDups
			}
			return ins, transform.NewTree, nil
		}
	})
}

func hoistSort(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		// match [node] -> Sort
		// move sort above [node]
		// exclusion conditions:
		// - parent does not have input cols for sort
		// -
		return n, transform.SameTree, nil
	})
}

type aliasScope struct {
	aliases map[string]sql.Expression
	parent  *aliasScope
}

func (a *aliasScope) push() *aliasScope {
	return &aliasScope{
		parent: a,
	}
}

func (a *aliasScope) addRef(alias *expression.Alias) {
	if a.aliases == nil {
		a.aliases = make(map[string]sql.Expression)
	}
	a.aliases[alias.Name()] = alias.Child
}

func (a *aliasScope) isOuterRef(name string) (sql.Expression, bool) {
	if a.aliases != nil {
		if a, ok := a.aliases[name]; ok {
			return a, false
		}
	}
	if a.parent == nil {
		return nil, false
	}
	found, _ := a.parent.isOuterRef(name)
	if found != nil {
		return found, true
	}
	return nil, false
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
	ret, err := inlineSubqueryAliasRefsHelper(&aliasScope{}, n)
	return ret, transform.NewTree, err
}

func inlineSubqueryAliasRefsHelper(scope *aliasScope, n sql.Node) (sql.Node, error) {
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
					if alias, inOuter := scope.isOuterRef(strings.ToLower(e.Name())); e.Table() == "" && alias != nil && inOuter {
						return alias, transform.NewTree, nil
					}
				case *plan.Subquery:
					subqScope := scope.push()
					newQ, err := inlineSubqueryAliasRefsHelper(subqScope, e.Query)
					if err != nil {
						return e, transform.SameTree, err
					}
					ret := *e
					ret.Query = newQ
					return &ret, transform.NewTree, nil
				default:
				}
				return e, transform.SameTree, nil
			})
			if err != nil {
				return nil, err
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
	var err error
	for i, c := range ret.Children() {
		newChildren[i], err = inlineSubqueryAliasRefsHelper(scope, c)
		if err != nil {
			return nil, err
		}
	}
	ret, err = ret.WithChildren(newChildren...)
	if err != nil {
		return nil, err
	}
	if err != nil {
		panic(err)
	}
	return ret, nil
}
