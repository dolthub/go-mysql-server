// Copyright 2020-2021 Dolthub, Inc.
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

package fixidx

import (
	"github.com/dolthub/go-mysql-server/sql/expression"
	"strings"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// ErrFieldMissing is returned when the field is not on the schema.
var ErrFieldMissing = errors.NewKind("field %q is not on schema")

// FixFieldIndexesOnExpressions executes FixFieldIndexes on a list of exprs.
func FixFieldIndexesOnExpressions(scope *plan.Scope, logFn func(string, ...any), schema sql.Schema, expressions ...sql.Expression) ([]sql.Expression, transform.TreeIdentity, error) {
	var result []sql.Expression
	var res sql.Expression
	var same transform.TreeIdentity
	var err error
	for i := range expressions {
		e := expressions[i]
		res, same, err = FixFieldIndexes(scope, logFn, schema, e)
		if err != nil {
			return nil, transform.SameTree, err
		}
		if !same {
			if result == nil {
				result = make([]sql.Expression, len(expressions))
				copy(result, expressions)
			}
			result[i] = res
		}
	}
	if len(result) > 0 {
		return result, transform.NewTree, nil
	}
	return expressions, transform.SameTree, nil
}

// FixFieldIndexes transforms the given expression by correcting the indexes of columns in GetField expressions,
// according to the schema given. Used when combining multiple tables together into a single join result, or when
// otherwise changing / combining schemas in the node tree.
func FixFieldIndexes(scope *plan.Scope, logFn func(string, ...any), schema sql.Schema, exp sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
	scopeLen := len(scope.Schema())

	return transform.Expr(exp, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		// For each GetField expression, re-index it with the appropriate index from the schema.
		case *expression.GetField:
			for i, col := range schema {
				newIndex := scopeLen + i
				if strings.EqualFold(e.Name(), col.Name) && strings.EqualFold(e.Table(), col.Source) {
					if newIndex != e.Index() {
						if logFn != nil {
							logFn("Rewriting field %s.%s from index %d to %d", e.Table(), e.Name(), e.Index(), newIndex)
						}
						return expression.NewGetFieldWithTable(
							newIndex,
							e.Type(),
							e.Table(),
							e.Name(),
							e.IsNullable(),
						), transform.NewTree, nil
					}
					return e, transform.SameTree, nil
				}
			}

			// If we didn't find the column in the schema of the node itself, look outward in surrounding scopes. Work
			// inner-to-outer, in  accordance with MySQL scope naming precedence rules.
			offset := 0
			for _, n := range scope.InnerToOuter() {
				schema := Schemas(n.Children())
				offset += len(schema)
				for i, col := range schema {
					if strings.EqualFold(e.Name(), col.Name) && strings.EqualFold(e.Table(), col.Source) {
						newIndex := scopeLen - offset + i
						if e.Index() != newIndex {
							if logFn != nil {
								logFn("Rewriting field %s.%s from index %d to %d", e.Table(), e.Name(), e.Index(), newIndex)
							}
							return expression.NewGetFieldWithTable(
								newIndex,
								e.Type(),
								e.Table(),
								e.Name(),
								e.IsNullable(),
							), transform.NewTree, nil
						}
						return e, transform.SameTree, nil
					}
				}
			}

			return nil, transform.SameTree, ErrFieldMissing.New(e.Name())
		}

		return e, transform.SameTree, nil
	})
}

// Schemas returns the Schemas for the nodes given appended in to a single one
func Schemas(nodes []sql.Node) sql.Schema {
	var schema sql.Schema
	for _, n := range nodes {
		schema = append(schema, n.Schema()...)
	}
	return schema
}

// FixFieldIndexesForExpressions transforms the expressions in the Node given, fixing the field indexes.
func FixFieldIndexesForExpressions(logFn func(string, ...any), node sql.Node, scope *plan.Scope) (sql.Node, transform.TreeIdentity, error) {
	if _, ok := node.(sql.Expressioner); !ok {
		return node, transform.SameTree, nil
	}

	var schemas []sql.Schema
	for _, child := range node.Children() {
		schemas = append(schemas, child.Schema())
	}

	if len(schemas) < 1 {
		return node, transform.SameTree, nil
	}

	n, sameC, err := transform.OneNodeExprsWithNode(node, func(_ sql.Node, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		for _, schema := range schemas {
			fixed, same, err := FixFieldIndexes(scope, logFn, schema, e)
			if err == nil {
				return fixed, same, nil
			}

			if ErrFieldMissing.Is(err) {
				continue
			}

			return nil, transform.SameTree, err
		}

		return e, transform.SameTree, nil
	})

	if err != nil {
		return nil, transform.SameTree, err
	}

	j, ok := n.(*plan.JoinNode)
	if !ok {
		return n, sameC, nil
	}
	sameJ := transform.SameTree
	var cond sql.Expression
	switch {
	case j.Op.IsCross():
	case j.Op.IsPartial():
		if !ok {
			return n, sameC && sameJ, nil
		}
		cond, sameJ, err = FixFieldIndexes(scope, logFn, append(j.Left().Schema(), j.Right().Schema()...), j.JoinCond())
		if err != nil {
			return nil, transform.SameTree, err
		}
		if !sameJ {
			n, err = j.WithExpressions(cond)
			if err != nil {
				return nil, transform.SameTree, err
			}
		}
	default:
		cond, sameJ, err = FixFieldIndexes(scope, logFn, j.Schema(), j.JoinCond())
		if err != nil {
			return nil, transform.SameTree, err
		}
		if !sameJ {
			n, err = j.WithExpressions(cond)
			if err != nil {
				return nil, transform.SameTree, err
			}
		}
	}

	return n, sameC && sameJ, nil
}

// FixFieldIndexesForTableNode transforms the expressions in the Node given,
// fixing the field indexes. This is useful for Table nodes that have
// expressions but no children.
func FixFieldIndexesForTableNode(ctx *sql.Context, logFn func(string, ...any), node sql.Node, scope *plan.Scope) (sql.Node, transform.TreeIdentity, error) {
	if _, ok := node.(sql.Expressioner); !ok {
		return node, transform.SameTree, nil
	}
	return transform.OneNodeExprsWithNode(node, func(_ sql.Node, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		schema := node.Schema()
		fixed, same, err := FixFieldIndexes(scope, logFn, schema, e)
		if err != nil {
			if ErrFieldMissing.Is(err) {
				return e, transform.SameTree, nil
			}
			return nil, transform.SameTree, err
		}
		return fixed, same, nil
	})
}

func FixFieldIndexesForNode(logFn func(string, ...any), scope *plan.Scope, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		ret := n
		var err error
		sameN := transform.SameTree
		switch n := n.(type) {
		case *plan.SubqueryAlias:
			scope := scope.NewScopeFromSubqueryAlias(n)
			ret, sameN, err = FixFieldIndexesForExpressions(logFn, n, scope)
		default:
			ret, sameN, err = FixFieldIndexesForExpressions(logFn, n, scope)
		}
		if err != nil {
			return n, transform.SameTree, err
		}
		ret, sameE, err := transform.NodeExprs(ret, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			if sq, ok := e.(*plan.Subquery); ok {
				return FixFieldIndexes(scope, logFn, ret.Schema(), sq)
			}
			return e, transform.SameTree, nil
		})
		if err != nil {
			return n, transform.SameTree, err
		}
		return ret, sameN && sameE, nil
	})
}
