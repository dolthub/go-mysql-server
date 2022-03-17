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

package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// FixFieldIndexesOnExpressions executes FixFieldIndexes on a list of exprs.
func FixFieldIndexesOnExpressions(ctx *sql.Context, scope *Scope, a *Analyzer, schema sql.Schema, expressions ...sql.Expression) ([]sql.Expression, sql.TreeIdentity, error) {
	var result []sql.Expression
	var res sql.Expression
	var same sql.TreeIdentity
	var err error
	for i := 0; i < len(expressions); i++ {
		e := expressions[i]
		res, same, err = FixFieldIndexes(ctx, scope, a, schema, e)
		if err != nil {
			return nil, sql.SameTree, err
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
		return result, sql.NewTree, nil
	}
	return expressions, sql.SameTree, nil
}

// FixFieldIndexes transforms the given expression by correcting the indexes of columns in GetField expressions,
// according to the schema given. Used when combining multiple tables together into a single join result, or when
// otherwise changing / combining schemas in the node tree.
func FixFieldIndexes(ctx *sql.Context, scope *Scope, a *Analyzer, schema sql.Schema, exp sql.Expression) (sql.Expression, sql.TreeIdentity, error) {
	scopeLen := len(scope.Schema())

	return expression.TransformUpHelper(exp, func(e sql.Expression) (sql.Expression, sql.TreeIdentity, error) {
		switch e := e.(type) {
		// For each GetField expression, re-index it with the appropriate index from the schema.
		case *expression.GetField:
			for i, col := range schema {
				newIndex := scopeLen + i
				if e.Name() == col.Name && e.Table() == col.Source {
					if newIndex != e.Index() {
						a.Log("Rewriting field %s.%s from index %d to %d", e.Table(), e.Name(), e.Index(), newIndex)
						return expression.NewGetFieldWithTable(
							newIndex,
							e.Type(),
							e.Table(),
							e.Name(),
							e.IsNullable(),
						), sql.NewTree, nil
					}
					return e, sql.SameTree, nil
				}
			}

			// If we didn't find the column in the schema of the node itself, look outward in surrounding scopes. Work
			// inner-to-outer, in  accordance with MySQL scope naming precedence rules.
			offset := 0
			for _, n := range scope.InnerToOuter() {
				schema := schemas(n.Children())
				offset += len(schema)
				for i, col := range schema {
					if e.Name() == col.Name && e.Table() == col.Source {
						newIndex := scopeLen - offset + i
						if e.Index() != newIndex {
							a.Log("Rewriting field %s.%s from index %d to %d", e.Table(), e.Name(), e.Index(), newIndex)
							return expression.NewGetFieldWithTable(
								newIndex,
								e.Type(),
								e.Table(),
								e.Name(),
								e.IsNullable(),
							), sql.NewTree, nil
						}
						return e, sql.SameTree, nil
					}
				}
			}

			return nil, sql.SameTree, ErrFieldMissing.New(e.Name())
		}

		return e, sql.SameTree, nil
	})
}

// schemas returns the schemas for the nodes given appended in to a single one
func schemas(nodes []sql.Node) sql.Schema {
	var schema sql.Schema
	for _, n := range nodes {
		schema = append(schema, n.Schema()...)
	}
	return schema
}

// Transforms the expressions in the Node given, fixing the field indexes.
func FixFieldIndexesForExpressions(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (sql.Node, sql.TreeIdentity, error) {
	if _, ok := node.(sql.Expressioner); !ok {
		return node, sql.SameTree, nil
	}

	var schemas []sql.Schema
	for _, child := range node.Children() {
		schemas = append(schemas, child.Schema())
	}

	if len(schemas) < 1 {
		return node, sql.SameTree, nil
	}

	n, sameC, err := plan.TransformExpressionsWithNode(node, func(_ sql.Node, e sql.Expression) (sql.Expression, sql.TreeIdentity, error) {
		for _, schema := range schemas {
			fixed, same, err := FixFieldIndexes(ctx, scope, a, schema, e)
			if err == nil {
				return fixed, same, nil
			}

			if ErrFieldMissing.Is(err) {
				continue
			}

			return nil, sql.SameTree, err
		}

		return e, sql.SameTree, nil
	})

	if err != nil {
		return nil, sql.SameTree, err
	}

	sameJ := sql.SameTree
	var cond sql.Expression
	switch j := n.(type) {
	case *plan.InnerJoin:
		cond, sameJ, err = FixFieldIndexes(ctx, scope, a, j.Schema(), j.Cond)
		if err != nil {
			return nil, sql.SameTree, err
		}
		if !sameJ {
			n, err = j.WithExpressions(cond)
			if err != nil {
				return nil, sql.SameTree, err
			}
		}
	case *plan.RightJoin:
		cond, sameJ, err = FixFieldIndexes(ctx, scope, a, j.Schema(), j.Cond)
		if err != nil {
			return nil, sql.SameTree, err
		}
		if !sameJ {
			n, err = j.WithExpressions(cond)
			if err != nil {
				return nil, sql.SameTree, err
			}
		}
	case *plan.LeftJoin:
		cond, sameJ, err = FixFieldIndexes(ctx, scope, a, j.Schema(), j.Cond)
		if err != nil {
			return nil, sql.SameTree, err
		}
		if !sameJ {
			n, err = j.WithExpressions(cond)
			if err != nil {
				return nil, sql.SameTree, err
			}
		}
	}

	return n, sameC && sameJ, nil
}

// Transforms the expressions in the Node given, fixing the field indexes. This is useful for Table nodes that have
// expressions but no children.
func FixFieldIndexesForTableNode(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (sql.Node, sql.TreeIdentity, error) {
	if _, ok := node.(sql.Expressioner); !ok {
		return node, sql.SameTree, nil
	}

	n, same, err := plan.TransformExpressionsWithNode(node, func(_ sql.Node, e sql.Expression) (sql.Expression, sql.TreeIdentity, error) {
		schema := node.Schema()
		fixed, same, err := FixFieldIndexes(ctx, scope, a, schema, e)
		if err != nil {
			if ErrFieldMissing.Is(err) {
				return e, sql.SameTree, nil
			}
			return nil, sql.SameTree, err
		}
		return fixed, same, nil
	})

	if err != nil {
		return nil, sql.SameTree, err
	}

	return n, same, nil
}
