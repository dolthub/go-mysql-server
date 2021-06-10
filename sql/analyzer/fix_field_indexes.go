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
func FixFieldIndexesOnExpressions(ctx *sql.Context, scope *Scope, a *Analyzer, schema sql.Schema, expressions ...sql.Expression) ([]sql.Expression, error) {
	var result = make([]sql.Expression, len(expressions))
	for i, e := range expressions {
		var err error
		result[i], err = FixFieldIndexes(ctx, scope, a, schema, e)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// FixFieldIndexes transforms the given expression by correcting the indexes of columns in GetField expressions,
// according to the schema given. Used when combining multiple tables together into a single join result, or when
// otherwise changing / combining schemas in the node tree.
func FixFieldIndexes(ctx *sql.Context, scope *Scope, a *Analyzer, schema sql.Schema, exp sql.Expression) (sql.Expression, error) {
	scopeLen := len(scope.Schema())

	return expression.TransformUp(ctx, exp, func(e sql.Expression) (sql.Expression, error) {
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
						), nil
					}
					return e, nil
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
							), nil
						}
						return e, nil
					}
				}
			}

			return nil, ErrFieldMissing.New(e.Name())
		}

		return e, nil
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
func FixFieldIndexesForExpressions(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (sql.Node, error) {
	if _, ok := node.(sql.Expressioner); !ok {
		return node, nil
	}

	var schemas []sql.Schema
	for _, child := range node.Children() {
		schemas = append(schemas, child.Schema())
	}

	if len(schemas) < 1 {
		return node, nil
	}

	n, err := plan.TransformExpressions(ctx, node, func(e sql.Expression) (sql.Expression, error) {
		for _, schema := range schemas {
			fixed, err := FixFieldIndexes(ctx, scope, a, schema, e)
			if err == nil {
				return fixed, nil
			}

			if ErrFieldMissing.Is(err) {
				continue
			}

			return nil, err
		}

		return e, nil
	})

	if err != nil {
		return nil, err
	}

	switch j := n.(type) {
	case *plan.InnerJoin:
		cond, err := FixFieldIndexes(ctx, scope, a, j.Schema(), j.Cond)
		if err != nil {
			return nil, err
		}

		n, err = j.WithExpressions(cond)
		if err != nil {
			return nil, err
		}
	case *plan.RightJoin:
		cond, err := FixFieldIndexes(ctx, scope, a, j.Schema(), j.Cond)
		if err != nil {
			return nil, err
		}

		n, err = j.WithExpressions(cond)
		if err != nil {
			return nil, err
		}
	case *plan.LeftJoin:
		cond, err := FixFieldIndexes(ctx, scope, a, j.Schema(), j.Cond)
		if err != nil {
			return nil, err
		}

		n, err = j.WithExpressions(cond)
		if err != nil {
			return nil, err
		}
	}

	return n, nil
}

// Transforms the expressions in the Node given, fixing the field indexes. This is useful for Table nodes that have
// expressions but no children.
func FixFieldIndexesForTableNode(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (sql.Node, error) {
	if _, ok := node.(sql.Expressioner); !ok {
		return node, nil
	}

	n, err := plan.TransformExpressions(ctx, node, func(e sql.Expression) (sql.Expression, error) {
		schema := node.Schema()
		fixed, err := FixFieldIndexes(ctx, scope, a, schema, e)
		if err != nil {
			if ErrFieldMissing.Is(err) {
				return e, nil
			}
			return nil, err
		}

		return fixed, nil
	})

	if err != nil {
		return nil, err
	}

	return n, nil
}
