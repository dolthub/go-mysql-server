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
	"fmt"
	"reflect"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

func hasRecursiveCte(node sql.Node) bool {
	hasRCTE := false
	transform.Inspect(node, func(n sql.Node) bool {
		if _, ok := n.(*plan.RecursiveTable); ok {
			hasRCTE = true
			return false
		}
		return true
	})
	return hasRCTE
}

func (b *Builder) buildSetOp(inScope *scope, u *ast.SetOp) (outScope *scope) {
	leftScope := b.buildSelectStmt(inScope, u.Left)
	rightScope := b.buildSelectStmt(inScope, u.Right)

	var setOpType int
	switch u.Type {
	case ast.UnionStr, ast.UnionAllStr, ast.UnionDistinctStr:
		setOpType = plan.UnionType
	case ast.IntersectStr, ast.IntersectAllStr, ast.IntersectDistinctStr:
		setOpType = plan.IntersectType
	case ast.ExceptStr, ast.ExceptAllStr, ast.ExceptDistinctStr:
		setOpType = plan.ExceptType
	default:
		b.handleErr(fmt.Errorf("unknown union type %s", u.Type))
	}

	if setOpType != plan.UnionType {
		if hasRecursiveCte(leftScope.node) {
			b.handleErr(sql.ErrRecursiveCTENotUnion.New())
		}
		if hasRecursiveCte(rightScope.node) {
			b.handleErr(sql.ErrRecursiveCTENotUnion.New())
		}
	}

	// all is not distinct
	distinct := true
	switch u.Type {
	case ast.UnionAllStr, ast.IntersectAllStr, ast.ExceptAllStr:
		distinct = false
	}

	limit := b.buildLimit(inScope, u.Limit)
	offset := b.buildOffset(inScope, u.Limit)

	for _, o := range u.OrderBy {
		if expr, ok := o.Expr.(*ast.ColName); ok && len(expr.Qualifier.Name.String()) != 0 {
			b.handleErr(ErrQualifiedOrderBy.New(expr.Qualifier.Name.String()))
		}
	}

	// mysql errors for order by right projection
	orderByScope := b.analyzeOrderBy(leftScope, leftScope, u.OrderBy)

	var sortFields sql.SortFields
	for _, c := range orderByScope.cols {
		so := sql.Ascending
		if c.descending {
			so = sql.Descending
		}
		scalar := c.scalar
		if scalar == nil {
			scalar = c.scalarGf()
		}
		// Unions pass order bys to the top scope, where the original
		// order by get field may no longer be accessible. Here it is
		// safe to assume the alias has already been computed.
		scalar, _, _ = transform.Expr(scalar, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			switch e := e.(type) {
			case *expression.Alias:
				return expression.NewGetField(int(c.id), e.Type(), e.Name(), e.IsNullable()), transform.NewTree, nil
			default:
				return e, transform.SameTree, nil
			}
		})
		sf := sql.SortField{
			Column: scalar,
			Order:  so,
		}
		sortFields = append(sortFields, sf)
	}

	n, ok := leftScope.node.(*plan.SetOp)
	if ok {
		if len(n.SortFields) > 0 {
			if len(sortFields) > 0 {
				err := sql.ErrConflictingExternalQuery.New()
				b.handleErr(err)
			}
			sortFields = n.SortFields
		}
		if n.Limit != nil {
			if limit != nil {
				err := fmt.Errorf("conflicing external LIMIT")
				b.handleErr(err)
			}
			limit = n.Limit
		}
		if n.Offset != nil {
			if offset != nil {
				err := fmt.Errorf("conflicing external OFFSET")
				b.handleErr(err)
			}
			offset = n.Offset
		}
		leftScope.node = plan.NewSetOp(n.SetOpType, n.Left(), n.Right(), n.Distinct, nil, nil, nil)
	}

	ret := plan.NewSetOp(setOpType, leftScope.node, rightScope.node, distinct, limit, offset, sortFields)
	outScope = leftScope
	outScope.node = b.mergeSetOpSchemas(ret)
	return
}

func (b *Builder) mergeSetOpSchemas(u *plan.SetOp) sql.Node {
	ls, rs := u.Left().Schema(), u.Right().Schema()
	if len(ls) != len(rs) {
		err := ErrUnionSchemasDifferentLength.New(len(ls), len(rs))
		b.handleErr(err)
	}
	les, res := make([]sql.Expression, len(ls)), make([]sql.Expression, len(rs))
	hasdiff := false
	var err error
	for i := range ls {
		les[i] = expression.NewGetFieldWithTable(i, ls[i].Type, ls[i].Source, ls[i].Name, ls[i].Nullable)
		res[i] = expression.NewGetFieldWithTable(i, rs[i].Type, rs[i].Source, rs[i].Name, rs[i].Nullable)
		if reflect.DeepEqual(ls[i].Type, rs[i].Type) {
			continue
		}
		hasdiff = true

		// try to get optimal type to convert both into
		convertTo := expression.GetConvertToType(ls[i].Type, rs[i].Type)

		// TODO: Principled type coercion...
		les[i], err = b.f.buildConvert(les[i], convertTo, 0, 0)
		res[i], err = b.f.buildConvert(res[i], convertTo, 0, 0)

		// Preserve schema names across the conversion.
		les[i] = expression.NewAlias(ls[i].Name, les[i])
		res[i] = expression.NewAlias(rs[i].Name, res[i])
	}
	var ret sql.Node = u
	if hasdiff {
		ret, err = u.WithChildren(
			plan.NewProject(les, u.Left()),
			plan.NewProject(res, u.Right()),
		)
		if err != nil {
			b.handleErr(err)
		}
	}
	return ret
}
