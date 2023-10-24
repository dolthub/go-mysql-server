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
	"strings"

	"github.com/dolthub/vitess/go/sqltypes"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func (b *Builder) analyzeOrderBy(fromScope, projScope *scope, order ast.OrderBy) (outScope *scope) {
	// Order by resolves to
	// 1) alias in projScope
	// 2) column name in fromScope
	// 3) index into projection scope

	// if regular col, make sure in aggOut or add to extra cols

	outScope = fromScope.replace()
	for _, o := range order {
		var descending bool
		switch strings.ToLower(o.Direction) {
		default:
			err := errInvalidSortOrder.New(o.Direction)
			b.handleErr(err)
		case ast.AscScr:
			descending = false
		case ast.DescScr:
			descending = true
		}

		switch e := o.Expr.(type) {
		case *ast.ColName:
			// check for projection alias first
			dbName := strings.ToLower(e.Qualifier.Qualifier.String())
			tblName := strings.ToLower(e.Qualifier.Name.String())
			colName := strings.ToLower(e.Name.String())
			c, ok := projScope.resolveColumn(dbName, tblName, colName, false)
			if ok {
				c.descending = descending
				outScope.addColumn(c)
				continue
			}

			// fromScope col
			c, ok = fromScope.resolveColumn(dbName, tblName, colName, true)
			if !ok {
				err := sql.ErrColumnNotFound.New(e.Name)
				b.handleErr(err)
			}
			c.descending = descending
			c.scalar = c.scalarGf()
			outScope.addColumn(c)
			fromScope.addExtraColumn(c)
		case *ast.SQLVal:
			// integer literal into projScope
			// else throw away
			expr := b.normalizeValArg(e)
			if val, ok := expr.(*ast.SQLVal); ok && val.Type == ast.IntVal {
				lit := b.convertInt(string(val.Val), 10)
				idx, _, err := types.Int64.Convert(lit.Value())
				if err != nil {
					b.handleErr(err)
				}
				intIdx, ok := idx.(int64)
				if !ok {
					b.handleErr(fmt.Errorf("expected integer order by literal"))
				}
				// negative intIdx is allowed in MySQL, and is treated as a no-op
				if intIdx < 0 {
					continue
				}
				if projScope == nil || len(projScope.cols) == 0 {
					err := fmt.Errorf("invalid order by ordinal context")
					b.handleErr(err)
				}
				// MySQL throws a column not found for intIdx = 0 and intIdx > len(cols)
				if intIdx > int64(len(projScope.cols)) || intIdx == 0 {
					err := sql.ErrColumnNotFound.New(fmt.Sprintf("%d", intIdx))
					b.handleErr(err)
				}
				target := projScope.cols[intIdx-1]
				scalar := target.scalar
				if scalar == nil {
					scalar = target.scalarGf()
				}
				if a, ok := target.scalar.(*expression.Alias); ok && a.Unreferencable() && fromScope.groupBy != nil {
					for _, c := range fromScope.groupBy.outScope.cols {
						if target.id == c.id {
							target = c
						}
					}
				}
				outScope.addColumn(scopeColumn{
					tableId:    target.tableId,
					col:        target.col,
					scalar:     scalar,
					typ:        target.typ,
					nullable:   target.nullable,
					descending: descending,
					id:         target.id,
				})
			}
		default:
			// track order by col
			// replace aggregations with refs
			// pick up auxiliary cols
			expr := b.buildScalar(fromScope, e)
			_, ok := outScope.getExpr(expr.String(), true)
			if ok {
				continue
			}
			// aggregate ref -> expr.String() in
			// or compound expression
			expr, _, _ = transform.Expr(expr, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				//  get fields outside of aggs need to be in extra cols
				switch e := e.(type) {
				case *expression.GetField:
					c, ok := fromScope.resolveColumn("", strings.ToLower(e.Table()), strings.ToLower(e.Name()), true)
					if !ok {
						err := sql.ErrColumnNotFound.New(e.Name)
						b.handleErr(err)
					}
					fromScope.addExtraColumn(c)
				case sql.WindowAdaptableExpression:
					// has to have been ref'd already
					id, ok := fromScope.getExpr(e.String(), true)
					if !ok {
						err := fmt.Errorf("faild to ref aggregate expression: %s", e.String())
						b.handleErr(err)
					}
					return expression.NewGetField(int(id), e.Type(), e.String(), e.IsNullable()), transform.NewTree, nil
				default:
				}
				return e, transform.SameTree, nil
			})
			col := scopeColumn{
				tableId:    sql.TableID{},
				col:        expr.String(),
				scalar:     expr,
				typ:        expr.Type(),
				nullable:   expr.IsNullable(),
				descending: descending,
			}
			outScope.newColumn(col)
		}
	}
	return
}

func (b *Builder) normalizeValArg(e *ast.SQLVal) ast.Expr {
	if e.Type != ast.ValArg || b.bindCtx == nil {
		return e
	}
	name := strings.TrimPrefix(string(e.Val), ":")
	if b.bindCtx.Bindings == nil {
		err := fmt.Errorf("bind variable not provided: '%s'", name)
		b.handleErr(err)
	}
	bv, ok := b.bindCtx.GetSubstitute(name)
	if !ok {
		err := fmt.Errorf("bind variable not provided: '%s'", name)
		b.handleErr(err)
	}

	val, err := sqltypes.BindVariableToValue(bv)
	if err != nil {
		b.handleErr(err)
	}
	expr, err := ast.ExprFromValue(val)
	switch e := expr.(type) {
	case *ast.SQLVal:
		return e
	case *ast.NullVal:
		return e
	default:
		err := fmt.Errorf("unknown ast.Expr: %T", e)
		b.handleErr(err)
	}
	return nil
}

func (b *Builder) buildOrderBy(inScope, orderByScope *scope) {
	if len(orderByScope.cols) == 0 {
		return
	}
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
		sf := sql.SortField{
			Column: scalar,
			Order:  so,
		}
		sortFields = append(sortFields, sf)
	}
	sort := plan.NewSort(sortFields, inScope.node)
	inScope.node = sort
	return
}
