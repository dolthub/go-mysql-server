package planbuilder

import (
	"fmt"
	"strings"

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
			c, ok := projScope.resolveColumn(strings.ToLower(e.Qualifier.String()), strings.ToLower(e.Name.String()), false)
			if ok {
				c.descending = descending
				outScope.addColumn(c)
				continue
			}

			// fromScope col
			c, ok = fromScope.resolveColumn(strings.ToLower(e.Qualifier.String()), strings.ToLower(e.Name.String()), true)
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
			if e.Type == ast.IntVal {
				lit := b.convertInt(string(e.Val), 10)
				idx, _, err := types.Int64.Convert(lit.Value())
				if err != nil {
					b.handleErr(err)
				}
				intIdx, ok := idx.(int64)
				if !ok {
					b.handleErr(fmt.Errorf("expected integer order by literal"))
				}
				if intIdx < 1 {
					b.handleErr(fmt.Errorf("expected positive integer order by literal"))
				}
				if projScope == nil || len(projScope.cols) == 0 {
					err := fmt.Errorf("invalid order by ordinal context")
					b.handleErr(err)
				}
				target := projScope.cols[intIdx-1]
				scalar := target.scalar
				if scalar == nil {
					scalar = target.scalarGf()
				}
				outScope.addColumn(scopeColumn{
					table:      target.table,
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
					c, ok := fromScope.resolveColumn(strings.ToLower(e.Table()), strings.ToLower(e.Name()), true)
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
				table:      "",
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
		sf := sql.SortField{
			Column: c.scalar,
			Order:  so,
		}
		sortFields = append(sortFields, sf)
	}
	sort := plan.NewSort(sortFields, inScope.node)
	inScope.node = sort
	return
}
