package planbuilder

import (
	"fmt"
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func (b *PlanBuilder) analyzeOrderBy(fromScope, projScope *scope, order ast.OrderBy) (outScope *scope) {
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
			c, ok = fromScope.resolveColumn(strings.ToLower(e.Qualifier.String()), strings.ToLower(e.Name.String()), false)
			if !ok {
				err := sql.ErrColumnNotFound.New(e.Name)
				b.handleErr(err)
			}
			c.descending = descending
			c.scalar = expression.NewGetFieldWithTable(int(c.id), c.typ, c.table, c.col, c.nullable)
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
					scalar = expression.NewGetFieldWithTable(int(target.id), target.typ, target.table, target.col, target.nullable)
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
			// we could add to aggregates here, ref GF in aggOut
			expr := b.buildScalar(fromScope, e)
			col := scopeColumn{
				table:      "",
				col:        expr.String(),
				scalar:     expr,
				typ:        expr.Type(),
				nullable:   expr.IsNullable(),
				descending: descending,
			}
			_, ok := outScope.getExpr(expr.String())
			if !ok {
				outScope.newColumn(col)
			}
		}
	}
	return
}

func (b *PlanBuilder) buildOrderBy(inScope, orderByScope *scope) {
	// TODO build Sort node over input
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
