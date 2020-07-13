package analyzer

import (
	"reflect"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

var (
	// ErrUnionSchemasDifferentLength is returned when the two sides of a
	// UNION do not have the same number of columns in their schemas.
	ErrUnionSchemasDifferentLength = errors.NewKind(
		"cannot union two queries whose schemas are different lengths; left has %d column(s) right has %d column(s).",
	)
)

func mergeUnionSchemas(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	if !n.Resolved() {
		return n, nil
	}
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		if u, ok := n.(*plan.Union); ok {
			ls, rs := u.Left.Schema(), u.Right.Schema()
			if len(ls) != len(rs) {
				return nil, ErrUnionSchemasDifferentLength.New(len(ls), len(rs))
			}
			les, res := make([]sql.Expression, len(ls)), make([]sql.Expression, len(rs))
			hasdiff := false
			for i := range ls {
				les[i] = expression.NewGetFieldWithTable(i, ls[i].Type, ls[i].Source, ls[i].Name, ls[i].Nullable)
				res[i] = expression.NewGetFieldWithTable(i, rs[i].Type, rs[i].Source, rs[i].Name, rs[i].Nullable)
				if reflect.DeepEqual(ls[i].Type, rs[i].Type) {
					continue
				}
				hasdiff = true

				// TODO: Principled type coercion...
				les[i] = expression.NewConvert(les[i], expression.ConvertToChar)
				res[i] = expression.NewConvert(res[i], expression.ConvertToChar)

				// Preserve schema names across the conversion.
				les[i] = expression.NewAlias(ls[i].Name, les[i])
				res[i] = expression.NewAlias(rs[i].Name, res[i])
			}
			if hasdiff {
				return u.WithChildren(
					plan.NewProject(les, u.Left),
					plan.NewProject(res, u.Right),
				)
			} else {
				return u, nil
			}
		}
		return n, nil
	})
}
