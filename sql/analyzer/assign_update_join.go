package analyzer

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// modifyUpdateExpressionsForJoin searches for a JOIN for UPDATE query and updates the child of the original update
// node to use a plan.UpdateJoin node as a child.
func modifyUpdateExpressionsForJoin(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	switch n := n.(type) {
	case *plan.Update:
		us, ok := n.Child.(*plan.UpdateSource)
		if !ok {
			return n, nil
		}

		var jn sql.Node
		plan.Inspect(us, func(node sql.Node) bool {
			switch node.(type) {
			case *plan.CrossJoin, plan.JoinNode:
				jn = node
				return false
			default:
				return true
			}
		})

		if jn == nil {
			return n, nil
		}

		if _, ok = jn.(*plan.InnerJoin); !ok {
			return n, sql.ErrUnsupportedFeature.New()
		}

		updaters, err := rowUpdatersByTable(ctx, us, jn)
		if err != nil {
			return nil, err
		}

		uj := plan.NewUpdateJoin(updaters, us)
		ret, err := n.WithChildren(uj)

		if err != nil {
			return nil, err
		}

		return ret, nil
	}

	return n, nil
}

// rowUpdatersByTable maps a set of tables to their RowUpdater objects.
func rowUpdatersByTable(ctx *sql.Context, node sql.Node, ij sql.Node) (map[string]sql.RowUpdater, error) {
	namesOfTableToBeUpdated := getTablesToBeUpdated(node)
	resolvedTables := getTablesByName(ij)

	ret := make(map[string]sql.RowUpdater)

	for k, v := range resolvedTables {
		if _, exists := namesOfTableToBeUpdated[k]; exists {
			updatable, ok := v.Table.(sql.UpdatableTable)
			if !ok {
				continue
			}

			keyless := sql.IsKeyless(updatable.Schema())
			if keyless {
				return nil, fmt.Errorf("error: keyless tables unsupported for UPDATE JOIN")
			}

			ret[k] = updatable.Updater(ctx)
		}
	}

	return ret, nil
}

// getTablesToBeUpdated takes a node and looks for the tables to modified by a SetField.
func getTablesToBeUpdated(node sql.Node) map[string]struct{} {
	ret := make(map[string]struct{})

	plan.InspectExpressions(node, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.SetField:
			gf := e.Left.(*expression.GetField)
			ret[gf.Table()] = struct{}{}
			return false
		}

		return true
	})

	return ret
}
