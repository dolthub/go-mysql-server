package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func modifyUpdateExpressionsForJoin(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	ret := n
	plan.Inspect(n, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.Update:
			us, ok := n.Child.(*plan.UpdateSource)
			if !ok {
				return false
			}

			ij, ok := us.Child.(*plan.InnerJoin) // just have this return all of the updatable tables
			if !ok {
				return false
			}

			// do something with update source so it better applies the update expressions
			tableEditorMap := getTableEditorMap(ctx, ij, getUpdatableTables(ctx, us)) // TODO: Do we want to manage RowIters?

			uj := plan.NewUpdateJoin(tableEditorMap, us.Child)
			ret, _ = ret.WithChildren(uj)
			return false
		default:
			return false
		}
	})

	return ret, nil
}

func getTableEditorMap(ctx *sql.Context, ij *plan.InnerJoin, utMap map[string]sql.UpdatableTable) map[string]*plan.TableEditorIter {
	ret := make(map[string]*plan.TableEditorIter)
	for tableName, updatable := range utMap {
		iter, _ := ij.RowIter(ctx, sql.Row{})
		ret[tableName] = plan.NewUpdateIter(ctx, iter, sql.Schema{}, updatable.Updater(ctx), sql.CheckConstraints{}).(*plan.TableEditorIter)
	}

	return ret
}


func getUpdatableTables(ctx *sql.Context, node sql.Node) map[string]sql.UpdatableTable {
	namesOfTableToBeUpdated := getTablesToBeUpdated(ctx, node)
	resolvedTables := getResolvedTables(node)

	ret := make(map[string]sql.UpdatableTable)

	for k, v := range resolvedTables {
		if _, exists := namesOfTableToBeUpdated[k]; exists {
			ret[k] = v.Table.(sql.UpdatableTable)
		}
	}

	return ret
}

func getTablesToBeUpdated(ctx *sql.Context, node sql.Node) map[string]interface{} {
	ret := make(map[string]interface{})

	plan.InspectExpressions(node, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.GetField:
			ret[e.Table()] = true
			return false
		}

		return true
	})


	return ret
}