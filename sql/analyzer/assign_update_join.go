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
			tableEditorMap := getTableEditorMap(ctx, ij, getUpdatableTables(ctx, us, ij)) // TODO: Do we want to manage RowIters?

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


func getUpdatableTables(ctx *sql.Context, node sql.Node, ij *plan.InnerJoin) map[string]sql.UpdatableTable {
	namesOfTableToBeUpdated := getTablesToBeUpdated(ctx, node)
	resolvedTables := getResolvedTableFromJoinStruct(ij)

	ret := make(map[string]sql.UpdatableTable)

	for k, v := range resolvedTables {
		if _, exists := namesOfTableToBeUpdated[k]; exists {
			ret[k] = v.Table.(sql.UpdatableTable)
		}
	}

	return ret
}

func getResolvedTableFromJoinStruct(node *plan.InnerJoin) map[string]*plan.ResolvedTable {
	toProcess := make([]sql.Node, 0)
	toProcess = append(toProcess, node)
	ret := make(map[string]*plan.ResolvedTable)
	for len(toProcess) > 0 {
		head := toProcess[0]
		children := head.Children()
		toProcess = toProcess[1:]

		if len(children) == 0 {
			continue
		}

		for _, child := range children {
			toAdd := getResolvedTable(child)
			if toAdd != nil {
				ret[toAdd.Name()] = toAdd
			}
			toProcess = append(toProcess, child)
		}
	}

	return ret
}

func getTablesToBeUpdated(ctx *sql.Context, node sql.Node) map[string]interface{} {
	ret := make(map[string]interface{})

	plan.InspectExpressions(node, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.GetField: // TODO: This should change to SetField to be more accurate but its fine for now
			ret[e.Table()] = true
			return false
		}

		return true
	})


	return ret
}