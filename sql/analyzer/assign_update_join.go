package analyzer

import (
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

		ij, ok := us.Child.(*plan.InnerJoin)
		if !ok {
			return n, nil
		}

		uj := plan.NewUpdateJoin(getRowUpdaterMap(ctx, us, ij), us)
		ret, err := n.WithChildren(uj)

		if err != nil {
			return nil, err
		}

		return ret, nil
	}

	return n, nil
}

// getRowUpdaterMap returns a maps set of tables to their RowUpdater objects.
func getRowUpdaterMap(ctx *sql.Context, node sql.Node, ij *plan.InnerJoin) map[string]sql.RowUpdater {
	namesOfTableToBeUpdated := getTablesToBeUpdated(node)
	resolvedTables := getResolvedTableFromJoin(ij)

	ret := make(map[string]sql.RowUpdater)

	for k, v := range resolvedTables {
		if _, exists := namesOfTableToBeUpdated[k]; exists {
			ret[k] = v.Table.(sql.UpdatableTable).Updater(ctx)
		}
	}

	return ret
}

// getResolvedTableFromJoin returns all resolved tables present in a join node
func getResolvedTableFromJoin(node *plan.InnerJoin) map[string]*plan.ResolvedTable {
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

// getTablesTobeUpdated takes a node and looks for the tables to modified by a SetField.
func getTablesToBeUpdated(node sql.Node) map[string]interface{} {
	ret := make(map[string]interface{})

	plan.InspectExpressions(node, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.SetField:
			gf := e.Left.(*expression.GetField)
			ret[gf.Table()] = true
			return false
		}

		return true
	})

	return ret
}
