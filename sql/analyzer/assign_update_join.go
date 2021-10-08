package analyzer

import (
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

var ErrNonUpdateInnerJoinNotSupports = errors.NewKind("error: Only INNER JOINs are support for Update currently")

// modifyUpdateExpressionsForJoin searches for a JOIN for UPDATE query and updates the child of the original update
// node to use a plan.UpdateJoin node as a child.
func modifyUpdateExpressionsForJoin(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	switch n := n.(type) {
	case *plan.Update:
		us, ok := n.Child.(*plan.UpdateSource)
		if !ok {
			return n, nil
		}

		jn, ok := us.Child.(plan.JoinNode)
		if !ok {
			return n, nil
		}

		if _, ok = jn.(*plan.InnerJoin); !ok {
			return n, ErrNonUpdateInnerJoinNotSupports.New()
		}

		uj := plan.NewUpdateJoin(getRowUpdaterMap(ctx, us, jn), us)
		ret, err := n.WithChildren(uj)

		if err != nil {
			return nil, err
		}

		return ret, nil
	}

	return n, nil
}

// getRowUpdaterMap maps a set of tables to their RowUpdater objects.
func getRowUpdaterMap(ctx *sql.Context, node sql.Node, ij plan.JoinNode) map[string]sql.RowUpdater {
	namesOfTableToBeUpdated := getTablesToBeUpdated(node)
	resolvedTables := getResolvedTableFromJoin(ij)

	ret := make(map[string]sql.RowUpdater)

	for k, v := range resolvedTables {
		if _, exists := namesOfTableToBeUpdated[k]; exists {
			updatable, ok := v.Table.(sql.UpdatableTable)
			if ok {
				ret[k] = updatable.Updater(ctx)
			}
		}
	}

	return ret
}

// getResolvedTableFromJoin returns all resolved tables present in a join node
func getResolvedTableFromJoin(node plan.JoinNode) map[string]*plan.ResolvedTable {
	toProcess := []sql.Node{node}
	ret := make(map[string]*plan.ResolvedTable)
	for len(toProcess) > 0 {
		head := toProcess[0]
		toProcess = toProcess[1:]

		children := head.Children()
		if len(children) == 0 {
			continue
		}

		for _, child := range children {
			toAdd := getResolvedTable(child)
			if toAdd != nil {
				ret[toAdd.Name()] = toAdd
			} else {
				toProcess = append(toProcess, child)
			}
		}
	}

	return ret
}

// getTablesToBeUpdated takes a node and looks for the tables to modified by a SetField.
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
