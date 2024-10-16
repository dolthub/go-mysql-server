package analyzer

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// modifyUpdateExprsForJoin searches for a JOIN for UPDATE query and updates the child of the original update
// node to use a plan.UpdateJoin node as a child.
func modifyUpdateExprsForJoin(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	switch n := n.(type) {
	case *plan.Update:
		us, ok := n.Child.(*plan.UpdateSource)
		if !ok {
			return n, transform.SameTree, nil
		}

		var jn sql.Node
		transform.Inspect(us, func(node sql.Node) bool {
			switch node.(type) {
			case *plan.JoinNode:
				jn = node
				return false
			default:
				return true
			}
		})

		if jn == nil {
			return n, transform.SameTree, nil
		}

		updaters, err := rowUpdatersByTable(ctx, us, jn)
		if err != nil {
			return nil, transform.SameTree, err
		}

		uj := plan.NewUpdateJoin(updaters, us)
		ret, err := n.WithChildren(uj)
		if err != nil {
			return nil, transform.SameTree, err
		}

		return ret, transform.NewTree, nil
	}

	return n, transform.SameTree, nil
}

// rowUpdatersByTable maps a set of tables to their RowUpdater objects.
func rowUpdatersByTable(ctx *sql.Context, node sql.Node, ij sql.Node) (map[string]sql.RowUpdater, error) {
	namesOfTableToBeUpdated := getTablesToBeUpdated(node)
	resolvedTables := getTablesByName(ij)

	rowUpdatersByTable := make(map[string]sql.RowUpdater)
	for tableToBeUpdated, _ := range namesOfTableToBeUpdated {
		resolvedTable, ok := resolvedTables[tableToBeUpdated]
		if !ok {
			return nil, plan.ErrUpdateForTableNotSupported.New(tableToBeUpdated)
		}

		var table = resolvedTable.UnderlyingTable()

		// If there is no UpdatableTable for a table being updated, error out
		updatable, ok := table.(sql.UpdatableTable)
		if !ok && updatable == nil {
			return nil, plan.ErrUpdateForTableNotSupported.New(tableToBeUpdated)
		}

		keyless := sql.IsKeyless(updatable.Schema())
		if keyless {
			return nil, sql.ErrUnsupportedFeature.New("error: keyless tables unsupported for UPDATE JOIN")
		}

		rowUpdatersByTable[tableToBeUpdated] = updatable.Updater(ctx)
	}

	return rowUpdatersByTable, nil
}

// getTablesToBeUpdated takes a node and looks for the tables to modified by a SetField.
func getTablesToBeUpdated(node sql.Node) map[string]struct{} {
	ret := make(map[string]struct{})

	transform.InspectExpressions(node, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.SetField:
			gf := e.LeftChild.(*expression.GetField)
			ret[strings.ToLower(gf.Table())] = struct{}{}
			return false
		}

		return true
	})

	return ret
}
