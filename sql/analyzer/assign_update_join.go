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

		updateJoinTargets, err := getTablesToBeUpdated(us, jn)
		if err != nil {
			return nil, transform.SameTree, err
		}
		ret := n.WithUpdateJoinTargets(updateJoinTargets)
		ret = ret.WithJoinSchema(jn.Schema())
		return ret, transform.NewTree, nil
	}

	return n, transform.SameTree, nil
}

func getTablesToBeUpdated(us sql.Node, jn sql.Node) ([]sql.Node, error) {
	namesOfTablesToBeUpdated := getNamesOfTablesToBeUpdated(us)
	resolvedTables := getTablesByName(jn)
	tablesToBeUpdated := make([]sql.Node, len(namesOfTablesToBeUpdated))

	for i, tableName := range namesOfTablesToBeUpdated {
		resolvedTable, ok := resolvedTables[tableName]
		if !ok {
			return nil, plan.ErrUpdateForTableNotSupported.New(tableName)
		}

		var table = resolvedTable.UnderlyingTable()

		updatable, ok := table.(sql.UpdatableTable)
		if !ok && updatable == nil {
			return nil, plan.ErrUpdateForTableNotSupported.New(tableName)
		}

		keyless := sql.IsKeyless(updatable.Schema())
		if keyless {
			return nil, sql.ErrUnsupportedFeature.New("error: keyless tables unsupported for UPDATE JOIN")
		}
		tablesToBeUpdated[i] = resolvedTable
	}

	return tablesToBeUpdated, nil
}

// getNamesOfTablesToBeUpdated takes a node and looks for the tables to modified by a SetField.
func getNamesOfTablesToBeUpdated(node sql.Node) []string {
	ret := make([]string, 0)

	transform.InspectExpressions(node, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.SetField:
			gf := e.LeftChild.(*expression.GetField)
			ret = append(ret, strings.ToLower(gf.Table()))
			return false
		}

		return true
	})

	return ret
}
