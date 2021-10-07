package analyzer

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func modifyUpdateExpressionsForJoin(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	plan.Inspect(n, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.Update:
			us, ok := n.Child.(*plan.UpdateSource)
			if !ok {
				return false
			}

			ij, ok := us.Child.(*plan.InnerJoin)
			if !ok {
				return false
			}


			// TODO: Replace the Update's child with UdateJoinSource w/ child UpdateSource

			// do something with update source so it better applies the update expressions
			fmt.Println(ij)
			return false
		default:
			return false
		}
	})

	return n, nil
}
//
//func getUpdatableTables(updateExprs []sql.Expression) map[string]sql.UpdatableTable {
//	ret := make(map[string]sql.UpdatableTable)
//
//	// iterate through all update Expressions to see the tables being modified
//	for _, ue := range updateExprs {
//		sf := ue.(*expression.SetField)
//		gf := sf.Left.(*expression.GetField)
//
//
//	}
//
//}