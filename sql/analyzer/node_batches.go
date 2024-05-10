package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// getBatchesForNode returns a partial analyzer ruleset for simple node
// types that require little prior validation before execution.
func getBatchesForNode(n sql.Node) ([]*Batch, bool) {
	switch n := n.(type) {
	case *plan.Commit:
		return nil, true
	case *plan.StartTransaction:
		return nil, true
	case *plan.InsertInto:
		if n.LiteralValueSource {
			return []*Batch{
				{
					Desc:       "simpleInsert",
					Iterations: 1,
					Rules: []Rule{
						{
							Id:    applyFKsId,
							Apply: applyForeignKeys,
						},
						{
							Id:    validatePrivilegesId,
							Apply: validatePrivileges,
						},
						{
							Id:    validateReadOnlyDatabaseId,
							Apply: validateReadOnlyDatabase,
						},
						{
							Id:    validateReadOnlyTransactionId,
							Apply: validateReadOnlyTransaction,
						},
					},
				},
				{
					Desc:       "onceAfterAll",
					Iterations: 1,
					Rules:      OnceAfterAll,
				},
			}, true
		}
	default:
		return nil, false
	}
}
