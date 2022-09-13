package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// TODO: come up with a better name
// fakeBlobIndex adds
func fakeBlobIndex(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	hasBlobIndex := false
	transform.Inspect(node, func(n sql.Node) bool {
		if resTbl, ok := n.(*plan.ResolvedTable); ok {
			sch := resTbl.Schema()
			if sch == nil {
			}

			for _, c := range sch {
				if sql.IsTextBlob(c.Type) && c.PrimaryKey {
					hasBlobIndex = true
					return false
				}
			}

		}

		return true
	})

	if !hasBlobIndex {
		return node, transform.SameTree, nil
	}

	hasSort := false
	transform.Inspect(node, func(n sql.Node) bool {
		_, ok := n.(*plan.Sort)
		if ok {
			hasSort = true
			return false
		}
		return true
	})

	if hasSort {
		return node, transform.SameTree, nil
	}

	return node, transform.SameTree, nil
}
