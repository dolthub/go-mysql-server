package analyzer

import (
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

func clearWarnings(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (sql.Node, error) {
	children := node.Children()
	if len(children) == 0 {
		return node, nil
	}

	switch ch := children[0].(type) {
	case plan.ShowWarnings:
		return node, nil
	case *plan.Offset:
		clearWarnings(ctx, a, ch, scope)
		return node, nil
	case *plan.Limit:
		clearWarnings(ctx, a, ch, scope)
		return node, nil
	}

	ctx.ClearWarnings()
	return node, nil
}
