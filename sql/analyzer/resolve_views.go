package analyzer

import (
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/plan"
)

func resolveViews(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, _ := ctx.Span("resolve_views")
	defer span.Finish()

	a.Log("resolve views, node of type: %T", n)
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		a.Log("transforming node of type: %T", n)
		if n.Resolved() {
			return n, nil
		}

		t, ok := n.(*plan.UnresolvedTable)
		if !ok {
			return n, nil
		}

		name := t.Name()
		db := t.Database
		if db == "" {
			db = a.Catalog.CurrentDatabase()
		}

		view, err := a.Catalog.ViewRegistry.View(db, name)
		if err == nil {
			a.Log("view resolved: %q", name)
			return view.Definition(), nil
		}

		if sql.ErrNonExistingView.Is(err) {
			return n, nil
		}

		return nil, err
	})
}
