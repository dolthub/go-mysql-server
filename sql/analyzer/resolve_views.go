package analyzer

import (
	"fmt"
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
			db = ctx.GetCurrentDatabase()
		}

		view, err := ctx.View(db, name)
		if err == nil {
			a.Log("view resolved: %q", name)

			// If this view is being asked for with an AS OF clause, then attempt to apply it to every table in the view.
			if t.AsOf != nil || t.Database != "" {
				a.Log("applying AS OF clause and database qualifier to view definition")

				// TODO: this direct editing of children is necessary because the view definition is declared as an opaque node,
				//  meaning that plan.TransformUp won't touch its children. It's only supposed to be touched by the
				//  resolve_subqueries function, which invokes the entire analyzer on the node. This is the only place we have
				//  to make this exception so far, but there may be others.
				children := view.Definition().Children()
				if len(children) == 1 {
					child, err := plan.TransformUp(children[0], func(n2 sql.Node) (sql.Node, error) {
						t2, ok := n2.(*plan.UnresolvedTable)
						if !ok {
							return n2, nil
						}

						if t.AsOf != nil {
							a.Log("applying AS OF clause to view " + t2.Name())
							if t2.AsOf != nil {
								return nil, sql.ErrIncompatibleAsOf.New(
									fmt.Sprintf("cannot combine AS OF clauses %s and %s",
										t.AsOf.String(), t2.AsOf.String()))
							}
							t2, _ = t2.WithAsOf(t.AsOf)
						}

						if t.Database != "" {
							a.Log("applying database clause to view " + t2.Name())
							if t2.Database == "" {
								t2, _ = t2.WithDatabase(db)
							}
						}

						return t2, nil
					})

					if err != nil {
						return nil, err
					}

					return view.Definition().WithChildren(child)
				}
			}

			return view.Definition(), nil
		}

		if sql.ErrNonExistingView.Is(err) {
			return n, nil
		}

		return nil, err
	})
}
