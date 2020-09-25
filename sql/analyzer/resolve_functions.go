package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// resolveFunctions replaces UnresolvedFunction nodes with equivalent functions from the Catalog.
func resolveFunctions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("resolve_functions")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		if n.Resolved() {
			return n, nil
		}

		return plan.TransformExpressionsUp(n, resolveFunctionsInExpr(a))
	})
}

func resolveFunctionsInExpr(a *Analyzer) sql.TransformExprFunc {
	return func(e sql.Expression) (sql.Expression, error) {
		if e.Resolved() {
			return e, nil
		}

		uf, ok := e.(*expression.UnresolvedFunction)
		if !ok {
			return e, nil
		}

		n := uf.Name()
		f, err := a.Catalog.Function(n)
		if err != nil {
			return nil, err
		}

		rf, err := f.Call(uf.Arguments...)
		if err != nil {
			return nil, err
		}

		a.Log("resolved function %q", n)
		return rf, nil
	}
}
