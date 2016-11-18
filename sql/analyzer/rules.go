package analyzer

import (
	"github.com/gitql/gitql/sql"
	"github.com/gitql/gitql/sql/expression"
	"github.com/gitql/gitql/sql/plan"
)

var DefaultRules = []Rule{
	{"resolve_tables", resolveTables},
	{"resolve_columns", resolveColumns},
}

func resolveTables(a *Analyzer, n sql.Node) sql.Node {
	return n.TransformUp(func(n sql.Node) sql.Node {
		t, ok := n.(*plan.UnresolvedTable)
		if !ok {
			return n
		}

		rt, err := a.Catalog.Table(a.CurrentDatabase, t.Name)

		if err != nil {
			return n
		}

		return rt
	})
}

func resolveColumns(a *Analyzer, n sql.Node) sql.Node {
	if n.Resolved() {
		return n
	}

	if len(n.Children()) != 1 {
		return n
	}

	child := n.Children()[0]

	colMap := map[string]*expression.GetField{}
	for idx, child := range child.Schema() {
		colMap[child.Name] = expression.NewGetField(idx, child.Type, child.Name)
	}

	return n.TransformExpressionsUp(func(e sql.Expression) sql.Expression {
		uc, ok := e.(*expression.UnresolvedColumn)
		if !ok {
			return e
		}

		gf, ok := colMap[uc.Name()]
		if !ok {
			return e
		}

		return gf
	})
}
