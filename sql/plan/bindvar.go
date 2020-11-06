package plan

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func ApplyBindings(n sql.Node, bindings map[string]sql.Expression) (sql.Node, error) {
	return TransformExpressionsUp(n, func(e sql.Expression) (sql.Expression, error) {
		if bv, ok := e.(*expression.BindVar); ok {
			val, found := bindings[bv.Name]
			if found {
				return val, nil
			}
		}
		return e, nil
	})
}
