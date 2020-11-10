package plan

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func ApplyBindings(n sql.Node, bindings map[string]sql.Expression) (sql.Node, error) {
	return TransformExpressionsUp(n, func(e sql.Expression) (sql.Expression, error) {
		if bv, ok := e.(*expression.BindVar); ok {
			val, found := bindings[bv.Name]
			if found {
				return val, nil
			} else {
				panic(fmt.Sprintf("%s, %v", bv.Name, bindings))
			}
		}
		return e, nil
	})
}
