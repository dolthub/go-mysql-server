package plan

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func ApplyBindings(n sql.Node, bindings map[string]sql.Expression) (sql.Node, error) {
        withSubqueries, err := TransformUp(n, func(n sql.Node) (sql.Node, error) {
                switch n := n.(type) {
                case *SubqueryAlias:
                        child, err := ApplyBindings(n.Child, bindings)
                        if err != nil {
                                return nil, err
                        }
                        return n.WithChildren(child)
                default:
                        return n, nil
                }
        })
	if err != nil {
		return nil, err
	}
	return TransformExpressionsUp(withSubqueries, func(e sql.Expression) (sql.Expression, error) {
		if bv, ok := e.(*expression.BindVar); ok {
			val, found := bindings[bv.Name]
			if found {
				return val, nil
			}
		}
		return e, nil
	})
}
