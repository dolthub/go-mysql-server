package expression

import "gopkg.in/sqle/sqle.v0/sql"

type UnaryExpression struct {
	Child sql.Expression
}

func (p UnaryExpression) Resolved() bool {
	return p.Child.Resolved()
}

type BinaryExpression struct {
	Left  sql.Expression
	Right sql.Expression
}

func (p BinaryExpression) Resolved() bool {
	return p.Left.Resolved() && p.Right.Resolved()
}

var defaultFunctions = map[string]interface{}{
	"count": NewCount,
	"first": NewFirst,
}

func RegisterDefaults(c *sql.Catalog) error {
	for k, v := range defaultFunctions {
		if err := c.RegisterFunction(k, v); err != nil {
			return err
		}
	}

	return nil
}
