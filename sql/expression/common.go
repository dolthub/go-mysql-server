package expression

import "gopkg.in/sqle/sqle.v0/sql"

type UnaryExpression struct {
	Child sql.Expression
}

func (p UnaryExpression) Resolved() bool {
	return p.Child.Resolved()
}

func (p UnaryExpression) IsNullable() bool {
	return p.Child.IsNullable()
}

type BinaryExpression struct {
	Left  sql.Expression
	Right sql.Expression
}

func (p BinaryExpression) Resolved() bool {
	return p.Left.Resolved() && p.Right.Resolved()
}

func (p BinaryExpression) IsNullable() bool {
	return p.Left.IsNullable() || p.Right.IsNullable()
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
