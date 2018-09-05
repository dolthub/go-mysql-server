package analyzer

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func parallelize(ctx *sql.Context, a *Analyzer, node sql.Node) (sql.Node, error) {
	if a.Parallelism <= 1 || !node.Resolved() {
		return node, nil
	}

	// Do not try to parallelize index operations.
	switch node.(type) {
	case *plan.CreateIndex, *plan.DropIndex:
		return node, nil
	}

	node, err := node.TransformUp(func(node sql.Node) (sql.Node, error) {
		if !isParallelizable(node) {
			return node, nil
		}

		return plan.NewExchange(a.Parallelism, node), nil
	})

	if err != nil {
		return nil, err
	}

	return node.TransformUp(removeRedundantExchanges)
}

// removeRedundantExchanges removes all the exchanges except for the topmost
// of all.
func removeRedundantExchanges(node sql.Node) (sql.Node, error) {
	exchange, ok := node.(*plan.Exchange)
	if !ok {
		return node, nil
	}

	e := &protectedExchange{exchange}
	return e.TransformUp(func(node sql.Node) (sql.Node, error) {
		if exchange, ok := node.(*plan.Exchange); ok {
			return exchange.Child, nil
		}
		return node, nil
	})
}

func isParallelizable(node sql.Node) bool {
	var ok = true
	var tableSeen bool
	var lastWasTable bool

	plan.Inspect(node, func(node sql.Node) bool {
		if node == nil {
			return true
		}

		lastWasTable = false
		if plan.IsBinary(node) {
			ok = false
			return false
		}

		switch node.(type) {
		// These nodes, even if they're unary, can't be parallelized because
		// they need all the rows to be effectively computed.
		case *plan.Limit,
			*plan.GroupBy,
			*plan.Sort,
			*plan.Offset,
			*plan.Distinct,
			*plan.OrderedDistinct:
			ok = false
			return false
		case sql.Table:
			lastWasTable = true
			tableSeen = true
		}

		return true
	})

	return ok && tableSeen && lastWasTable
}

// protectedExchange is a placeholder node that protects a certain exchange
// node from being removed during transformations.
type protectedExchange struct {
	*plan.Exchange
}

// TransformUp transforms the child with the given transform function but it
// will not call the transform function with the new instance. Instead of
// another protectedExchange, it will return an Exchange.
func (e *protectedExchange) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := e.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return plan.NewExchange(e.Parallelism, child), nil
}
