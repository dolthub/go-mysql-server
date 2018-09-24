package analyzer

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func shouldParallelize(node sql.Node) bool {
	// Do not try to parallelize index operations.
	switch node.(type) {
	case *plan.CreateIndex, *plan.DropIndex, *plan.Describe:
		return false
	default:
		return true
	}
}

func parallelize(ctx *sql.Context, a *Analyzer, node sql.Node) (sql.Node, error) {
	if a.Parallelism <= 1 || !node.Resolved() {
		return node, nil
	}

	proc, ok := node.(*plan.QueryProcess)
	if (ok && !shouldParallelize(proc.Child)) || !shouldParallelize(node) {
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
		// These are the only unary nodes that can be parallelized. Any other
		// unary nodes will not.
		case *plan.Filter,
			*plan.Project,
			*plan.TableAlias,
			*plan.Exchange:
		case sql.Table:
			lastWasTable = true
			tableSeen = true
		default:
			ok = false
			return false
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
