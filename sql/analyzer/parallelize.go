package analyzer

import (
	"strconv"

	"github.com/go-kit/kit/metrics/discard"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/plan"
)

var (
	// ParallelQueryCounter describes a metric that accumulates
	// number of parallel queries monotonically.
	ParallelQueryCounter = discard.NewCounter()
)

func shouldParallelize(node sql.Node) bool {
	// Do not try to parallelize index operations or schema operations
	switch node.(type) {
	case *plan.AlterIndex, *plan.CreateIndex, *plan.Describe, *plan.DropIndex, *plan.ShowCreateTable:
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

	node, err := plan.TransformUp(node, func(node sql.Node) (sql.Node, error) {
		if !isParallelizable(node) {
			return node, nil
		}
		ParallelQueryCounter.With("parallelism", strconv.Itoa(a.Parallelism)).Add(1)

		return plan.NewExchange(a.Parallelism, node), nil
	})

	if err != nil {
		return nil, err
	}

	return plan.TransformUp(node, removeRedundantExchanges)
}

// removeRedundantExchanges removes all the exchanges except for the topmost
// of all.
func removeRedundantExchanges(node sql.Node) (sql.Node, error) {
	exchange, ok := node.(*plan.Exchange)
	if !ok {
		return node, nil
	}

	child, err := plan.TransformUp(exchange.Child, func(node sql.Node) (sql.Node, error) {
		if exchange, ok := node.(*plan.Exchange); ok {
			return exchange.Child, nil
		}
		return node, nil
	})
	if err != nil {
		return nil, err
	}

	return exchange.WithChildren(child)
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
