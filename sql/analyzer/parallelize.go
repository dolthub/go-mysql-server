package analyzer

import (
	"strconv"

	"github.com/go-kit/kit/metrics/discard"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

var (
	// ParallelQueryCounter describes a metric that accumulates
	// number of parallel queries monotonically.
	ParallelQueryCounter = discard.NewCounter()
)

func shouldParallelize(node sql.Node, scope *Scope) bool {
	// Don't parallelize subqueries, this can blow up the execution graph quickly
	if len(scope.Schema()) > 0 {
		return false
	}

	// Do not try to parallelize DDL or descriptive operations
	return !isDdlNode(node)
}

// isDdlNode returns whether the node given is a DDL operation, which includes things like SHOW commands. In general,
// these are nodes that interact only with schema and the catalog, not with any table rows.
func isDdlNode(node sql.Node) bool {
	switch node.(type) {
	case *plan.CreateTable, *plan.DropTable, *plan.Truncate,
		*plan.AddColumn, *plan.ModifyColumn, *plan.DropColumn,
		*plan.RenameTable, *plan.RenameColumn,
		*plan.CreateIndex, *plan.AlterIndex, *plan.DropIndex,
		*plan.CreateForeignKey, *plan.DropForeignKey,
		*plan.CreateTrigger, *plan.DropTrigger,
		*plan.ShowTables, *plan.ShowCreateTable,
		*plan.ShowTriggers, *plan.ShowCreateTrigger,
		*plan.ShowDatabases, *plan.ShowCreateDatabase,
		*plan.ShowColumns, *plan.ShowIndexes,
		*plan.ShowProcessList, *plan.ShowTableStatus,
		*plan.ShowVariables, *plan.ShowWarnings:
		return true
	default:
		return false
	}
}

func parallelize(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (sql.Node, error) {
	if a.Parallelism <= 1 || !node.Resolved() {
		return node, nil
	}

	proc, ok := node.(*plan.QueryProcess)
	if (ok && !shouldParallelize(proc.Child, nil)) || !shouldParallelize(node, scope) {
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
		// IndexedTablesAccess already uses an index for lookups, so parallelizing it won't help in most cases (and can
		// blow up the query execution graph)
		case *plan.IndexedTableAccess:
			ok = false
			return false
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
