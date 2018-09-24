package analyzer

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

// trackProcess will wrap the query in a process node and add progress items
// to the already existing process.
func trackProcess(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	if !n.Resolved() {
		return n, nil
	}

	if _, ok := n.(*plan.QueryProcess); ok {
		return n, nil
	}

	processList := a.Catalog.ProcessList

	var seen = make(map[string]struct{})
	n, err := n.TransformUp(func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.ResolvedTable:
			if _, ok := n.Table.(*plan.ProcessTable); ok {
				return n, nil
			}

			name := n.Table.Name()
			if _, ok := seen[name]; ok {
				return n, nil
			}

			var total int64 = -1
			if counter, ok := n.Table.(sql.PartitionCounter); ok {
				count, err := counter.PartitionCount(ctx)
				if err != nil {
					return nil, err
				}
				total = count
			}
			processList.AddProgressItem(ctx.Pid(), name, total)

			seen[name] = struct{}{}
			t := plan.NewProcessTable(n.Table, func() {
				processList.UpdateProgress(ctx.Pid(), name, 1)
			})

			return plan.NewResolvedTable(t), nil
		default:
			return n, nil
		}
	})
	if err != nil {
		return nil, err
	}

	// Don't wrap CreateIndex in a QueryProcess, as it is a CreateIndexProcess.
	// CreateIndex will take care of marking the process as done on its own.
	if _, ok := n.(*plan.CreateIndex); ok {
		return n, nil
	}

	return plan.NewQueryProcess(n, func() { processList.Done(ctx.Pid()) }), nil
}
