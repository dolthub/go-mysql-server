package analyzer

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func trackProcess(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	if !n.Resolved() {
		return n, nil
	}

	if _, ok := n.(*plan.QueryProcess); ok {
		return n, nil
	}

	var query string
	if x, ok := ctx.Value(sql.QueryKey).(string); ok {
		query = x
	}

	var typ = sql.QueryProcess
	if _, ok := n.(*plan.CreateIndex); ok {
		typ = sql.CreateIndexProcess
	}

	processList := a.Catalog.ProcessList
	pid := processList.AddProcess(ctx, typ, query)

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
			processList.AddProgressItem(pid, name, total)

			seen[name] = struct{}{}
			t := plan.NewProcessTable(n.Table, func() {
				processList.UpdateProgress(pid, name, 1)
			})

			return plan.NewResolvedTable(t), nil
		default:
			return n, nil
		}
	})
	if err != nil {
		return nil, err
	}

	return plan.NewQueryProcess(n, func() { processList.Done(pid) }), nil
}
