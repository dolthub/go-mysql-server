package analyzer

import (
	"fmt"
	"hash/crc32"
	"io"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func trackProcess(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	if !n.Resolved() {
		return n, nil
	}

	if _, ok := n.(*queryProcess); ok {
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
	wrapTable := func(name string, t sql.Table) (sql.Table, error) {
		if _, ok := seen[name]; ok {
			return t, nil
		}

		var total int64 = -1
		if counter, ok := t.(sql.PartitionCounter); ok {
			count, err := counter.PartitionCount(ctx)
			if err != nil {
				return nil, err
			}
			total = count
		}
		processList.AddProgressItem(pid, name, total)

		seen[name] = struct{}{}
		return &processTable{t, func() {
			processList.UpdateProgress(pid, name, 1)
		}}, nil
	}

	n, err := n.TransformUp(func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.ResolvedTable:
			if _, ok := n.Table.(*processTable); ok {
				return n, nil
			}

			t, err := wrapTable(n.Name(), n.Table)
			if err != nil {
				return nil, err
			}

			return plan.NewResolvedTable(n.Name(), t), nil
		case sql.Table:
			var name string
			if nameable, ok := n.(sql.Nameable); ok {
				name = nameable.Name()
			} else {
				name = fmt.Sprintf("unnamed[%d]", crc32.ChecksumIEEE([]byte(n.String())))
			}

			t, err := wrapTable(name, n)
			if err != nil {
				return nil, err
			}

			return plan.NewResolvedTable(name, t), nil
		default:
			return n, nil
		}
	})
	if err != nil {
		return nil, err
	}

	return &queryProcess{n, func() { processList.Done(pid) }}, nil
}

type queryProcess struct {
	sql.Node
	notify func()
}

func (p *queryProcess) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	n, err := p.Node.TransformUp(f)
	if err != nil {
		return nil, err
	}

	np := *p
	np.Node = n
	return &np, nil
}

func (p *queryProcess) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	n, err := p.Node.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	np := *p
	np.Node = n
	return &np, nil
}

func (p *queryProcess) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	iter, err := p.Node.RowIter(ctx)
	if err != nil {
		return nil, err
	}

	return &trackedRowIter{iter, p.notify}, nil
}

type processTable struct {
	sql.Table
	notify func()
}

func (t *processTable) Underlying() sql.Table {
	return t.Table
}

func (t *processTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	iter, err := t.Table.PartitionRows(ctx, p)
	if err != nil {
		return nil, err
	}

	return &trackedRowIter{iter, t.notify}, nil
}

type trackedRowIter struct {
	iter   sql.RowIter
	notify func()
}

func (i *trackedRowIter) done() {
	if i.notify != nil {
		i.notify()
		i.notify = nil
	}
}

func (i *trackedRowIter) Next() (sql.Row, error) {
	row, err := i.iter.Next()
	if err != nil {
		if err == io.EOF {
			i.done()
		}
		return nil, err
	}
	return row, nil
}

func (i *trackedRowIter) Close() error {
	i.done()
	return i.iter.Close()
}
