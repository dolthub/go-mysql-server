package plan

import (
	"fmt"
	"io"
	"sync"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// ErrNoPartitionable is returned when no Partitionable node is found
// in the Exchange tree.
var ErrNoPartitionable = errors.NewKind("no partitionable node found in exchange tree")

// Exchange is a node that can parallelize the underlying tree iterating
// partitions concurrently.
type Exchange struct {
	UnaryNode
	Parallelism int
}

// NewExchange creates a new Exchange node.
func NewExchange(
	parallelism int,
	child sql.Node,
) *Exchange {
	return &Exchange{
		UnaryNode:   UnaryNode{Child: child},
		Parallelism: parallelism,
	}
}

// RowIter implements the sql.Node interface.
func (e *Exchange) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	var t sql.Table
	Inspect(e.Child, func(n sql.Node) bool {
		if table, ok := n.(sql.Table); ok {
			t = table
			return false
		}
		return true
	})
	if t == nil {
		return nil, ErrNoPartitionable.New()
	}

	partitions, err := t.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	return newExchangeRowIter(ctx, e.Parallelism, partitions, e.Child), nil
}

func (e *Exchange) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Exchange(parallelism=%d)", e.Parallelism)
	_ = p.WriteChildren(e.Child.String())
	return p.String()
}

// TransformUp implements the sql.Node interface.
func (e *Exchange) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	child, err := e.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewExchange(e.Parallelism, child))
}

// TransformExpressionsUp implements the sql.Node interface.
func (e *Exchange) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	child, err := e.Child.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	return NewExchange(e.Parallelism, child), nil
}

type exchangeRowIter struct {
	ctx         *sql.Context
	parallelism int
	partitions  sql.PartitionIter
	tree        sql.Node
	mut         sync.Mutex
	tokens      chan struct{}
	started     bool
	rows        chan sql.Row
	err         chan error
	quit        chan struct{}
}

func newExchangeRowIter(
	ctx *sql.Context,
	parallelism int,
	iter sql.PartitionIter,
	tree sql.Node,
) *exchangeRowIter {
	return &exchangeRowIter{
		ctx:         ctx,
		parallelism: parallelism,
		rows:        make(chan sql.Row, parallelism),
		err:         make(chan error),
		started:     false,
		tree:        tree,
		partitions:  iter,
		quit:        make(chan struct{}),
	}
}

func (it *exchangeRowIter) releaseToken() {
	it.mut.Lock()
	defer it.mut.Unlock()

	if it.tokens != nil {
		it.tokens <- struct{}{}
	}
}

func (it *exchangeRowIter) closeTokens() {
	it.mut.Lock()
	defer it.mut.Unlock()

	close(it.tokens)
	it.tokens = nil
}

func (it *exchangeRowIter) fillTokens() {
	it.mut.Lock()
	defer it.mut.Unlock()

	it.tokens = make(chan struct{}, it.parallelism)
	for i := 0; i < it.parallelism; i++ {
		it.tokens <- struct{}{}
	}
}

func (it *exchangeRowIter) start() {
	it.fillTokens()

	var partitions = make(chan sql.Partition)
	go it.iterPartitions(partitions)

	var wg sync.WaitGroup

	for {
		select {
		case <-it.ctx.Done():
			it.closeTokens()
			return
		case <-it.quit:
			it.closeTokens()
			return
		case p, ok := <-partitions:
			if !ok {
				it.closeTokens()

				wg.Wait()
				close(it.rows)
				return
			}

			wg.Add(1)
			go func(p sql.Partition) {
				it.iterPartition(p)
				wg.Done()

				it.releaseToken()
			}(p)
		}
	}
}

func (it *exchangeRowIter) iterPartitions(ch chan<- sql.Partition) {
	defer func() {
		close(ch)

		if err := it.partitions.Close(); err != nil {
			it.err <- err
		}
	}()

	for {
		select {
		case <-it.ctx.Done():
			return
		case <-it.quit:
			return
		case <-it.tokens:
		}

		p, err := it.partitions.Next()
		if err != nil {
			if err != io.EOF {
				it.err <- err
			}
			return
		}

		ch <- p
	}
}

func (it *exchangeRowIter) iterPartition(p sql.Partition) {
	node, err := it.tree.TransformUp(func(n sql.Node) (sql.Node, error) {
		if t, ok := n.(sql.Table); ok {
			return &exchangePartition{p, t}, nil
		}

		return n, nil
	})
	if err != nil {
		it.err <- err
		return
	}

	rows, err := node.RowIter(it.ctx)
	if err != nil {
		it.err <- err
		return
	}

	defer func() {
		if err := rows.Close(); err != nil {
			it.err <- err
		}
	}()

	for {
		select {
		case <-it.ctx.Done():
			return
		case <-it.quit:
			return
		default:
		}

		row, err := rows.Next()
		if err != nil {
			if err == io.EOF {
				break
			}

			it.err <- err
			return
		}

		it.rows <- row
	}
}

func (it *exchangeRowIter) Next() (sql.Row, error) {
	if !it.started {
		it.started = true
		go it.start()
	}

	select {
	case row, ok := <-it.rows:
		if !ok {
			return nil, io.EOF
		}
		return row, nil
	case err := <-it.err:
		_ = it.Close()
		return nil, err
	}
}

func (it *exchangeRowIter) Close() error {
	if it.quit == nil {
		return nil
	}

	close(it.quit)
	return nil
}

type exchangePartition struct {
	sql.Partition
	table sql.Table
}

var _ sql.Node = (*exchangePartition)(nil)

func (p *exchangePartition) String() string {
	return fmt.Sprintf("Partition(%s)", string(p.Key()))
}

func (exchangePartition) Children() []sql.Node { return nil }

func (exchangePartition) Resolved() bool { return true }

func (p *exchangePartition) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	return p.table.PartitionRows(ctx, p.Partition)
}

func (p *exchangePartition) Schema() sql.Schema {
	return p.table.Schema()
}

func (p *exchangePartition) TransformExpressionsUp(sql.TransformExprFunc) (sql.Node, error) {
	return p, nil
}

func (p *exchangePartition) TransformUp(sql.TransformNodeFunc) (sql.Node, error) {
	return p, nil
}
