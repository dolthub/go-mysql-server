package plan

import (
	"context"
	"fmt"
	"io"
	"sync"

	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
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
func (e *Exchange) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
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

	return newExchangeRowIter(ctx, e.Parallelism, partitions, row, e.Child), nil
}

func (e *Exchange) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Exchange(parallelism=%d)", e.Parallelism)
	_ = p.WriteChildren(e.Child.String())
	return p.String()
}

func (e *Exchange) DebugString() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Exchange(parallelism=%d)", e.Parallelism)
	_ = p.WriteChildren(sql.DebugString(e.Child))
	return p.String()
}

// WithChildren implements the Node interface.
func (e *Exchange) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}

	return NewExchange(e.Parallelism, children[0]), nil
}

type exchangeRowIter struct {
	ctx         *sql.Context
	parallelism int
	partitions  sql.PartitionIter
	row         sql.Row
	tree        sql.Node
	mut         sync.RWMutex
	tokensChan  chan struct{}
	started     bool
	rows        chan sql.Row
	err         chan error

	quitMut  sync.RWMutex
	quitChan chan struct{}
}

func newExchangeRowIter(
	ctx *sql.Context,
	parallelism int,
	iter sql.PartitionIter,
	row sql.Row,
	tree sql.Node,
) *exchangeRowIter {
	return &exchangeRowIter{
		ctx:         ctx,
		parallelism: parallelism,
		rows:        make(chan sql.Row, parallelism),
		err:         make(chan error, 1),
		started:     false,
		tree:        tree,
		partitions:  iter,
		row:         row,
		quitChan:    make(chan struct{}),
	}
}

func (it *exchangeRowIter) releaseToken() {
	it.mut.Lock()
	defer it.mut.Unlock()

	if it.tokensChan != nil {
		it.tokensChan <- struct{}{}
	}
}

func (it *exchangeRowIter) closeTokens() {
	it.mut.Lock()
	defer it.mut.Unlock()

	close(it.tokensChan)
	it.tokensChan = nil
}

func (it *exchangeRowIter) tokens() chan struct{} {
	it.mut.RLock()
	defer it.mut.RUnlock()
	return it.tokensChan
}

func (it *exchangeRowIter) fillTokens() {
	it.mut.Lock()
	defer it.mut.Unlock()

	it.tokensChan = make(chan struct{}, it.parallelism)
	for i := 0; i < it.parallelism; i++ {
		it.tokensChan <- struct{}{}
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
			it.err <- context.Canceled
			it.closeTokens()
			return
		case <-it.quit():
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
		if x := recover(); x != nil {
			it.err <- fmt.Errorf("mysql_server caught panic:\n%v", x)
		}

		close(ch)
	}()

	for {
		select {
		case <-it.ctx.Done():
			it.err <- context.Canceled
			return
		case <-it.quit():
			return
		case <-it.tokens():
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
	span, ctx := it.ctx.Span("exchange.IterPartition")
	rowCount := 0
	defer func() {
		span.LogKV("num_rows", rowCount)
		span.Finish()
	}()

	node, err := TransformUp(it.tree, func(n sql.Node) (sql.Node, error) {
		if t, ok := n.(sql.Table); ok {
			return &exchangePartition{p, t}, nil
		}

		return n, nil
	})
	if err != nil {
		it.err <- err
		return
	}

	rows, err := node.RowIter(ctx, it.row)
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
			it.err <- context.Canceled
			return
		case <-it.quit():
			return
		default:
		}

		var row sql.Row
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic in iterPartition: %v", r)
				}
			}()
			row, err = rows.Next()
		}()
		if err != nil {
			if err == io.EOF {
				break
			}

			it.err <- err
			return
		}

		rowCount++
		it.rows <- row
	}
}

func (it *exchangeRowIter) Next() (sql.Row, error) {
	if !it.started {
		it.started = true
		go it.start()
	}

	select {
	case err := <-it.err:
		_ = it.Close()
		return nil, err
	case row, ok := <-it.rows:
		if !ok {
			return nil, io.EOF
		}
		return row, nil
	}
}

func (it *exchangeRowIter) quit() chan struct{} {
	it.quitMut.RLock()
	defer it.quitMut.RUnlock()
	return it.quitChan
}

func (it *exchangeRowIter) Close() error {
	it.quitMut.Lock()
	if it.quitChan != nil {
		close(it.quitChan)
		it.quitChan = nil
	}
	it.quitMut.Unlock()

	if it.partitions != nil {
		return it.partitions.Close()
	}

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

func (p *exchangePartition) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return p.table.PartitionRows(ctx, p.Partition)
}

func (p *exchangePartition) Schema() sql.Schema {
	return p.table.Schema()
}

// WithChildren implements the Node interface.
func (p *exchangePartition) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}

	return p, nil
}
