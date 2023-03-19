// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"fmt"
	"io"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/transform"
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

var _ sql.Node = (*Exchange)(nil)
var _ sql.Node2 = (*Exchange)(nil)

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
	transform.Inspect(e.Child, func(n sql.Node) bool {
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

	// How this is structured is a little subtle. A top-level
	// errgroup run |iterPartitions| and listens on the shutdown
	// hook.  A different, dependent, errgroup runs
	// |e.Parallelism| instances of |iterPartitionRows|. A
	// goroutine within the top-level errgroup |Wait|s on the
	// dependent errgroup and closes |rowsCh| once all its
	// goroutines are completed.

	partitionsCh := make(chan sql.Partition)
	rowsCh := make(chan sql.Row, e.Parallelism*16)

	eg, egCtx := ctx.NewErrgroup()
	eg.Go(func() error {
		defer close(partitionsCh)
		return iterPartitions(egCtx, partitions, partitionsCh)
	})

	// Spawn |iterPartitionRows| goroutines in the dependent
	// errgroup.
	getRowIter := e.getRowIterFunc(row)
	seg, segCtx := egCtx.NewErrgroup()
	for i := 0; i < e.Parallelism; i++ {
		seg.Go(func() error {
			return iterPartitionRows(segCtx, getRowIter, partitionsCh, rowsCh)
		})
	}

	eg.Go(func() error {
		defer close(rowsCh)
		err := seg.Wait()
		if err != nil {
			return err
		}
		// If everything in |seg| returned |nil|,
		// |iterPartitions| is done, |partitionsCh| is closed,
		// and every partition RowIter returned |EOF|. That
		// means we're EOF here.
		return io.EOF
	})

	waiter := func() error { return eg.Wait() }
	shutdownHook := newShutdownHook(eg, egCtx)
	return &exchangeRowIter{shutdownHook: shutdownHook, waiter: waiter, rows: rowsCh}, nil
}

// RowIter2 implements the sql.Node2 interface.
func (e *Exchange) RowIter2(ctx *sql.Context, f *sql.RowFrame) (sql.RowIter2, error) {
	var t sql.Table2
	transform.Inspect(e.Child, func(n sql.Node) bool {
		if table, ok := n.(sql.Table2); ok {
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

	// See notes on concurrency in RowIter
	partitionsCh := make(chan sql.Partition)
	rowsCh := make(chan sql.Row2, e.Parallelism*16)

	eg, egCtx := ctx.NewErrgroup()
	eg.Go(func() error {
		defer close(partitionsCh)
		return iterPartitions(egCtx, partitions, partitionsCh)
	})

	getRowIter := e.getRowIter2Func()
	seg, segCtx := egCtx.NewErrgroup()
	for i := 0; i < e.Parallelism; i++ {
		seg.Go(func() error {
			return iterPartitionRows2(segCtx, getRowIter, partitionsCh, rowsCh)
		})
	}

	eg.Go(func() error {
		defer close(rowsCh)
		err := seg.Wait()
		if err != nil {
			return err
		}
		// If everything in |seg| returned |nil|,
		// |iterPartitions| is done, |partitionsCh| is closed,
		// and every partition RowIter returned |EOF|. That
		// means we're EOF here.
		return io.EOF
	})

	waiter := func() error { return eg.Wait() }
	shutdownHook := newShutdownHook(eg, egCtx)
	return &exchangeRowIter{shutdownHook: shutdownHook, waiter: waiter, rows2: rowsCh}, nil
}

func (e *Exchange) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Exchange")
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

// CheckPrivileges implements the interface sql.Node.
func (e *Exchange) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return e.Child.CheckPrivileges(ctx, opChecker)
}

func (e *Exchange) getRowIterFunc(row sql.Row) func(*sql.Context, sql.Partition) (sql.RowIter, error) {
	return func(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
		node, _, err := transform.Node(e.Child, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
			if t, ok := n.(sql.Table); ok {
				return &exchangePartition{partition, t}, transform.NewTree, nil
			}
			return n, transform.SameTree, nil
		})
		if err != nil {
			return nil, err
		}
		return node.RowIter(ctx, row)
	}
}

func (e *Exchange) getRowIter2Func() func(*sql.Context, sql.Partition, *sql.RowFrame) (sql.RowIter2, error) {
	return func(ctx *sql.Context, partition sql.Partition, frame *sql.RowFrame) (sql.RowIter2, error) {
		node, _, err := transform.Node(e.Child, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
			if t, ok := n.(sql.Table); ok {
				return &exchangePartition{partition, t}, transform.NewTree, nil
			}
			return n, transform.SameTree, nil
		})
		if err != nil {
			return nil, err
		}
		return node.(sql.Node2).RowIter2(ctx, frame)
	}
}

// exchangeRowIter implements sql.RowIter for an exchange
// node. Calling |Next| reads off of |rows|, while calling |Close|
// calls |shutdownHook| and waits for exchange node workers to
// shutdown. If |rows| is closed, |Next| returns the error returned by
// |waiter|. |Close| returns the error returned by |waiter|, except it
// returns |nil| if |waiter| returns |io.EOF| or |shutdownHookErr|.
type exchangeRowIter struct {
	shutdownHook func()
	waiter       func() error
	rows         <-chan sql.Row
	rows2        <-chan sql.Row2
}

var _ sql.RowIter = (*exchangeRowIter)(nil)
var _ sql.RowIter2 = (*exchangeRowIter)(nil)

func (i *exchangeRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.rows == nil {
		panic("Next called for a Next2 iterator")
	}
	r, ok := <-i.rows
	if !ok {
		return nil, i.waiter()
	}
	return r, nil
}

func (i *exchangeRowIter) Next2(ctx *sql.Context, frame *sql.RowFrame) error {
	if i.rows2 == nil {
		panic("Next2 called for a Next iterator")
	}
	r, ok := <-i.rows2
	if !ok {
		return i.waiter()
	}

	frame.Append(r...)
	return nil
}

func (i *exchangeRowIter) Close(ctx *sql.Context) error {
	i.shutdownHook()
	err := i.waiter()
	if err == shutdownHookErr || err == io.EOF {
		return nil
	}
	return err
}

type exchangePartition struct {
	sql.Partition
	table sql.Table
}

var _ sql.Node = (*exchangePartition)(nil)
var _ sql.Node2 = (*exchangePartition)(nil)

func (p *exchangePartition) String() string {
	return fmt.Sprintf("Partition(%s)", string(p.Key()))
}

func (exchangePartition) Children() []sql.Node { return nil }

func (exchangePartition) Resolved() bool { return true }

func (p *exchangePartition) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return p.table.PartitionRows(ctx, p.Partition)
}

func (p *exchangePartition) RowIter2(ctx *sql.Context, f *sql.RowFrame) (sql.RowIter2, error) {
	return p.table.(sql.Table2).PartitionRows2(ctx, p.Partition)
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

// CheckPrivileges implements the interface sql.Node.
func (p *exchangePartition) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if node, ok := p.table.(sql.Node); ok {
		return node.CheckPrivileges(ctx, opChecker)
	}
	// If the table is not a ResolvedTable or other such node, then I guess we'll return true as to not fail.
	// This may not be the correct behavior though, as it's just a guess.
	return true
}

type rowIterPartitionFunc func(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error)
type rowIter2PartitionFunc func(ctx *sql.Context, partition sql.Partition, frame *sql.RowFrame) (sql.RowIter2, error)

func sendAllRows(ctx *sql.Context, iter sql.RowIter, rows chan<- sql.Row) (rowCount int, rerr error) {
	defer func() {
		cerr := iter.Close(ctx)
		if rerr == nil {
			rerr = cerr
		}
	}()
	for {
		r, err := iter.Next(ctx)
		if err == io.EOF {
			return rowCount, nil
		}
		if err != nil {
			return rowCount, err
		}
		rowCount++
		select {
		case rows <- r:
		case <-ctx.Done():
			return rowCount, ctx.Err()
		}
	}
}

func sendAllRows2(ctx *sql.Context, iter sql.RowIter2, rows chan<- sql.Row2, f *sql.RowFrame) (rowCount int, rerr error) {
	defer func() {
		cerr := iter.Close(ctx)
		if rerr == nil {
			rerr = cerr
		}
	}()
	for {
		f.Clear()
		err := iter.Next2(ctx, f)
		if err == io.EOF {
			return rowCount, nil
		}
		if err != nil {
			return rowCount, err
		}
		rowCount++
		select {
		case rows <- f.Row2Copy():
		case <-ctx.Done():
			return rowCount, ctx.Err()
		}
	}
}

// iterPartitionRows is the parallel worker for an Exchange node. It
// is meant to be run as a goroutine in an errgroup.Group. It will
// values read off of |partitions|. For each value it reads, it will
// call |getRowIter| to get a row projectIter, and will then call |Next| on
// that row projectIter, passing every row it gets into |rows|. If it
// receives an error at any point, it returns it. |iterPartitionRows|
// stops iterating and returns |nil| when |partitions| is closed.
func iterPartitionRows(ctx *sql.Context, getRowIter rowIterPartitionFunc, partitions <-chan sql.Partition, rows chan<- sql.Row) (rerr error) {
	defer func() {
		if r := recover(); r != nil {
			rerr = fmt.Errorf("panic in ExchangeIterPartitionRows: %v", r)
		}
	}()
	for {
		select {
		case p, ok := <-partitions:
			if !ok {
				return nil
			}
			span, ctx := ctx.Span("exchange.IterPartition")
			iter, err := getRowIter(ctx, p)
			if err != nil {
				return err
			}
			count, err := sendAllRows(ctx, iter, rows)
			span.SetAttributes(attribute.Int("num_rows", count))
			span.End()
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func iterPartitionRows2(ctx *sql.Context, getRowIter rowIter2PartitionFunc, partitions <-chan sql.Partition, rows chan<- sql.Row2) (rerr error) {
	defer func() {
		if r := recover(); r != nil {
			rerr = fmt.Errorf("panic in ExchangeIterPartitionRows2: %v", r)
		}
	}()
	for {
		select {
		case p, ok := <-partitions:
			if !ok {
				return nil
			}
			f := sql.NewRowFrame()

			span, ctx := ctx.Span("exchange.IterPartition")
			iter, err := getRowIter(ctx, p, f)
			if err != nil {
				return err
			}
			count, err := sendAllRows2(ctx, iter, rows, f)
			span.SetAttributes(attribute.Int("num_rows", count))
			span.End()
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// iterPartitions will call Next() on |iter| and send every result it
// finds to |partitions|.  Meant to be run as a goroutine in an
// errgroup, it returns a non-nil error if it gets an error and it
// return |ctx.Err()| if the context becomes Done().
func iterPartitions(ctx *sql.Context, iter sql.PartitionIter, partitions chan<- sql.Partition) (rerr error) {
	defer func() {
		if r := recover(); r != nil {
			rerr = fmt.Errorf("panic in iterPartitions: %v", r)
		}
	}()
	defer func() {
		cerr := iter.Close(ctx)
		if rerr == nil {
			rerr = cerr
		}
	}()
	for {
		p, err := iter.Next(ctx)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		select {
		case partitions <- p:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

var shutdownHookErr = fmt.Errorf("shutdown hook")

// newShutdownHook returns a |func()| that can be called to cancel the
// |ctx| associated with the supplied |eg|. It is safe to call the
// hook more than once.
//
// If an errgroup is shutdown with a shutdown hook, eg.Wait() will
// return |shutdownHookErr|. This can be used to consider requested
// shutdowns successful in some contexts, for example.
func newShutdownHook(eg *errgroup.Group, ctx context.Context) func() {
	stop := make(chan struct{})
	eg.Go(func() error {
		select {
		case <-stop:
			return shutdownHookErr
		case <-ctx.Done():
			return nil
		}
	})
	shutdownOnce := &sync.Once{}
	return func() {
		shutdownOnce.Do(func() {
			close(stop)
		})
	}
}
