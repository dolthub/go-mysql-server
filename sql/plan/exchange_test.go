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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestExchange(t *testing.T) {
	children := NewProject(
		[]sql.Expression{
			expression.NewGetField(0, sql.Text, "partition", false),
			expression.NewArithmetic(
				expression.NewGetField(1, sql.Int64, "val", false),
				expression.NewLiteral(int64(1), sql.Int64),
				"+",
			),
		},
		NewFilter(
			expression.NewLessThan(
				expression.NewGetField(1, sql.Int64, "val", false),
				expression.NewLiteral(int64(4), sql.Int64),
			),
			&partitionable{nil, 3, 6},
		),
	)

	expected := []sql.Row{
		{"1", int64(2)},
		{"1", int64(3)},
		{"1", int64(4)},
		{"2", int64(2)},
		{"2", int64(3)},
		{"2", int64(4)},
		{"3", int64(2)},
		{"3", int64(3)},
		{"3", int64(4)},
	}

	for i := 1; i <= 4; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			require := require.New(t)

			exchange := NewExchange(i, children)
			ctx := sql.NewEmptyContext()
			iter, err := exchange.RowIter(ctx, nil)
			require.NoError(err)

			rows, err := sql.RowIterToRows(ctx, iter)
			require.NoError(err)
			require.ElementsMatch(expected, rows)
		})
	}
}

func TestExchangeCancelled(t *testing.T) {
	children := NewProject(
		[]sql.Expression{
			expression.NewGetField(0, sql.Text, "partition", false),
			expression.NewArithmetic(
				expression.NewGetField(1, sql.Int64, "val", false),
				expression.NewLiteral(int64(1), sql.Int64),
				"+",
			),
		},
		NewFilter(
			expression.NewLessThan(
				expression.NewGetField(1, sql.Int64, "val", false),
				expression.NewLiteral(int64(4), sql.Int64),
			),
			&partitionable{nil, 3, 2048},
		),
	)

	exchange := NewExchange(3, children)
	require := require.New(t)

	c, cancel := context.WithCancel(context.Background())
	ctx := sql.NewContext(c)
	cancel()

	iter, err := exchange.RowIter(ctx, nil)
	require.NoError(err)

	_, err = iter.Next(ctx)
	require.Equal(context.Canceled, err)
}

func TestExchangeIterPartitionsPanic(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	piter, err := (&partitionable{nil, 3, 2048}).Partitions(ctx)
	assert.NoError(t, err)
	closedCh := make(chan sql.Partition)
	close(closedCh)
	err = iterPartitions(ctx, piter, closedCh)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "panic")

	openCh := make(chan sql.Partition)
	err = iterPartitions(ctx, &partitionPanic{}, openCh)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "panic")
}

func TestExchangeIterPartitionRowsPanic(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	partitions := make(chan sql.Partition, 1)
	partitions <- Partition("test")
	err := iterPartitionRows(ctx, func(*sql.Context, sql.Partition) (sql.RowIter, error) {
		return &rowIterPanic{}, nil
	}, partitions, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "panic")

	closedCh := make(chan sql.Row)
	close(closedCh)
	partitions <- Partition("test")
	err = iterPartitionRows(ctx, func(*sql.Context, sql.Partition) (sql.RowIter, error) {
		return &partitionRows{Partition("test"), 10}, nil
	}, partitions, closedCh)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "panic")
}

type partitionable struct {
	sql.Node
	partitions       int
	rowsPerPartition int
}

// WithChildren implements the Node interface.
func (p *partitionable) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}

	return p, nil
}

// CheckPrivileges implements the interface sql.Node.
func (p *partitionable) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return p.Node.CheckPrivileges(ctx, opChecker)
}

func (partitionable) Children() []sql.Node { return nil }

func (p partitionable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return &exchangePartitionIter{int32(p.partitions)}, nil
}

func (p partitionable) PartitionRows(_ *sql.Context, part sql.Partition) (sql.RowIter, error) {
	return &partitionRows{part, int32(p.rowsPerPartition)}, nil
}

func (partitionable) Schema() sql.Schema {
	return sql.Schema{
		{Name: "partition", Type: sql.Text, Source: "foo"},
		{Name: "val", Type: sql.Int64, Source: "foo"},
	}
}

func (partitionable) Name() string { return "partitionable" }

type Partition string

func (p Partition) Key() []byte {
	return []byte(p)
}

type exchangePartitionIter struct {
	num int32
}

func (i *exchangePartitionIter) Next(*sql.Context) (sql.Partition, error) {
	new := atomic.AddInt32(&i.num, -1)
	if new < 0 {
		return nil, io.EOF
	}

	return Partition(fmt.Sprint(new + 1)), nil
}

func (i *exchangePartitionIter) Close(*sql.Context) error {
	atomic.StoreInt32(&i.num, -1)
	return nil
}

type partitionRows struct {
	sql.Partition
	num int32
}

func (r *partitionRows) Next(*sql.Context) (sql.Row, error) {
	new := atomic.AddInt32(&r.num, -1)
	if new < 0 {
		return nil, io.EOF
	}

	return sql.NewRow(string(r.Key()), int64(new+1)), nil
}

func (r *partitionRows) Close(*sql.Context) error {
	atomic.StoreInt32(&r.num, -1)
	return nil
}

type rowIterPanic struct {
}

func (*rowIterPanic) Next(*sql.Context) (sql.Row, error) {
	panic("i panic")
}

func (*rowIterPanic) Close(*sql.Context) error {
	return nil
}

type partitionPanic struct {
	sql.Partition
	closed bool
}

func (*partitionPanic) Next(*sql.Context) (sql.Partition, error) {
	panic("partitionPanic.Next")
}

func (p *partitionPanic) Close(_ *sql.Context) error {
	p.closed = true
	return nil
}
