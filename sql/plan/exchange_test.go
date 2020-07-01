package plan

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
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
			iter, err := exchange.RowIter(sql.NewEmptyContext())
			require.NoError(err)

			rows, err := sql.RowIterToRows(iter)
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
			&partitionable{nil, 3, 6},
		),
	)

	exchange := NewExchange(3, children)
	require := require.New(t)

	c, cancel := context.WithCancel(context.Background())
	ctx := sql.NewContext(c)
	cancel()

	iter, err := exchange.RowIter(ctx)
	require.NoError(err)

	_, err = iter.Next()
	require.Equal(context.Canceled, err)
}

func TestExchangePanicRecover(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	it := &partitionPanic{}
	ex := newExchangeRowIter(ctx, 1, it, nil)
	ex.start()
	it.Close()

	require.True(t, it.closed)
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

func (partitionable) Children() []sql.Node { return nil }

func (p partitionable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return &exchangePartitionIter{p.partitions}, nil
}

func (p partitionable) PartitionRows(_ *sql.Context, part sql.Partition) (sql.RowIter, error) {
	return &partitionRows{part, p.rowsPerPartition}, nil
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
	num int
}

func (i *exchangePartitionIter) Next() (sql.Partition, error) {
	if i.num <= 0 {
		return nil, io.EOF
	}

	i.num--
	return Partition(fmt.Sprint(i.num + 1)), nil
}

func (i *exchangePartitionIter) Close() error {
	i.num = -1
	return nil
}

type partitionRows struct {
	sql.Partition
	num int
}

func (r *partitionRows) Next() (sql.Row, error) {
	if r.num <= 0 {
		return nil, io.EOF
	}

	r.num--
	return sql.NewRow(string(r.Key()), int64(r.num+1)), nil
}

func (r *partitionRows) Close() error {
	r.num = -1
	return nil
}

type partitionPanic struct {
	sql.Partition
	closed bool
}

func (*partitionPanic) Next() (sql.Partition, error) {
	panic("partitionPanic.Next")
}

func (p *partitionPanic) Close() error {
	p.closed = true
	return nil
}
