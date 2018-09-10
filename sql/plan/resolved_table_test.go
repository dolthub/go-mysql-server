package plan

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestResolvedTable(t *testing.T) {
	var require = require.New(t)

	table := NewResolvedTable(newTableTest("test"))
	require.NotNil(table)

	iter, err := table.RowIter(sql.NewEmptyContext())
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 9)

	tableTest, ok := table.Table.(*dummyTable)
	require.True(ok)

	for i, row := range rows {
		expected := tableTest.rows[i]
		require.ElementsMatch(expected, row)
	}
}

func newTableTest(source string) sql.Table {
	schema := []*sql.Column{
		{Name: "col1", Type: sql.Int32, Source: source, Default: int32(0), Nullable: false},
		{Name: "col2", Type: sql.Int64, Source: source, Default: int64(0), Nullable: false},
		{Name: "col3", Type: sql.Text, Source: source, Default: "", Nullable: false},
	}

	keys := [][]byte{
		[]byte("partition1"),
		[]byte("partition2"),
		[]byte("partition3"),
	}

	rows := []sql.Row{
		sql.NewRow(int32(1), int64(9), "one, nine"),
		sql.NewRow(int32(2), int64(8), "two, eight"),
		sql.NewRow(int32(3), int64(7), "three, seven"),
		sql.NewRow(int32(4), int64(6), "four, six"),
		sql.NewRow(int32(5), int64(5), "five, five"),
		sql.NewRow(int32(6), int64(4), "six, four"),
		sql.NewRow(int32(7), int64(3), "seven, three"),
		sql.NewRow(int32(8), int64(2), "eight, two"),
		sql.NewRow(int32(9), int64(1), "nine, one"),
	}

	partitions := map[string][]sql.Row{
		"partition1": []sql.Row{rows[0], rows[1], rows[2]},
		"partition2": []sql.Row{rows[3], rows[4], rows[5]},
		"partition3": []sql.Row{rows[6], rows[7], rows[8]},
	}

	return &dummyTable{schema, keys, partitions, rows}
}

type dummyTable struct {
	schema     sql.Schema
	keys       [][]byte
	partitions map[string][]sql.Row
	rows       []sql.Row
}

var _ sql.Table = (*dummyTable)(nil)

func (t *dummyTable) Name() string { return "dummy" }

func (t *dummyTable) String() string {
	panic("not implemented")
}

func (*dummyTable) Insert(*sql.Context, sql.Row) error {
	panic("not implemented")
}

func (t *dummyTable) Schema() sql.Schema { return t.schema }

func (t *dummyTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return &partitionIter{keys: t.keys}, nil
}

func (t *dummyTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	rows, ok := t.partitions[string(partition.Key())]
	if !ok {
		return nil, fmt.Errorf(
			"partition not found: %q", partition.Key(),
		)
	}

	return sql.RowsToRowIter(rows...), nil
}

type partition struct {
	key []byte
}

func (p *partition) Key() []byte { return p.key }

type partitionIter struct {
	keys [][]byte
	pos  int
}

func (p *partitionIter) Next() (sql.Partition, error) {
	if p.pos >= len(p.keys) {
		return nil, io.EOF
	}

	key := p.keys[p.pos]
	p.pos++
	return &partition{key}, nil
}

func (p *partitionIter) Close() error { return nil }
