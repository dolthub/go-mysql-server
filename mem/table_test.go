package mem

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestTableName(t *testing.T) {
	require := require.New(t)
	s := sql.Schema{
		{"col1", sql.Text, nil, true, ""},
	}
	table := NewTable("test", s)
	require.Equal("test", table.Name())
}

const expectedString = `Table(foo)
 ├─ Column(col1, TEXT, nullable=true)
 └─ Column(col2, INT64, nullable=false)
`

func TestTableString(t *testing.T) {
	require := require.New(t)
	table := NewTable("foo", sql.Schema{
		{"col1", sql.Text, nil, true, ""},
		{"col2", sql.Int64, nil, false, ""},
	})
	require.Equal(expectedString, table.String())
}

func TestTableInsert(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	s := sql.Schema{
		{"col1", sql.Text, nil, true, ""},
	}

	table := NewTable("test", s)

	rows, err := sql.NodeToRows(ctx, table)
	require.NoError(err)
	require.Len(rows, 0)

	err = table.Insert(sql.NewEmptyContext(), sql.NewRow("foo"))
	rows, err = sql.NodeToRows(ctx, table)
	require.NoError(err)
	require.Len(rows, 1)
	require.Nil(s.CheckRow(rows[0]))

	err = table.Insert(sql.NewEmptyContext(), sql.NewRow("bar"))
	rows, err = sql.NodeToRows(ctx, table)
	require.NoError(err)
	require.Len(rows, 2)
	require.Nil(s.CheckRow(rows[0]))
	require.Nil(s.CheckRow(rows[1]))
}

func TestTableIndexKeyValueIter(t *testing.T) {
	require := require.New(t)

	table := NewTable("foo", sql.Schema{
		{Name: "foo", Type: sql.Text},
		{Name: "bar", Type: sql.Int64},
	})

	rows := []sql.Row{
		sql.NewRow("foo", int64(1)),
		sql.NewRow("bar", int64(2)),
		sql.NewRow("baz", int64(3)),
	}

	for _, r := range rows {
		require.NoError(table.Insert(sql.NewEmptyContext(), r))
	}

	iter, err := table.IndexKeyValueIter(sql.NewEmptyContext(), []string{"bar"})
	require.NoError(err)

	type result struct {
		key    int64
		values []interface{}
	}

	var obtained []result
	for {
		values, data, err := iter.Next()
		if err == io.EOF {
			break
		}

		var r result
		require.NoError(binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &r.key))
		r.values = values
		obtained = append(obtained, r)
	}

	var expected = []result{
		{0, []interface{}{int64(1)}},
		{1, []interface{}{int64(2)}},
		{2, []interface{}{int64(3)}},
	}

	require.Equal(expected, obtained)
}

func TestTableIndex(t *testing.T) {
	require := require.New(t)

	table := NewTable("foo", sql.Schema{
		{Name: "foo", Type: sql.Text},
		{Name: "bar", Type: sql.Int64},
	})

	rows := []sql.Row{
		sql.NewRow("foo", int64(1)),
		sql.NewRow("bar", int64(2)),
		sql.NewRow("baz", int64(3)),
		sql.NewRow("qux", int64(4)),
	}

	for _, r := range rows {
		require.NoError(table.Insert(sql.NewEmptyContext(), r))
	}

	index := &index{keys: []int64{1, 2}}

	it, err := table.WithProjectFiltersAndIndex(sql.NewEmptyContext(), nil, nil, index)
	require.NoError(err)

	result, err := sql.RowIterToRows(it)
	require.NoError(err)

	expected := []sql.Row{
		{"bar", int64(2)},
		{"baz", int64(3)},
	}

	require.Equal(expected, result)
}

type index struct {
	keys []int64
	pos  int
}

func (i *index) Next() ([]byte, error) {
	if i.pos >= len(i.keys) {
		return nil, io.EOF
	}

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, i.keys[i.pos]); err != nil {
		return nil, err
	}

	i.pos++
	return buf.Bytes(), nil
}

func (i *index) Close() error {
	i.pos = len(i.keys)
	return nil
}
