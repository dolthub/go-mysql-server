package plan

import (
	"bytes"
	"encoding/gob"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestPushdownProjectionTable(t *testing.T) {
	require := require.New(t)
	memTable := mem.NewTable("table", sql.Schema{
		{Name: "a", Type: sql.Int64, Nullable: false},
		{Name: "b", Type: sql.Int64, Nullable: false},
		{Name: "c", Type: sql.Int64, Nullable: false},
	})

	table := NewPushdownProjectionTable(
		[]string{"a", "c"},
		&pushdownProjectionTable{memTable},
	)

	rows := collectRows(t, table)
	expected := []sql.Row{
		sql.Row{int64(1), nil, int64(1)},
		sql.Row{int64(2), nil, int64(2)},
		sql.Row{int64(3), nil, int64(3)},
		sql.Row{int64(4), nil, int64(4)},
	}

	require.Equal(expected, rows)
}

func TestPushdownProjectionAndFiltersTable(t *testing.T) {
	require := require.New(t)
	memTable := mem.NewTable("table", sql.Schema{
		{Name: "a", Type: sql.Int64, Nullable: false},
		{Name: "b", Type: sql.Int64, Nullable: false},
		{Name: "c", Type: sql.Int64, Nullable: false},
	})

	table := NewPushdownProjectionAndFiltersTable(
		[]sql.Expression{
			expression.NewGetField(0, sql.Int64, "a", false),
			expression.NewGetField(2, sql.Int64, "c", false),
		},
		[]sql.Expression{
			expression.NewNot(expression.NewEquals(
				expression.NewGetField(0, sql.Int64, "a", false),
				expression.NewLiteral(int64(1), sql.Int64),
			)),
			expression.NewNot(expression.NewEquals(
				expression.NewGetField(0, sql.Int64, "a", false),
				expression.NewLiteral(int64(3), sql.Int64),
			)),
		},
		&pushdownProjectionAndFiltersTable{memTable},
	)

	rows := collectRows(t, table)
	expected := []sql.Row{
		sql.Row{int64(2), nil, int64(2)},
		sql.Row{int64(4), nil, int64(4)},
	}

	require.Equal(expected, rows)
}

type pushdownProjectionTable struct {
	sql.Table
}

func (t *pushdownProjectionTable) WithProject(_ *sql.Context, cols []string) (sql.RowIter, error) {
	var fields []int
Loop:
	for i, col := range t.Schema() {
		for _, colName := range cols {
			if colName == col.Name {
				fields = append(fields, i)
				continue Loop
			}
		}
	}

	return &pushdownProjectionIter{len(t.Schema()), fields, 0}, nil
}

type pushdownProjectionIter struct {
	len    int
	fields []int
	iter   int64
}

func (it *pushdownProjectionIter) Next() (sql.Row, error) {
	if it.iter > 3 {
		return nil, io.EOF
	}

	var row = make(sql.Row, it.len)
	it.iter++
	for _, f := range it.fields {
		row[f] = it.iter
	}
	return row, nil
}

func (it *pushdownProjectionIter) Close() error {
	it.iter = 4
	return nil
}

type pushdownProjectionAndFiltersTable struct {
	sql.Table
}

func (pushdownProjectionAndFiltersTable) HandledFilters([]sql.Expression) []sql.Expression {
	panic("not implemented")
}

func (t *pushdownProjectionAndFiltersTable) WithProjectAndFilters(ctx *sql.Context, cols, filters []sql.Expression) (sql.RowIter, error) {
	var fields []int
Loop:
	for i, col := range t.Schema() {
		for _, c := range cols {
			if c, ok := c.(sql.Nameable); ok {
				if c.Name() == col.Name {
					fields = append(fields, i)
					continue Loop
				}
			}
		}
	}

	return &pushdownProjectionAndFiltersIter{
		&pushdownProjectionIter{len(t.Schema()), fields, 0},
		ctx,
		filters,
	}, nil
}

type pushdownProjectionAndFiltersIter struct {
	sql.RowIter
	ctx     *sql.Context
	filters []sql.Expression
}

func (it *pushdownProjectionAndFiltersIter) Next() (sql.Row, error) {
Loop:
	for {
		row, err := it.RowIter.Next()
		if err != nil {
			return nil, err
		}

		for _, f := range it.filters {
			result, err := f.Eval(it.ctx, row)
			if err != nil {
				return nil, err
			}

			if result != true {
				continue Loop
			}
		}

		return row, nil
	}
}

func TestPushdownIndexableTable(t *testing.T) {
	require := require.New(t)

	index := &indexLookup{[]interface{}{1, 2, 3}}
	filters := []sql.Expression{
		expression.NewLiteral(1, sql.Int64),
		expression.NewLiteral(2, sql.Int64),
	}
	columns := []sql.Expression{
		expression.NewLiteral(3, sql.Int64),
		expression.NewLiteral(4, sql.Int64),
	}

	table := &pushdownIndexableTable{nil, t, columns, filters, index, false}

	pushdownIndexableTable := NewIndexableTable(columns, filters, index, table)

	_, err := pushdownIndexableTable.RowIter(sql.NewEmptyContext())
	require.NoError(err)
	require.True(table.called)
}

type pushdownIndexableTable struct {
	sql.PushdownProjectionAndFiltersTable
	t                *testing.T
	columns, filters []sql.Expression
	index            sql.IndexLookup
	called           bool
}

func (t *pushdownIndexableTable) WithProjectFiltersAndIndex(
	ctx *sql.Context,
	columns, filters []sql.Expression,
	index sql.IndexValueIter,
) (sql.RowIter, error) {
	t.called = true
	require := require.New(t.t)
	require.Equal(t.columns, columns)
	require.Equal(t.filters, filters)
	values, err := t.index.Values()
	require.NoError(err)
	require.Equal(values, index)
	return sql.RowsToRowIter(), nil
}

func (t *pushdownIndexableTable) IndexKeyValueIter(_ *sql.Context, colNames []string) (sql.IndexKeyValueIter, error) {
	panic("not implemented")
}

func (t *pushdownIndexableTable) Name() string { return "name" }

type indexLookup struct {
	values []interface{}
}

func (l indexLookup) Values() (sql.IndexValueIter, error) {
	return &indexValueIter{l.values, 0}, nil
}

type indexValueIter struct {
	values []interface{}
	pos    int
}

func (i *indexValueIter) Next() ([]byte, error) {
	if i.pos >= len(i.values) {
		return nil, io.EOF
	}

	i.pos++

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(i.values[i.pos-1]); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (i *indexValueIter) Close() error {
	i.pos = len(i.values)
	return nil
}
