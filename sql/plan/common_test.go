package plan

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

var benchtable = func() *mem.Table {
	schema := sql.Schema{
		{Name: "strfield", Type: sql.Text, Nullable: true},
		{Name: "floatfield", Type: sql.Float64, Nullable: true},
		{Name: "boolfield", Type: sql.Boolean, Nullable: false},
		{Name: "intfield", Type: sql.Int32, Nullable: false},
		{Name: "bigintfield", Type: sql.Int64, Nullable: false},
		{Name: "blobfield", Type: sql.Blob, Nullable: false},
	}
	t := mem.NewTable("test", schema)

	for i := 0; i < 100; i++ {
		n := fmt.Sprint(i)
		err := t.Insert(
			sql.NewEmptyContext(),
			sql.NewRow(
				repeatStr(n, i%10+1),
				float64(i),
				i%2 == 0,
				int32(i),
				int64(i),
				[]byte(repeatStr(n, 100+(i%100))),
			),
		)
		if err != nil {
			panic(err)
		}

		if i%2 == 0 {
			err := t.Insert(
				sql.NewEmptyContext(),
				sql.NewRow(
					repeatStr(n, i%10+1),
					float64(i),
					i%2 == 0,
					int32(i),
					int64(i),
					[]byte(repeatStr(n, 100+(i%100))),
				),
			)
			if err != nil {
				panic(err)
			}
		}
	}

	return t
}()

func repeatStr(str string, n int) string {
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		buf.WriteString(str)
	}
	return buf.String()
}

func assertRows(t *testing.T, iter sql.RowIter, expected int64) {
	t.Helper()
	require := require.New(t)

	var rows int64
	for {
		_, err := iter.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			require.NoError(err)
		}

		rows++
	}

	require.Equal(expected, rows)
}

func collectRows(t *testing.T, node sql.Node) []sql.Row {
	t.Helper()
	ctx := sql.NewEmptyContext()

	iter, err := node.RowIter(ctx)
	require.NoError(t, err)

	var rows []sql.Row
	for {
		row, err := iter.Next()
		if err == io.EOF {
			return rows
		}
		require.NoError(t, err)
		rows = append(rows, row)
	}
}

func TestIsUnary(t *testing.T) {
	require := require.New(t)
	table := mem.NewTable("foo", nil)

	require.True(IsUnary(NewFilter(nil, NewResolvedTable(table))))
	require.False(IsUnary(NewCrossJoin(
		NewResolvedTable(table),
		NewResolvedTable(table),
	)))
}

func TestIsBinary(t *testing.T) {
	require := require.New(t)
	table := mem.NewTable("foo", nil)

	require.False(IsBinary(NewFilter(nil, NewResolvedTable(table))))
	require.True(IsBinary(NewCrossJoin(
		NewResolvedTable(table),
		NewResolvedTable(table),
	)))
}
