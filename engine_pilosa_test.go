// +build !windows

package sqle_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/index/pilosa"
	"github.com/liquidata-inc/go-mysql-server/test"

	"github.com/stretchr/testify/require"
)

func TestIndexes(t *testing.T) {
	e, idxReg := newEngine(t)
	viewReg := sql.NewViewRegistry()

	tmpDir, err := ioutil.TempDir(os.TempDir(), "pilosa-test")
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(tmpDir, 0644))

	idxReg.RegisterIndexDriver(pilosa.NewDriver(tmpDir))

	_, _, err = e.Query(
		newCtx(idxReg),
		"CREATE INDEX idx_i USING pilosa ON mytable (i)",
	)
	require.NoError(t, err)

	_, _, err = e.Query(
		newCtx(idxReg),
		"CREATE INDEX idx_s USING pilosa ON mytable (s)",
	)
	require.NoError(t, err)

	_, _, err = e.Query(
		newCtx(idxReg),
		"CREATE INDEX idx_is USING pilosa ON mytable (i, s)",
	)
	require.NoError(t, err)

	defer func() {
		done, err := idxReg.DeleteIndex("mydb", "idx_i", true)
		require.NoError(t, err)
		<-done

		done, err = idxReg.DeleteIndex("mydb", "idx_s", true)
		require.NoError(t, err)
		<-done

		done, err = idxReg.DeleteIndex("foo", "idx_is", true)
		require.NoError(t, err)
		<-done
	}()

	testCases := []struct {
		query    string
		expected []sql.Row
	}{
		{
			"SELECT * FROM mytable WHERE i = 2",
			[]sql.Row{
				{int64(2), "second row"},
			},
		},
		{
			"SELECT * FROM mytable WHERE i > 1",
			[]sql.Row{
				{int64(3), "third row"},
				{int64(2), "second row"},
			},
		},
		{
			"SELECT * FROM mytable WHERE i < 3",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(2), "second row"},
			},
		},
		{
			"SELECT * FROM mytable WHERE i <= 2",
			[]sql.Row{
				{int64(2), "second row"},
				{int64(1), "first row"},
			},
		},
		{
			"SELECT * FROM mytable WHERE i >= 2",
			[]sql.Row{
				{int64(2), "second row"},
				{int64(3), "third row"},
			},
		},
		{
			"SELECT * FROM mytable WHERE i = 2 AND s = 'second row'",
			[]sql.Row{
				{int64(2), "second row"},
			},
		},
		{
			"SELECT * FROM mytable WHERE i = 2 AND s = 'third row'",
			([]sql.Row)(nil),
		},
		{
			"SELECT * FROM mytable WHERE i BETWEEN 1 AND 2",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(2), "second row"},
			},
		},
		{
			"SELECT * FROM mytable WHERE i = 1 OR i = 2",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(2), "second row"},
			},
		},
		{
			"SELECT * FROM mytable WHERE i = 1 AND i = 2",
			([]sql.Row)(nil),
		},
		{
			"SELECT i as mytable_i FROM mytable WHERE mytable_i = 2",
			[]sql.Row{
				{int64(2)},
			},
		},
		{
			"SELECT i as mytable_i FROM mytable WHERE mytable_i > 1",
			[]sql.Row{
				{int64(3)},
				{int64(2)},
			},
		},
		{
			"SELECT i as mytable_i, s as mytable_s FROM mytable WHERE mytable_i = 2 AND mytable_s = 'second row'",
			[]sql.Row{
				{int64(2), "second row"},
			},
		},
		{
			"SELECT s, SUBSTRING(s, 1, 1) AS sub_s FROM mytable WHERE sub_s = 's'",
			[]sql.Row{
				{"second row", "s"},
			},
		},
		{
			"SELECT count(i) AS mytable_i, SUBSTR(s, -3) AS mytable_s FROM mytable WHERE i > 0 AND mytable_s='row' GROUP BY mytable_s",
			[]sql.Row{
				{int64(3), "row"},
			},
		},
		{
			"SELECT mytable_i FROM (SELECT i AS mytable_i FROM mytable) as t WHERE mytable_i > 1",
			[]sql.Row{
				{int64(2)},
				{int64(3)},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.query, func(t *testing.T) {
			require := require.New(t)

			tracer := new(test.MemTracer)
			ctx := sql.NewContext(context.TODO(), sql.WithTracer(tracer), sql.WithIndexRegistry(idxReg), sql.WithViewRegistry(viewReg)).WithCurrentDB("mydb")

			_, it, err := e.Query(ctx, tt.query)
			require.NoError(err)

			rows, err := sql.RowIterToRows(it)
			require.NoError(err)

			require.ElementsMatch(tt.expected, rows)
			require.Equal("plan.ResolvedTable", tracer.Spans[len(tracer.Spans)-1])
		})
	}
}

func TestCreateIndex(t *testing.T) {
	require := require.New(t)
	e, idxReg := newEngine(t)

	tmpDir, err := ioutil.TempDir(os.TempDir(), "pilosa-test")
	require.NoError(err)

	require.NoError(os.MkdirAll(tmpDir, 0644))
	idxReg.RegisterIndexDriver(pilosa.NewDriver(tmpDir))

	_, iter, err := e.Query(newCtx(idxReg), "CREATE INDEX myidx USING pilosa ON mytable (i)")
	require.NoError(err)
	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 0)

	defer func() {
		time.Sleep(1 * time.Second)
		done, err := idxReg.DeleteIndex("foo", "myidx", true)
		require.NoError(err)
		<-done

		require.NoError(os.RemoveAll(tmpDir))
	}()
}
