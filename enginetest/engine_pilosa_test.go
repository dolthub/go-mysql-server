// Copyright 2020 Liquidata, Inc.
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

// +build !windows

package enginetest_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/liquidata-inc/go-mysql-server/enginetest"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/index/pilosa"
	"github.com/liquidata-inc/go-mysql-server/test"

	"github.com/stretchr/testify/require"
)

type pilosaHarness struct {
	memoryHarness
	tmpDir string
}

var _ enginetest.IndexDriverHarness = (*pilosaHarness)(nil)

func (p *pilosaHarness) IndexDriver(dbs []sql.Database) sql.IndexDriver {
	return pilosa.NewDriver(p.tmpDir)
}

// TODO: we should run the entire enginetest suite against this harness, not just the tests below. But the integration
//  with pilosa is broken at the moment.
func newPilosaHarness(tmpDir string) *pilosaHarness {
	return &pilosaHarness{
		memoryHarness: *newDefaultMemoryHarness(),
		tmpDir: tmpDir,
	}
}

func TestIndexes(t *testing.T) {
	t.Skip("Pilosa integration is currently broken.")

	tmpDir, err := ioutil.TempDir(os.TempDir(), "pilosa-test")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(tmpDir, 0644))

	e, idxReg := enginetest.NewEngine(t, newPilosaHarness(tmpDir))
	viewReg := sql.NewViewRegistry()

	_, _, err = e.Query(
		enginetest.NewCtx(idxReg),
		"CREATE INDEX idx_i USING pilosa ON mytable (i)",
	)
	require.NoError(t, err)

	_, _, err = e.Query(
		enginetest.NewCtx(idxReg),
		"CREATE INDEX idx_s USING pilosa ON mytable (s)",
	)
	require.NoError(t, err)

	_, _, err = e.Query(
		enginetest.NewCtx(idxReg),
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
	t.Skip("Pilosa integration is currently broken.")

	require := require.New(t)

	tmpDir, err := ioutil.TempDir(os.TempDir(), "pilosa-test")
	require.NoError(err)
	require.NoError(os.MkdirAll(tmpDir, 0644))

	e, idxReg := enginetest.NewEngine(t, newPilosaHarness(tmpDir))

	_, iter, err := e.Query(enginetest.NewCtx(idxReg), "CREATE INDEX myidx USING pilosa ON mytable (i)")
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
