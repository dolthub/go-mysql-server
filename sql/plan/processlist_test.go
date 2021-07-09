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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestShowProcessList(t *testing.T) {
	require := require.New(t)

	addr := "127.0.0.1:34567"

	n := NewShowProcessList()
	p := sql.NewProcessList()
	sess := sql.NewSession("0.0.0.0:3306", sql.Client{Address: addr, User: "foo"}, 1)
	ctx := sql.NewContext(context.Background(), sql.WithPid(1), sql.WithSession(sess))

	ctx, err := p.AddProcess(ctx, "SELECT foo")
	require.NoError(err)

	p.AddTableProgress(ctx.Pid(), "a", 5)
	p.AddTableProgress(ctx.Pid(), "b", 6)

	ctx = sql.NewContext(context.Background(), sql.WithPid(2), sql.WithSession(sess))
	ctx, err = p.AddProcess(ctx, "SELECT bar")
	require.NoError(err)

	p.AddTableProgress(ctx.Pid(), "foo", 2)

	p.UpdateTableProgress(1, "a", 3)
	p.UpdateTableProgress(1, "a", 1)
	p.UpdatePartitionProgress(1, "a", "a-1", 7)
	p.UpdatePartitionProgress(1, "a", "a-2", 9)
	p.UpdateTableProgress(1, "b", 2)
	p.UpdateTableProgress(2, "foo", 1)

	n.ProcessList = p
	n.Database = "foo"

	iter, err := n.RowIter(ctx, nil)
	require.NoError(err)
	rows, err := sql.RowIterToRows(ctx, iter)
	require.NoError(err)

	expected := []sql.Row{
		{int64(1), "foo", addr, "foo", "Query", int64(0),
			`
a (4/5 partitions)
 ├─ a-1 (7/? rows)
 └─ a-2 (9/? rows)

b (2/6 partitions)
`, "SELECT foo"},
		{int64(1), "foo", addr, "foo", "Query", int64(0), "\nfoo (1/2 partitions)\n", "SELECT bar"},
	}

	require.ElementsMatch(expected, rows)
}
