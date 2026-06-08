// Copyright 2020-2026 Dolthub, Inc.
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

package rowexec

import (
	"context"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

// https://github.com/dolthub/go-mysql-server/issues/3560
func TestUnclosedCachedResultIterDoesNotLeakMemory(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := sql.NewContext(context.TODO(), sql.WithMemoryManager(
		sql.NewMemoryManager(mockReporter{2, 1}),
	), sql.WithSession(memory.NewSession(sql.NewBaseSession(), pro)))

	table := memory.NewTable(ctx, db.BaseDatabase, "left", lSchema, nil)
	insertData(t, ctx, table)
	node := plan.NewCachedResults(plan.NewResolvedTable(table, db, nil))

	iter, err := NewBuilder(nil, sql.EngineOverrides{}).Build(ctx, node, nil)
	require.NoError(err)
	require.Equal(0, ctx.Memory.NumCaches())

	_, err = iter.Next(ctx)
	require.NoError(err)
	require.Equal(0, ctx.Memory.NumCaches())
}
