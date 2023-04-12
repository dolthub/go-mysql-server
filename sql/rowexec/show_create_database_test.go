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

package rowexec

import (
	"github.com/dolthub/go-mysql-server/sql/plan"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestShowCreateDatabase(t *testing.T) {
	require := require.New(t)

	node := plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), true)
	ctx := sql.NewEmptyContext()
	iter, err := DefaultBuilder.Build(ctx, node, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(ctx, nil, iter)
	require.NoError(err)

	require.Equal([]sql.Row{
		{"foo", "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `foo` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin */"},
	}, rows)

	node = plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), false)
	ctx = sql.NewEmptyContext()
	iter, err = DefaultBuilder.Build(ctx, node, nil)
	require.NoError(err)

	rows, err = sql.RowIterToRows(ctx, nil, iter)
	require.NoError(err)

	require.Equal([]sql.Row{
		{"foo", "CREATE DATABASE `foo` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin */"},
	}, rows)
}
