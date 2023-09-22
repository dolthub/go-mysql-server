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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestOffsetPlan(t *testing.T) {
	require := require.New(t)

	db, table, _ := getTestingTable(t)
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	offset := plan.NewOffset(expression.NewLiteral(0, types.Int8), plan.NewResolvedTable(table, nil, nil))
	require.Equal(1, len(offset.Children()))

	iter, err := DefaultBuilder.Build(ctx, offset, nil)
	require.NoError(err)
	require.NotNil(iter)
}

func TestOffset(t *testing.T) {
	require := require.New(t)

	db, table, n := getTestingTable(t)
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	offset := plan.NewOffset(expression.NewLiteral(1, types.Int8), plan.NewResolvedTable(table, nil, nil))

	iter, err := DefaultBuilder.Build(ctx, offset, nil)
	require.NoError(err)
	assertRows(t, ctx, iter, int64(n-1))
}
