// Copyright 2026 Dolthub, Inc.
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

package expression

import (
	"math"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
)

func TestRowCountSum(t *testing.T) {
	ctx := sql.NewEmptyContext()
	for _, tt := range []struct {
		limit, offset uint64
		expected      int64
	}{
		{0, 0, 0}, {2, 1, 3},
		{math.MaxInt64 - 1, 1, math.MaxInt64},
		{math.MaxInt64 - 1, 2, math.MaxInt64},
		{math.MaxInt64, 1, math.MaxInt64},
		{1, math.MaxUint64, math.MaxInt64},
		{math.MaxUint64, math.MaxUint64, math.MaxInt64},
	} {
		sum := NewRowCountSum(NewLiteral(tt.limit, types.Uint64), NewLiteral(tt.offset, types.Uint64))
		got, err := sum.Eval(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, tt.expected, got)
	}
	for _, children := range [][2]sql.Expression{
		{NewLiteral(nil, types.Null), NewLiteral(int64(1), types.Int64)},
		{NewLiteral(int64(1), types.Int64), NewLiteral(nil, types.Null)},
	} {
		got, err := NewRowCountSum(children[0], children[1]).Eval(ctx, nil)
		require.NoError(t, err)
		require.Nil(t, got)
	}
	sum := NewRowCountSum(NewBindVar("lim"), NewLiteral(int64(1), types.Int64))
	_, err := sum.Eval(ctx, nil)
	require.True(t, sql.ErrUnboundPreparedStatementVariable.Is(err))
	replaced, err := sum.WithChildren(ctx, NewLiteral(int64(2), types.Int64), NewLiteral(int64(3), types.Int64))
	require.NoError(t, err)
	got, err := replaced.Eval(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, int64(5), got)
	_, err = sum.WithChildren(ctx)
	require.Error(t, err)
}
