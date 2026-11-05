// Copyright 2024 Dolthub, Inc.
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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestAnyValue(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11911
	ctx := sql.NewEmptyContext()

	child := expression.NewGetField(0, types.Int64, "v", true)
	f := NewAnyValue(ctx, child)

	require.Equal(t, types.Int64, f.Type(ctx))
	require.True(t, f.IsNullable(ctx))
	require.Equal(t, "ANY_VALUE(v)", f.String())

	val, err := f.Eval(ctx, sql.Row{int64(42)})
	require.NoError(t, err)
	require.Equal(t, int64(42), val)

	val, err = f.Eval(ctx, sql.Row{nil})
	require.NoError(t, err)
	require.Nil(t, val)
}
