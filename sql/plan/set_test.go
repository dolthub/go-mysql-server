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
	"github.com/dolthub/vitess/go/sqltypes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestSet(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewContext(context.Background(), sql.WithSession(sql.NewBaseSession()))

	s := NewSet(
		[]sql.Expression{
			expression.NewSetField(expression.NewUserVar("foo"), expression.NewLiteral("bar", sql.LongText)),
			expression.NewSetField(expression.NewUserVar("baz"), expression.NewLiteral(int64(1), sql.Int64)),
		},
	)

	_, err := s.RowIter(ctx, nil)
	require.NoError(err)

	typ, v, err := ctx.GetUserVariable(ctx, "foo")
	require.NoError(err)
	require.Equal(sql.MustCreateStringWithDefaults(sqltypes.VarChar, 3), typ)
	require.Equal("bar", v)

	typ, v, err = ctx.GetUserVariable(ctx, "baz")
	require.NoError(err)
	require.Equal(sql.Int64, typ)
	require.Equal(int64(1), v)
}
