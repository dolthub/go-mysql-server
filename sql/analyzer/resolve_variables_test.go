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

package analyzer

import (
	"context"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSetDefault(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewContext(context.Background(), sql.WithSession(sql.NewBaseSession()))

	s := plan.NewSet(
		[]sql.Expression{
			expression.NewSetField(expression.NewSystemVar("auto_increment_increment", sql.Int64), expression.NewLiteral(int64(123), sql.Int64)),
			expression.NewSetField(expression.NewSystemVar("sql_select_limit", sql.Int64), expression.NewLiteral(int64(1), sql.Int64)),
		},
	)

	_, err := s.RowIter(ctx, nil)
	require.NoError(err)

	typ, v := ctx.Get("auto_increment_increment")
	require.Equal(sql.Int64, typ)
	require.Equal(int64(123), v)

	typ, v = ctx.Get("sql_select_limit")
	require.Equal(sql.Int64, typ)
	require.Equal(int64(1), v)

	s = plan.NewSet(
		[]sql.Expression{
			expression.NewSetField(expression.NewSystemVar("auto_increment_increment", sql.Int64), expression.NewDefaultColumn("")),
			expression.NewSetField(expression.NewSystemVar("sql_select_limit", sql.Int64), expression.NewDefaultColumn("")),
		},
	)

	_, err = s.RowIter(ctx, nil)
	require.NoError(err)

	defaults := sql.DefaultSessionConfig()

	typ, v = ctx.Get("auto_increment_increment")
	require.Equal(defaults["auto_increment_increment"].Typ, typ)
	require.Equal(defaults["auto_increment_increment"].Value, v)

	typ, v = ctx.Get("sql_select_limit")
	require.Equal(defaults["sql_select_limit"].Typ, typ)
	require.Equal(defaults["sql_select_limit"].Value, v)
}
