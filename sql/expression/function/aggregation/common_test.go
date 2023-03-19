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

package aggregation

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gabereiser/go-mysql-server/sql"
)

func eval(t *testing.T, e sql.Expression, row sql.Row) interface{} {
	t.Helper()

	ctx := sql.NewEmptyContext()
	v, err := e.Eval(ctx, row)
	require.NoError(t, err)
	return v
}

func evalBuffer(t *testing.T, buf sql.AggregationBuffer) interface{} {
	t.Helper()

	ctx := sql.NewEmptyContext()
	v, err := buf.Eval(ctx)
	require.NoError(t, err)
	return v
}

func aggregate(t *testing.T, agg sql.Aggregation, rows ...sql.Row) interface{} {
	t.Helper()

	ctx := sql.NewEmptyContext()
	buf, _ := agg.NewBuffer()
	for _, row := range rows {
		require.NoError(t, buf.Update(ctx, row))
	}
	return evalBuffer(t, buf)
}
