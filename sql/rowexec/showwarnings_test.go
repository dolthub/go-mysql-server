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
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestShowWarnings(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewEmptyContext()
	ctx.Session.Warn(&sql.Warning{Level: "l1", Message: "w1", Code: 1})
	ctx.Session.Warn(&sql.Warning{Level: "l2", Message: "w2", Code: 2})
	ctx.Session.Warn(&sql.Warning{Level: "l4", Message: "w3", Code: 3})

	sw := plan.ShowWarnings(ctx.Session.Warnings())
	require.True(sw.Resolved())

	it, err := DefaultBuilder.Build(ctx, sw, nil)
	require.NoError(err)

	n := 3
	for row, err := it.Next(ctx); err == nil; row, err = it.Next(ctx) {
		level := row.GetValue(0).(string)
		code := row.GetValue(1).(int)
		message := row.GetValue(2).(string)

		t.Logf("level: %s\tcode: %v\tmessage: %s\n", level, code, message)

		require.Equal(n, code)
		n--
	}
	if err != io.EOF {
		require.NoError(err)
	}
	require.NoError(it.Close(ctx))
}
