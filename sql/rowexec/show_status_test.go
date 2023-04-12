// Copyright 2020-2022 Dolthub, Inc.
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

func TestShowStatus(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	var res sql.Row
	var err error
	n := plan.NewShowStatus(plan.ShowStatusModifier_Global)
	iter, err := DefaultBuilder.Build(ctx, n, nil)
	require.NoError(err)

	for {
		res, err = iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if res[0] == "uptime" {
			require.True(res[1].(int) >= 0)
		}
		require.NoError(err)
	}
}
