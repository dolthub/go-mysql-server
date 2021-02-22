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

package plan_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	. "github.com/dolthub/go-mysql-server/sql/plan"
)

func TestShowGrants(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	iter, err := NewShowGrants().RowIter(ctx, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(ctx, iter)
	require.NoError(err)

	expected := []sql.Row{
		{"GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION"},
	}

	require.Equal(expected, rows)
}
