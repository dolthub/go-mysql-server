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

package aggregation

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TestNTileNonNumericArg is a regression test for a panic where a non-numeric
// NTILE bucket expression (e.g. NTILE('x')) triggered
// "interface conversion: interface {} is uint64, not int64" in StartPartition.
// A non-numeric argument must be reported as an invalid-argument error instead.
func TestNTileNonNumericArg(t *testing.T) {
	ctx := sql.NewEmptyContext()
	n := NewNTile(expression.NewLiteral("x", types.Text))

	interval := sql.WindowInterval{Start: 0, End: 2}
	err := n.StartPartition(ctx, interval, nil)
	require.Error(t, err)
	require.True(t, sql.ErrInvalidArgument.Is(err), "want ErrInvalidArgument, got %v", err)
}
