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

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	agg "github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
)

func TestExcludingWindowFrame(t *testing.T) {
	frame := NewRowsUnboundedPrecedingToUnboundedFollowingFrame()

	// EXCLUDE NO OTHERS is the default, and leaves the frame unchanged
	require.Same(t, frame, NewExcludingWindowFrame(frame, sql.WindowFrameExcludeNoOthers))
	require.Nil(t, NewExcludingWindowFrame(nil, sql.WindowFrameExcludeTies))

	excluding := NewExcludingWindowFrame(frame, sql.WindowFrameExcludeTies)
	require.Equal(t, "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES", excluding.String())
	require.True(t, excluding.UnboundedPreceding())
	require.True(t, excluding.UnboundedFollowing())

	framer, err := excluding.NewFramer(&sql.WindowDefinition{})
	require.NoError(t, err)
	require.IsType(t, &agg.ExcludingFramer{}, framer)
}
