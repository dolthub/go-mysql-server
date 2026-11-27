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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestTimeToSec(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f := NewTimeToSec(ctx, expression.NewGetField(0, types.LongText, "foo", true))

	for _, tt := range []struct {
		name     string
		value    any
		expected any
	}{
		{"null", nil, nil},
		{"ordinary time", "01:02:03", int64(3723)},
		{"extended hours", "25:00:00", int64(90000)},
		{"negative time", "-00:00:05", int64(-5)},
		{"negative hours", "-01:00:00", int64(-3600)},
		{"minimum time", "-838:59:59", int64(-3020399)},
		{"negative number", int64(-100), int64(-60)},
		{"fraction is truncated", "00:00:05.9", int64(5)},
		{"negative fraction is truncated", -5.5, int64(-5)},
		{"negative fraction below one second", -0.5, int64(0)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := f.Eval(ctx, sql.NewRow(tt.value))
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}
