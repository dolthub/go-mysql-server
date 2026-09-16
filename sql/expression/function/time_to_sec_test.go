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
		value    interface{}
		expected interface{}
	}{
		{"null", nil, nil},
		{"ordinary time", "01:02:03", uint64(3723)},
		{"extended hours", "25:00:00", uint64(90000)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := f.Eval(ctx, sql.NewRow(tt.value))
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}
