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

package expression

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestDistinctExpressionEvalDistinct(t *testing.T) {
	ctx := sql.NewEmptyContext()
	expr := NewDistinctExpression(NewGetField(0, types.Int32, "v", true))

	tests := []struct {
		name    string
		value   any
		include bool
	}{
		{name: "first null", value: nil, include: true},
		{name: "duplicate null", value: nil, include: false},
		{name: "first value", value: int32(1), include: true},
		{name: "duplicate value", value: int32(1), include: false},
		{name: "different value", value: int32(2), include: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, include, err := expr.EvalDistinct(ctx, sql.Row{tt.value})
			require.NoError(t, err)
			require.Equal(t, tt.value, value)
			require.Equal(t, tt.include, include)
		})
	}
}
