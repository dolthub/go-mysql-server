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

package plan_test

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/require"
)

func TestHashLookupExtendedKeyConversion(t *testing.T) {
	ctx := sql.NewEmptyContext()
	integer := hashNumericType{FakeExtendedType: sql.FakeExtendedType{Name: "integer", ZeroVal: int32(0)}}
	numeric := hashNumericType{FakeExtendedType: sql.FakeExtendedType{Name: "numeric", ZeroVal: float64(0)}}
	for _, tt := range []struct {
		name     string
		source   sql.ExtendedType
		target   sql.ExtendedType
		input    interface{}
		expected interface{}
	}{
		{
			name:     "integer to numeric",
			source:   integer,
			target:   numeric,
			input:    int32(5),
			expected: float64(5),
		},
		{
			name:     "numeric to integer",
			source:   numeric,
			target:   integer,
			input:    float64(5),
			expected: int32(5),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lookup := plan.HashLookup{CompareType: tt.target}
			probe := expression.NewGetField(0, tt.source, "probe", false)
			entry := expression.NewGetField(0, tt.target, "entry", false)
			probeKey, _, err := lookup.GetHashKey(ctx, probe, sql.Row{tt.input})
			require.NoError(t, err)
			entryKey, _, err := lookup.GetHashKey(ctx, entry, sql.Row{tt.expected})
			require.NoError(t, err)
			require.Equal(t, entryKey, probeKey)
		})
	}
}

// hashNumericType uses FakeExtendedType's strict numeric conversion without
// advertising a collated string to the hash function.
type hashNumericType struct {
	sql.FakeExtendedType
}

func (hashNumericType) Type() query.Type {
	return query.Type_INT64
}
