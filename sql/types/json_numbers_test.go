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

package types_test

import (
	"math"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	sqljson "github.com/dolthub/go-mysql-server/sql/expression/function/json"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
)

func TestJSONNumberKindsRoundTrip(t *testing.T) {
	tests := []struct {
		input, output, kind string
		value               any
	}{
		{"42", "42", "INTEGER", int64(42)},
		{"4294967295", "4294967295", "INTEGER", int64(4294967295)},
		{"4294967296", "4294967296", "UNSIGNED INTEGER", uint64(4294967296)},
		{"-42", "-42", "INTEGER", int64(-42)},
		{"42.0", "42.0", "DOUBLE", float64(42)},
		{"4.2e1", "42.0", "DOUBLE", float64(42)},
		{"1.25", "1.25", "DOUBLE", 1.25},
		{"-0.0", "-0.0", "DOUBLE", math.Copysign(0, -1)},
		{"9007199254740993", "9007199254740993", "UNSIGNED INTEGER", uint64(9007199254740993)},
		{"-9223372036854775808", "-9223372036854775808", "INTEGER", int64(math.MinInt64)},
		{"9223372036854775807", "9223372036854775807", "UNSIGNED INTEGER", uint64(math.MaxInt64)},
		{"18446744073709551615", "18446744073709551615", "UNSIGNED INTEGER", uint64(math.MaxUint64)},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			var value any
			require.NoError(t, types.JsonUnmarshal([]byte(tt.input), &value))
			require.Equal(t, tt.value, value)
			require.Equal(t, tt.kind, sqljson.TypeOfJsonValue(value))
			encoded, err := types.MarshallJsonValue(value)
			require.NoError(t, err)
			require.Equal(t, tt.output, string(encoded))
			displayed, err := types.JsonToMySqlString(t.Context(), types.JSONDocument{Val: value})
			require.NoError(t, err)
			require.Equal(t, tt.output, displayed)
			var decoded any
			require.NoError(t, types.JsonUnmarshal(encoded, &decoded))
			require.Equal(t, tt.value, decoded)
			require.Equal(t, tt.kind, sqljson.TypeOfJsonValue(decoded))
		})
	}
}

func TestJSONNestedNumberKinds(t *testing.T) {
	input := `{"steps":[{"from":42,"ratio":42.0,"to":42}],"unsigned":18446744073709551615}`
	doc := types.MustJSON(input)
	encoded, err := types.MarshallJsonValue(doc.Val)
	require.NoError(t, err)
	require.Equal(t, input, string(encoded))
	require.Equal(t, int64(42), doc.Val.(map[string]any)["steps"].([]any)[0].(map[string]any)["from"])
}

func TestJSONFloatMarshalRejectsNonFinite(t *testing.T) {
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		_, err := types.MarshallJsonValue(map[string]any{"v": value})
		require.Error(t, err)
		_, err = types.JsonToMySqlString(t.Context(), types.JSONDocument{Val: value})
		require.Error(t, err)
	}
}

func TestJSONNumberKindsRowEquality(t *testing.T) {
	ctx := sql.NewEmptyContext()
	schema := sql.Schema{&sql.Column{Name: "doc", Type: types.JSON}}
	tests := []struct {
		left, right string
		equal       bool
	}{
		{`{"steps":[{"from":42}]}`, `{"steps":[{"from":42.0}]}`, true},
		{`{"steps":[{"from":42.0}]}`, `{"steps":[{"from":42}]}`, true},
		{`{"steps":[{"from":42.0}]}`, `{"steps":[{"from":4.2e1}]}`, true},
	}
	for _, tt := range tests {
		t.Run(tt.left+"/"+tt.right, func(t *testing.T) {
			left, right := types.MustJSON(tt.left), types.MustJSON(tt.right)
			comparison, err := types.CompareJSON(ctx, left, right)
			require.NoError(t, err)
			require.Zero(t, comparison)
			equal, err := (sql.Row{left}).Equals(ctx, sql.Row{right}, schema)
			require.NoError(t, err)
			require.Equal(t, tt.equal, equal)
		})
	}
}
