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

package types

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/stretchr/testify/require"
)

// TestJsonUnmarshalPreserveNumberTokensSupportsUnboundedExponent verifies lexical number retention.
func TestJsonUnmarshalPreserveNumberTokensSupportsUnboundedExponent(t *testing.T) {
	input := []byte(`{"exact":123456789012345678901234567890.123456789,"exponent":1e3000000000}`)
	var value interface{}
	require.NoError(t, JsonUnmarshalPreserveNumberTokens(input, &value))

	object := value.(map[string]interface{})
	require.Equal(t, json.Number("123456789012345678901234567890.123456789"), object["exact"])
	require.Equal(t, json.Number("1e3000000000"), object["exponent"])
	encoded, err := MarshallJsonValue(value)
	require.NoError(t, err)
	require.Equal(t, string(input), string(encoded))
}

// TestJsonUnmarshalPreserveNumberPrecision verifies exact fixed-point decoding.
func TestJsonUnmarshalPreserveNumberPrecision(t *testing.T) {
	input := []byte(`{"value":123456789012345678901234567890.123456789}`)
	var value interface{}
	require.NoError(t, JsonUnmarshalPreserveNumberPrecision(input, &value))

	decimal, ok := value.(map[string]interface{})["value"].(*apd.Decimal)
	require.True(t, ok)
	require.Equal(t, "123456789012345678901234567890.123456789", decimal.Text('f'))

	encoded, err := MarshallJsonValue(value)
	require.NoError(t, err)
	require.JSONEq(t, string(input), string(encoded))
}

// TestJsonPrecisionNestedRoundTripAndDeepCopy verifies nested precision and copy isolation.
func TestJsonPrecisionNestedRoundTripAndDeepCopy(t *testing.T) {
	input := []byte(`{"array":[9007199254740992.1,{"value":-1234567890.123456789}],"ordinary":1.25}`)
	var value interface{}
	require.NoError(t, JsonUnmarshalPreserveNumberPrecision(input, &value))

	object := value.(map[string]interface{})
	array := object["array"].([]interface{})
	require.IsType(t, &apd.Decimal{}, array[0])
	require.IsType(t, &apd.Decimal{}, array[1].(map[string]interface{})["value"])
	require.Equal(t, "1.25", object["ordinary"].(*apd.Decimal).String())

	clone := DeepCopyJson(value).(map[string]interface{})
	clonedDecimal := clone["array"].([]interface{})[0].(*apd.Decimal)
	originalDecimal := array[0].(*apd.Decimal)
	require.NotSame(t, originalDecimal, clonedDecimal)
	clonedDecimal.Neg(clonedDecimal)
	require.Equal(t, "9007199254740992.1", originalDecimal.Text('f'))

	encoded, err := MarshallJsonValue(value)
	require.NoError(t, err)
	require.JSONEq(t, string(input), string(encoded))
}

// TestCompareJSONPreservedDecimals verifies exact ordering across JSON numeric representations.
func TestCompareJSONPreservedDecimals(t *testing.T) {
	left := mustPreciseJSON(t, `9007199254740992.1`)
	right := mustPreciseJSON(t, `9007199254740992.2`)
	cmp, err := CompareJSON(t.Context(), left, right)
	require.NoError(t, err)
	require.Negative(t, cmp)

	equalScale := mustPreciseJSON(t, `9007199254740992.10`)
	cmp, err = CompareJSON(t.Context(), left, equalScale)
	require.NoError(t, err)
	require.Zero(t, cmp)

	cmp, err = CompareJSON(t.Context(), mustPreciseJSON(t, `1.0`), int64(1))
	require.NoError(t, err)
	require.Zero(t, cmp)

	cmp, err = CompareJSON(t.Context(), left, float64(9007199254740992))
	require.NoError(t, err)
	require.Positive(t, cmp)
}

// TestContainsJSONPreservedDecimals verifies exact containment across JSON numeric representations.
func TestContainsJSONPreservedDecimals(t *testing.T) {
	target := mustPreciseJSON(t, `[9007199254740992.1, 1.0]`).Val

	contained, err := ContainsJSON(target, mustPreciseJSON(t, `9007199254740992.10`).Val)
	require.NoError(t, err)
	require.True(t, contained)

	contained, err = ContainsJSON(target, int64(1))
	require.NoError(t, err)
	require.True(t, contained)

	contained, err = ContainsJSON(target, mustPreciseJSON(t, `9007199254740992.2`).Val)
	require.NoError(t, err)
	require.False(t, contained)
}

// TestJsonUnmarshalRetainsMySQLNumberRepresentations verifies that the ordinary
// decoder continues to normalize JSON text through float64 where MySQL does.
func TestJsonUnmarshalRetainsMySQLNumberRepresentations(t *testing.T) {
	var value interface{}
	require.NoError(t, JsonUnmarshal([]byte(`1234567890.123456789`), &value))
	require.IsType(t, float64(0), value)
	require.Equal(t, float64(1234567890.1234567), value)

	require.NoError(t, JsonUnmarshal([]byte(`9007199254740993`), &value))
	require.Equal(t, int64(9007199254740993), value)

	require.NoError(t, JsonUnmarshal([]byte(`1e100000`), &value))
	require.True(t, math.IsInf(value.(float64), 1))
}

// TestJsonUnmarshalPreserveNumberPrecisionHandlesExponent verifies exact exponent decoding.
func TestJsonUnmarshalPreserveNumberPrecisionHandlesExponent(t *testing.T) {
	var value interface{}
	require.NoError(t, JsonUnmarshalPreserveNumberPrecision([]byte(`1.234567890123456789e100`), &value))
	require.Equal(t, "1.234567890123456789E+100", value.(*apd.Decimal).String())

	value = nil
	require.NoError(t, JsonUnmarshalPreserveNumberPrecision([]byte(`1e131071`), &value))
	require.Equal(t, int32(131071), value.(*apd.Decimal).Exponent)

	value = nil
	require.NoError(t, JsonUnmarshalPreserveNumberPrecision([]byte(`-1.25e+131070`), &value))
	require.True(t, value.(*apd.Decimal).Negative)
	require.Equal(t, int32(131068), value.(*apd.Decimal).Exponent)
	require.Equal(t, "125", value.(*apd.Decimal).Coeff.String())

	value = nil
	require.Error(t, JsonUnmarshalPreserveNumberPrecision([]byte(`1e3000000000`), &value))
}

// TestJsonUnmarshalKeepsExactlyRepresentableFractionAsFloat verifies the float fast path.
func TestJsonUnmarshalKeepsExactlyRepresentableFractionAsFloat(t *testing.T) {
	var value interface{}
	require.NoError(t, JsonUnmarshal([]byte(`1.25`), &value))
	require.Equal(t, 1.25, value)
}

// mustPreciseJSON parses a test document without normalizing its numbers through float64.
func mustPreciseJSON(t *testing.T, input string) JSONDocument {
	t.Helper()
	var value interface{}
	require.NoError(t, JsonUnmarshalPreserveNumberPrecision([]byte(input), &value))
	return JSONDocument{Val: value}
}

// BenchmarkJSONNumberDecoding compares the numeric policies used by MySQL, storage, and PostgreSQL.
func BenchmarkJSONNumberDecoding(b *testing.B) {
	inputs := map[string][]byte{
		"ordinary": []byte(`{"id":42,"ratio":1.25,"items":[1,2,3,4],"nested":{"enabled":true}}`),
		"precise":  []byte(`{"id":42,"ratio":12345678901234567890.123456789,"items":[1.1,2.2,3.3,4.4]}`),
	}
	decoders := map[string]func([]byte, *interface{}) error{
		"mysql_normalized": JsonUnmarshal,
		"exact":            JsonUnmarshalPreserveNumberPrecision,
	}
	for inputName, input := range inputs {
		for decoderName, decoder := range decoders {
			b.Run(inputName+"/"+decoderName, func(b *testing.B) {
				b.ReportAllocs()
				benchmarkJSONNumberDecoder(b, input, decoder)
			})
		}
	}
}

// benchmarkJSONNumberDecoder repeatedly decodes one document with the selected numeric policy.
func benchmarkJSONNumberDecoder(b *testing.B, input []byte, decoder func([]byte, *interface{}) error) {
	b.Helper()
	for i := 0; i < b.N; i++ {
		var value interface{}
		if err := decoder(input, &value); err != nil {
			b.Fatal(err)
		}
	}
}
