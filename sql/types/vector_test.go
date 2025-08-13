// Copyright 2022 Dolthub, Inc.
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
	"context"
	"testing"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorConversion(t *testing.T) {
	vecType, err := CreateVectorType(3)
	require.NoError(t, err)

	ctx := context.Background()

	// Only binary can be converted to vectors, all other types should fail.
	tests := []struct {
		name      string
		input     interface{}
		expected  []float32
		expectErr bool
	}{
		{
			name:      "json_string",
			input:     "[1.0, 2.0, 3.0]",
			expectErr: true,
		},
		{
			name:     "float32_slice",
			input:    []float32{1.5, 2.5, 3.5},
			expected: []float32{1.5, 2.5, 3.5},
		},
		{
			name:     "interface_slice",
			input:    []interface{}{1.0, 2.0, 3.0},
			expected: []float32{1.0, 2.0, 3.0},
		},
		{
			name:      "wrong_dimensions",
			input:     []float32{1.5, 2.5},
			expectErr: true,
		},
		{
			name:     "byte_array",
			input:    []byte{0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40},
			expected: []float32{1.0, 2.0, 3.0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _, err := vecType.Convert(ctx, tt.input)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.expected == nil {
					assert.Nil(t, result)
				} else {
					assert.Equal(t, tt.expected, result)
				}
			}
		})
	}
}

func TestVectorCompare(t *testing.T) {
	vecType, err := CreateVectorType(3)
	require.NoError(t, err)

	ctx := context.Background()

	// Vectors can only be compared with other vectors of the same dimension, and compare based on their byte representations
	tests := []struct {
		name      string
		a         interface{}
		b         interface{}
		expected  int
		expectErr bool
	}{
		{
			name:     "equal_vectors",
			a:        []float64{1.0, 2.0, 3.0},
			b:        []float64{1.0, 2.0, 3.0},
			expected: 0,
		},
		{
			name:     "a_less_than_b",
			a:        []float64{1.0, 2.0, 3.0},
			b:        []float64{1.0, 2.0, 4.0},
			expected: -1,
		},
		{
			name:     "a_greater_than_b",
			a:        []float64{1.0, 3.0, 3.0},
			b:        []float64{1.0, 2.0, 3.0},
			expected: 1,
		},
		{
			name:      "different_dimensions",
			a:         []float64{1.0, 2.0},
			b:         []float64{1.0, 2.0, 3.0},
			expectErr: true,
		},
		{
			name:      "wrong_types",
			a:         "string",
			b:         []float64{1.0, 2.0, 3.0},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := vecType.Compare(ctx, tt.a, tt.b)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestVectorParsingFromSQL(t *testing.T) {
	// Test parsing VECTOR(3) type from SQL DDL
	stmt, err := sqlparser.Parse("CREATE TABLE test (vec VECTOR(3))")
	require.NoError(t, err)

	createTable, ok := stmt.(*sqlparser.DDL)
	require.True(t, ok)

	colType := createTable.TableSpec.Columns[0].Type
	assert.Equal(t, "VECTOR", colType.Type)
	require.NotNil(t, colType.Length)
	assert.Equal(t, "3", string(colType.Length.Val))

	// Test converting to Go type
	goType, err := ColumnTypeToType(&colType)
	require.NoError(t, err)

	vectorType, ok := goType.(VectorType)
	require.True(t, ok)
	assert.Equal(t, 3, vectorType.Dimensions)
	assert.Equal(t, "VECTOR(3)", vectorType.String())
}
