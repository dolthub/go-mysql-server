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
	"fmt"
	"testing"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorTypeCreation(t *testing.T) {
	tests := []struct {
		dimensions int
		expectErr  bool
	}{
		{1, false},
		{3, false},
		{128, false},
		{16000, false},
		{0, true},
		{16001, true},
		{-1, true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("dimensions_%d", tt.dimensions), func(t *testing.T) {
			vecType, err := CreateVectorType(tt.dimensions)
			if tt.expectErr {
				assert.Error(t, err)
				assert.Nil(t, vecType)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, vecType)
				assert.Equal(t, fmt.Sprintf("VECTOR(%d)", tt.dimensions), vecType.String())
			}
		})
	}
}

func TestVectorConversion(t *testing.T) {
	vecType, err := CreateVectorType(3)
	require.NoError(t, err)

	ctx := context.Background()

	tests := []struct {
		name      string
		input     interface{}
		expected  []float64
		expectErr bool
	}{
		{
			name:     "json_string",
			input:    "[1.0, 2.0, 3.0]",
			expected: []float64{1.0, 2.0, 3.0},
		},
		{
			name:     "float64_slice",
			input:    []float64{1.5, 2.5, 3.5},
			expected: []float64{1.5, 2.5, 3.5},
		},
		{
			name:     "interface_slice",
			input:    []interface{}{1.0, 2.0, 3.0},
			expected: []float64{1.0, 2.0, 3.0},
		},
		{
			name:     "mixed_interface_slice",
			input:    []interface{}{1, 2.5, "3.0"},
			expected: []float64{1.0, 2.5, 3.0},
		},
		{
			name:      "wrong_dimensions",
			input:     "[1.0, 2.0]",
			expectErr: true,
		},
		{
			name:      "invalid_json",
			input:     "[1.0, 2.0, invalid]",
			expectErr: true,
		},
		{
			name:      "non_array_json",
			input:     "1.0",
			expectErr: true,
		},
		{
			name:     "nil",
			input:    nil,
			expected: nil,
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

	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := vecType.Compare(ctx, tt.a, tt.b)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
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