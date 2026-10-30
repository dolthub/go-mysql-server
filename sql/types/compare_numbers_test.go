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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCompareNumbersFloatBoundaries verifies integer comparisons before potentially overflowing float conversions.
func TestCompareNumbersFloatBoundaries(t *testing.T) {
	tests := []struct {
		name     string
		integer  any
		number   float64
		expected int
	}{
		{"maximum signed rounded up", int64(math.MaxInt64), 1 << 63, -1},
		{"maximum signed exact float neighbor", int64(math.MaxInt64), math.Nextafter(1<<63, 0), 1},
		{"minimum signed exact", int64(math.MinInt64), -1 << 63, 0},
		{"minimum signed next lower float", int64(math.MinInt64), math.Nextafter(-1<<63, math.Inf(-1)), 1},
		{"minimum signed next higher float", int64(math.MinInt64), math.Nextafter(-1<<63, 0), -1},
		{"maximum unsigned rounded up", uint64(math.MaxUint64), 1 << 64, -1},
		{"maximum unsigned exact float neighbor", uint64(math.MaxUint64), math.Nextafter(1<<64, 0), 1},
		{"signed positive infinity", int64(math.MaxInt64), math.Inf(1), -1},
		{"signed negative infinity", int64(math.MinInt64), math.Inf(-1), 1},
		{"unsigned positive infinity", uint64(math.MaxUint64), math.Inf(1), -1},
		{"unsigned negative infinity", uint64(0), math.Inf(-1), 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, compareNumbers(tt.integer, tt.number))
			require.Equal(t, -tt.expected, compareNumbers(tt.number, tt.integer))
		})
	}
}
