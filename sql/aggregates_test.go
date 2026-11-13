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

package sql

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWindowIntervalExclusions(t *testing.T) {
	tests := []struct {
		Name     string
		Interval WindowInterval
		Indices  []int
		Segments []WindowInterval
	}{
		{
			Name:     "no exclusions",
			Interval: WindowInterval{Start: 2, End: 5},
			Indices:  []int{2, 3, 4},
			Segments: []WindowInterval{{Start: 2, End: 5}},
		},
		{
			Name:     "empty interval",
			Interval: WindowInterval{Start: 2, End: 2},
		},
		{
			Name: "middle row excluded",
			Interval: WindowInterval{
				Start:      2,
				End:        5,
				Exclusions: [2]WindowExclusion{{Start: 3, End: 4}},
			},
			Indices:  []int{2, 4},
			Segments: []WindowInterval{{Start: 2, End: 3}, {Start: 4, End: 5}},
		},
		{
			Name: "two exclusions keep the row between them",
			Interval: WindowInterval{
				Start:      0,
				End:        6,
				Exclusions: [2]WindowExclusion{{Start: 1, End: 3}, {Start: 4, End: 5}},
			},
			Indices:  []int{0, 3, 5},
			Segments: []WindowInterval{{Start: 0, End: 1}, {Start: 3, End: 4}, {Start: 5, End: 6}},
		},
		{
			Name: "exclusions overlapping the interval bounds",
			Interval: WindowInterval{
				Start:      2,
				End:        6,
				Exclusions: [2]WindowExclusion{{Start: 0, End: 3}, {Start: 5, End: 8}},
			},
			Indices:  []int{3, 4},
			Segments: []WindowInterval{{Start: 3, End: 5}},
		},
		{
			Name: "exclusions outside the interval",
			Interval: WindowInterval{
				Start:      2,
				End:        4,
				Exclusions: [2]WindowExclusion{{Start: 0, End: 1}, {Start: 5, End: 6}},
			},
			Indices:  []int{2, 3},
			Segments: []WindowInterval{{Start: 2, End: 4}},
		},
		{
			Name: "every row excluded",
			Interval: WindowInterval{
				Start:      2,
				End:        4,
				Exclusions: [2]WindowExclusion{{Start: 2, End: 4}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			require.Equal(t, tt.Indices, slices.Collect(tt.Interval.Indices()))
			require.Equal(t, tt.Segments, slices.Collect(tt.Interval.Segments()))
			require.Equal(t, len(tt.Indices), tt.Interval.Len())

			for n := -1; n <= len(tt.Indices); n++ {
				idx, ok := tt.Interval.Nth(n)
				if n < 0 || n >= len(tt.Indices) {
					require.False(t, ok)
				} else {
					require.True(t, ok)
					require.Equal(t, tt.Indices[n], idx)
				}
			}

			last, ok := tt.Interval.Last()
			require.Equal(t, len(tt.Indices) > 0, ok)
			if ok {
				require.Equal(t, tt.Indices[len(tt.Indices)-1], last)
			}
		})
	}
}
