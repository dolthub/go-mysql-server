// Copyright 2022 DoltHub, Inc.
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

package aggregation

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestWindowFramers(t *testing.T) {
	tests := []struct {
		Name     string
		Framer   sql.WindowFramer
		Expected []sql.WindowInterval
	}{
		{
			Name:   "partition framer",
			Framer: NewPartitionFramer(),
			Expected: []sql.WindowInterval{
				{},
				{End: 2},
				{End: 2},
				{Start: 2, End: 6},
				{Start: 2, End: 6},
				{Start: 2, End: 6},
				{Start: 2, End: 6},
				{Start: 6, End: 9},
				{Start: 6, End: 9},
				{Start: 6, End: 9},
			},
		},
		{
			Name:   "group by framer",
			Framer: NewGroupByFramer(),
			Expected: []sql.WindowInterval{
				{},
				{End: 2},
				{Start: 2, End: 6},
				{Start: 6, End: 9},
			},
		},
		{
			Name:   "rows unbounded preceeding to current row framer",
			Framer: NewUnboundedPrecedingToCurrentRowFramer(),
			Expected: []sql.WindowInterval{
				{},
				{Start: 0, End: 1},
				{Start: 0, End: 2},
				{Start: 2, End: 3},
				{Start: 2, End: 4},
				{Start: 2, End: 5},
				{Start: 2, End: 6},
				{Start: 6, End: 7},
				{Start: 6, End: 8},
				{Start: 6, End: 9},
			},
		},
	}

	partitions := []sql.WindowInterval{
		{}, // check nil row behavior, wouldn't happen normally
		{Start: 0, End: 2},
		{Start: 2, End: 6},
		{Start: 6, End: 9},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			_, err := tt.Framer.Interval()
			require.Error(t, err)
			require.Equal(t, err, ErrPartitionNotSet)

			var res []sql.WindowInterval
			var frame sql.WindowInterval
			for _, p := range partitions {
				tt.Framer = tt.Framer.NewFramer(p)
				for {
					frame, err = tt.Framer.Next()
					if errors.Is(err, io.EOF) {
						break
					}
					res = append(res, frame)
				}
			}
			require.Equal(t, tt.Expected, res)
		})
	}
}
