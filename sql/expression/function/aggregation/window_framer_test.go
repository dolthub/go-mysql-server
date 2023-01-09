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
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestWindowRowFramers(t *testing.T) {
	tests := []struct {
		Name     string
		Framer   func(sql.WindowFrame, *sql.WindowDefinition) (sql.WindowFramer, error)
		Expected []sql.WindowInterval
	}{
		{
			Name:   "rows unbounded preceding to current row framer",
			Framer: NewRowsUnboundedPrecedingToCurrentRowFramer,
			Expected: []sql.WindowInterval{
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
		{
			Name:   "rows unbounded preceding to 1 following framer",
			Framer: NewRowsUnboundedPrecedingToNFollowingFramer,
			Expected: []sql.WindowInterval{
				{Start: 0, End: 2},
				{Start: 0, End: 2},
				{Start: 2, End: 4},
				{Start: 2, End: 5},
				{Start: 2, End: 6},
				{Start: 2, End: 6},
				{Start: 6, End: 8},
				{Start: 6, End: 9},
				{Start: 6, End: 9},
			},
		},
		{
			Name:   "rows 2 preceding to 1 following framer",
			Framer: NewRowsNPrecedingToNFollowingFramer,
			Expected: []sql.WindowInterval{
				{Start: 0, End: 2},
				{Start: 0, End: 2},
				{Start: 2, End: 4},
				{Start: 2, End: 5},
				{Start: 2, End: 6},
				{Start: 3, End: 6},
				{Start: 6, End: 8},
				{Start: 6, End: 9},
				{Start: 6, End: 9},
			},
		},
		{
			Name:   "rows unbound preceding to unbound following framer",
			Framer: NewRowsUnboundedPrecedingToUnboundedFollowingFramer,
			Expected: []sql.WindowInterval{
				{Start: 0, End: 2},
				{Start: 0, End: 2},
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
			Name:   "rows 2 preceding to 1 preceding framer",
			Framer: NewRowsNPrecedingToNPrecedingFramer,
			Expected: []sql.WindowInterval{
				{Start: 0, End: 0},
				{Start: 0, End: 1},
				{Start: 2, End: 2},
				{Start: 2, End: 3},
				{Start: 2, End: 4},
				{Start: 3, End: 5},
				{Start: 6, End: 6},
				{Start: 6, End: 7},
				{Start: 6, End: 8},
			},
		},
		{
			Name:   "rows 1 following to 1 following framer",
			Framer: NewRowsNFollowingToNFollowingFramer,
			Expected: []sql.WindowInterval{
				{Start: 1, End: 2},
				{Start: 2, End: 2},
				{Start: 3, End: 4},
				{Start: 4, End: 5},
				{Start: 5, End: 6},
				{Start: 6, End: 6},
				{Start: 7, End: 8},
				{Start: 8, End: 9},
				{Start: 9, End: 9},
			},
		},
		{
			Name:   "rows current row to current row framer",
			Framer: NewRowsCurrentRowToCurrentRowFramer,
			Expected: []sql.WindowInterval{
				{Start: 0, End: 1},
				{Start: 1, End: 2},
				{Start: 2, End: 3},
				{Start: 3, End: 4},
				{Start: 4, End: 5},
				{Start: 5, End: 6},
				{Start: 6, End: 7},
				{Start: 7, End: 8},
				{Start: 8, End: 9},
			},
		},
	}

	partitions := []sql.WindowInterval{
		{}, // nil rows creates one empty partition
		{Start: 0, End: 2},
		{Start: 2, End: 6},
		{Start: 6, End: 9},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			frameDef := dummyFrame{}
			framer, err := tt.Framer(frameDef, nil)
			require.NoError(t, err)

			_, err = framer.Interval()
			require.Error(t, err)
			require.Equal(t, err, ErrPartitionNotSet)

			var res []sql.WindowInterval
			var frame sql.WindowInterval
			for _, p := range partitions {
				framer, err = framer.NewFramer(p)
				require.NoError(t, err)

				for {
					frame, err = framer.Next(ctx, nil)
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

func TestWindowRangeFramers(t *testing.T) {
	tests := []struct {
		Name     string
		Framer   func(sql.WindowFrame, *sql.WindowDefinition) (sql.WindowFramer, error)
		OrderBy  []sql.Expression
		Expected []sql.WindowInterval
	}{
		{
			Name:   "range unbound preceding to unbound following framer",
			Framer: NewRangeUnboundedPrecedingToUnboundedFollowingFramer,
			Expected: []sql.WindowInterval{
				{},
				{Start: 0, End: 10}, {Start: 0, End: 10}, {Start: 0, End: 10}, {Start: 0, End: 10}, {Start: 0, End: 10}, {Start: 0, End: 10}, {Start: 0, End: 10}, {Start: 0, End: 10}, {Start: 0, End: 10}, {Start: 0, End: 10},
				{Start: 10, End: 16}, {Start: 10, End: 16}, {Start: 10, End: 16}, {Start: 10, End: 16}, {Start: 10, End: 16}, {Start: 10, End: 16},
				{Start: 16, End: 19}, {Start: 16, End: 19}, {Start: 16, End: 19},
			},
		},
		{
			Name:   "range 2 preceding to 1 preceding framer",
			Framer: NewRangeNPrecedingToNPrecedingFramer,
			Expected: []sql.WindowInterval{
				{},
				{Start: 0, End: 0}, {Start: 0, End: 0}, {Start: 0, End: 2}, {Start: 2, End: 3}, {Start: 3, End: 4}, {Start: 3, End: 4}, {Start: 4, End: 6}, {Start: 4, End: 7}, {Start: 4, End: 7}, {Start: 6, End: 9},
				{Start: 10, End: 10}, {Start: 10, End: 10}, {Start: 10, End: 12}, {Start: 12, End: 13}, {Start: 13, End: 14}, {Start: 13, End: 14},
				{Start: 16, End: 16}, {Start: 16, End: 17}, {Start: 16, End: 18},
			},
		},
		{
			Name:   "range current row to current row framer",
			Framer: NewRangeCurrentRowToCurrentRowFramer,
			Expected: []sql.WindowInterval{
				{},
				{Start: 0, End: 2}, {Start: 0, End: 2}, {Start: 2, End: 3}, {Start: 3, End: 4}, {Start: 4, End: 6}, {Start: 4, End: 6}, {Start: 6, End: 7}, {Start: 7, End: 9}, {Start: 7, End: 9}, {Start: 9, End: 10},
				{Start: 10, End: 12}, {Start: 10, End: 12}, {Start: 12, End: 13}, {Start: 13, End: 14}, {Start: 14, End: 16}, {Start: 14, End: 16},
				{Start: 16, End: 17}, {Start: 17, End: 18}, {Start: 18, End: 19},
			},
		},
		{
			Name:   "range unbounded preceding to current row framer",
			Framer: NewRangeUnboundedPrecedingToCurrentRowFramer,
			Expected: []sql.WindowInterval{
				{},
				{Start: 0, End: 2}, {Start: 0, End: 2}, {Start: 0, End: 3}, {Start: 0, End: 4}, {Start: 0, End: 6}, {Start: 0, End: 6}, {Start: 0, End: 7}, {Start: 0, End: 9}, {Start: 0, End: 9}, {Start: 0, End: 10},
				{Start: 10, End: 12}, {Start: 10, End: 12}, {Start: 10, End: 13}, {Start: 10, End: 14}, {Start: 10, End: 16}, {Start: 10, End: 16},
				{Start: 16, End: 17}, {Start: 16, End: 18}, {Start: 16, End: 19},
			},
		},
		{
			Name:   "range 1 following to 1 following framer",
			Framer: NewRangeNFollowingToNFollowingFramer,
			Expected: []sql.WindowInterval{
				{},
				{Start: 2, End: 3}, {Start: 2, End: 3}, {Start: 3, End: 3}, {Start: 4, End: 4}, {Start: 6, End: 7}, {Start: 6, End: 7}, {Start: 7, End: 9}, {Start: 9, End: 10}, {Start: 9, End: 10}, {Start: 10, End: 10},
				{Start: 12, End: 13}, {Start: 12, End: 13}, {Start: 13, End: 13}, {Start: 14, End: 14}, {Start: 16, End: 16}, {Start: 16, End: 16},
				{Start: 17, End: 18}, {Start: 18, End: 19}, {Start: 19, End: 19},
			},
		},
		{
			Name:   "range unbounded preceding to 1 following framer",
			Framer: NewRangeUnboundedPrecedingToNFollowingFramer,
			Expected: []sql.WindowInterval{
				{},
				{Start: 0, End: 3}, {Start: 0, End: 3}, {Start: 0, End: 3}, {Start: 0, End: 4}, {Start: 0, End: 7}, {Start: 0, End: 7}, {Start: 0, End: 9}, {Start: 0, End: 10}, {Start: 0, End: 10}, {Start: 0, End: 10},
				{Start: 10, End: 13}, {Start: 10, End: 13}, {Start: 10, End: 13}, {Start: 10, End: 14}, {Start: 10, End: 16}, {Start: 10, End: 16},
				{Start: 16, End: 18}, {Start: 16, End: 19}, {Start: 16, End: 19},
			},
		},
		{
			Name:   "rows 2 preceding to 1 following framer",
			Framer: NewRangeNPrecedingToNFollowingFramer,
			Expected: []sql.WindowInterval{
				{},
				{Start: 0, End: 3}, {Start: 0, End: 3}, {Start: 0, End: 3}, {Start: 2, End: 4}, {Start: 3, End: 7}, {Start: 3, End: 7}, {Start: 4, End: 9}, {Start: 4, End: 10}, {Start: 4, End: 10}, {Start: 6, End: 10},
				{Start: 10, End: 13}, {Start: 10, End: 13}, {Start: 10, End: 13}, {Start: 12, End: 14}, {Start: 13, End: 16}, {Start: 13, End: 16},
				{Start: 16, End: 18}, {Start: 16, End: 19}, {Start: 16, End: 19},
			},
		},
	}

	buffer := []sql.Row{
		{0, 1}, {0, 1}, {0, 2}, {0, 4}, {0, 6}, {0, 6}, {0, 7}, {0, 8}, {0, 8}, {0, 9},
		{1, 1}, {1, 1}, {1, 2}, {1, 4}, {1, 6}, {1, 6},
		{2, 1}, {2, 2}, {2, 3},
	}
	partitions := []sql.WindowInterval{
		{}, // nil rows creates one empty partition
		{Start: 0, End: 10},
		{Start: 10, End: 16},
		{Start: 16, End: 19},
	}
	expr := expression.NewGetField(1, types.Int64, "", false)

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			frameDef := dummyFrame{}
			w := &sql.WindowDefinition{OrderBy: sql.SortFields{{Column: expr, Order: 1}}}
			framer, err := tt.Framer(frameDef, w)
			require.NoError(t, err)

			_, err = framer.Interval()
			require.Error(t, err)
			require.Equal(t, err, ErrPartitionNotSet)

			var res []sql.WindowInterval
			var frame sql.WindowInterval
			for _, p := range partitions {
				framer, err = framer.NewFramer(p)
				require.NoError(t, err)
				for {
					frame, err = framer.Next(ctx, buffer)
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

type dummyFrame struct{}

var _ sql.WindowFrame = (*dummyFrame)(nil)

func (d dummyFrame) String() string {
	panic("implement me")
}

func (d dummyFrame) NewFramer(*sql.WindowDefinition) (sql.WindowFramer, error) {
	panic("implement me")
}

func (d dummyFrame) UnboundedFollowing() bool {
	return true
}

func (d dummyFrame) UnboundedPreceding() bool {
	return true
}

func (d dummyFrame) StartCurrentRow() bool {
	return true
}

func (d dummyFrame) EndCurrentRow() bool {
	return true
}

func (d dummyFrame) StartNPreceding() sql.Expression {
	return expression.NewLiteral(int8(2), types.Int8)
}

func (d dummyFrame) StartNFollowing() sql.Expression {
	return expression.NewLiteral(int8(1), types.Int8)
}

func (d dummyFrame) EndNPreceding() sql.Expression {
	return expression.NewLiteral(int8(1), types.Int8)
}

func (d dummyFrame) EndNFollowing() sql.Expression {
	return expression.NewLiteral(int8(1), types.Int8)
}
