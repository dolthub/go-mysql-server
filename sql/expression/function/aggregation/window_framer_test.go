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
)

func TestWindowRowFramers(t *testing.T) {
	tests := []struct {
		Name     string
		Framer   func(frame sql.WindowFrame) (sql.WindowFramer, error)
		Expected []sql.WindowInterval
	}{
		{
			Name:   "rows unbounded preceding to current row framer",
			Framer: NewRowsUnboundedPrecedingToCurrentRowFramer,
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
		{
			Name:   "rows unbounded preceding to 1 following framer",
			Framer: NewRowsUnboundedPrecedingToNFollowingFramer,
			Expected: []sql.WindowInterval{
				{},
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
				{},
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
				{},
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
				{},
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
				{},
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
				{},
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
			frameDef := dummyFrame{}
			framer, err := tt.Framer(frameDef)
			require.NoError(t, err)

			_, err = framer.Interval()
			require.Error(t, err)
			require.Equal(t, err, ErrPartitionNotSet)

			var res []sql.WindowInterval
			var frame sql.WindowInterval
			for _, p := range partitions {
				framer = framer.NewFramer(p)

				for {
					frame, err = framer.Next()
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

func (d dummyFrame) NewFramer() (sql.WindowFramer, error) {
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
	return expression.NewLiteral(int8(2), sql.Int8)
}

func (d dummyFrame) StartNFollowing() sql.Expression {
	return expression.NewLiteral(int8(1), sql.Int8)
}

func (d dummyFrame) EndNPreceding() sql.Expression {
	return expression.NewLiteral(int8(1), sql.Int8)
}

func (d dummyFrame) EndNFollowing() sql.Expression {
	return expression.NewLiteral(int8(1), sql.Int8)
}
