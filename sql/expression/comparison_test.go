// Copyright 2020-2021 Dolthub, Inc.
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

package expression

import (
	"encoding/binary"
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

const (
	testEqual int = iota
	testLess
	testGreater
	testRegexp
	testNotRegexp
	testNil
)

var comparisonCases = map[sql.Type]map[int][][]interface{}{
	types.LongText: {
		testEqual: {
			{"foo", "foo"},
			{"", ""},
		},
		testLess: {
			{"a", "b"},
			{"", "1"},
		},
		testGreater: {
			{"b", "a"},
			{"1", ""},
		},
		testNil: {
			{nil, "a"},
			{"a", nil},
			{nil, nil},
		},
	},
	types.Int32: {
		testEqual: {
			{int32(1), int32(1)},
			{int32(0), int32(0)},
		},
		testLess: {
			{int32(-1), int32(0)},
			{int32(1), int32(2)},
		},
		testGreater: {
			{int32(2), int32(1)},
			{int32(0), int32(-1)},
		},
		testNil: {
			{nil, int32(1)},
			{int32(1), nil},
			{nil, nil},
		},
	},
}

var likeComparisonCases = map[sql.Type]map[int][][]interface{}{
	types.LongText: {
		testRegexp: {
			{"foobar", ".*bar"},
			{"foobarfoo", ".*bar.*"},
			{"bar", "bar"},
			{"barfoo", "bar.*"},
		},
		testNotRegexp: {
			{"foobara", ".*bar$"},
			{"foofoo", ".*bar.*"},
			{"bara", "bar$"},
			{"abarfoo", "^bar.*"},
		},
		testNil: {
			{"foobar", nil},
			{nil, ".*bar"},
			{nil, nil},
		},
	},
	types.Int32: {
		testRegexp: {
			{int32(1), int32(1)},
			{int32(0), int32(0)},
		},
		testNotRegexp: {
			{int32(-1), int32(0)},
			{int32(1), int32(2)},
		},
	},
}

func TestEquals(t *testing.T) {
	require := require.New(t)
	for resultType, cmpCase := range comparisonCases {
		get0 := NewGetField(0, resultType, "col1", true)
		require.NotNil(get0)
		get1 := NewGetField(1, resultType, "col2", true)
		require.NotNil(get1)
		eq := NewEquals(get0, get1)
		require.NotNil(eq)
		require.Equal(types.Boolean, eq.Type())
		for cmpResult, cases := range cmpCase {
			for _, pair := range cases {
				row := sql.NewRow(pair[0], pair[1])
				require.NotNil(row)
				cmp := eval(t, eq, row)
				if cmpResult == testEqual {
					require.Equal(true, cmp)
				} else if cmpResult == testNil {
					require.Nil(cmp)
				} else {
					require.Equal(false, cmp)
				}
			}
		}
	}
}

func TestNullSafeEquals(t *testing.T) {
	require := require.New(t)
	for resultType, cmpCase := range comparisonCases {
		get0 := NewGetField(0, resultType, "col1", true)
		require.NotNil(get0)
		get1 := NewGetField(1, resultType, "col2", true)
		require.NotNil(get1)
		seq := NewNullSafeEquals(get0, get1)
		require.NotNil(seq)
		require.Equal(types.Boolean, seq.Type())
		for cmpResult, cases := range cmpCase {
			for _, pair := range cases {
				row := sql.NewRow(pair[0], pair[1])
				require.NotNil(row)
				cmp := eval(t, seq, row)
				if cmpResult == testEqual {
					require.Equal(true, cmp)
				} else if cmpResult == testNil {
					if pair[0] == nil && pair[1] == nil {
						require.Equal(true, cmp)
					} else {
						require.Equal(false, cmp)
					}
				} else {
					require.Equal(false, cmp)
				}
			}
		}
	}
}

func TestLessThan(t *testing.T) {
	require := require.New(t)
	for resultType, cmpCase := range comparisonCases {
		get0 := NewGetField(0, resultType, "col1", true)
		require.NotNil(get0)
		get1 := NewGetField(1, resultType, "col2", true)
		require.NotNil(get1)
		eq := NewLessThan(get0, get1)
		require.NotNil(eq)
		require.Equal(types.Boolean, eq.Type())
		for cmpResult, cases := range cmpCase {
			for _, pair := range cases {
				row := sql.NewRow(pair[0], pair[1])
				require.NotNil(row)
				cmp := eval(t, eq, row)
				if cmpResult == testLess {
					require.Equal(true, cmp, "%v < %v", pair[0], pair[1])
				} else if cmpResult == testNil {
					require.Nil(cmp)
				} else {
					require.Equal(false, cmp)
				}
			}
		}
	}
}

func TestGreaterThan(t *testing.T) {
	require := require.New(t)
	for resultType, cmpCase := range comparisonCases {
		get0 := NewGetField(0, resultType, "col1", true)
		require.NotNil(get0)
		get1 := NewGetField(1, resultType, "col2", true)
		require.NotNil(get1)
		eq := NewGreaterThan(get0, get1)
		require.NotNil(eq)
		require.Equal(types.Boolean, eq.Type())
		for cmpResult, cases := range cmpCase {
			for _, pair := range cases {
				row := sql.NewRow(pair[0], pair[1])
				require.NotNil(row)
				cmp := eval(t, eq, row)
				if cmpResult == testGreater {
					require.Equal(true, cmp)
				} else if cmpResult == testNil {
					require.Nil(cmp)
				} else {
					require.Equal(false, cmp)
				}
			}
		}
	}
}

func TestValueComparison(t *testing.T) {
	t.Skip("TODO: write tests for comparison between sql.Values")
}

// BenchmarkComparison
// BenchmarkComparison-14    	 4426766	       264.4 ns/op
func BenchmarkComparison(b *testing.B) {
	ctx := sql.NewEmptyContext()
	gf1 := NewGetField(0, types.Int64, "col1", true)
	gf2 := NewGetField(1, types.Int64, "col2", true)
	cmp := newComparison(gf1, gf2)
	row := sql.Row{1, 1}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		res, err := cmp.Compare(ctx, row)
		require.NoError(b, err)
		require.Equal(b, 0, res)
	}
}

// BenchmarkValueComparison
// BenchmarkValueComparison-14    	 4115744	       285.8 ns/op
func BenchmarkValueComparison(b *testing.B) {
	ctx := sql.NewEmptyContext()
	gf1 := NewGetField(0, types.Int64, "col1", true)
	gf2 := NewGetField(1, types.Int64, "col2", true)
	cmp := newComparison(gf1, gf2)
	row := sql.ValueRow{
		{
			Val: binary.LittleEndian.AppendUint64(nil, uint64(1)),
			Typ: sqltypes.Int64,
		},
		{
			Val: binary.LittleEndian.AppendUint64(nil, uint64(1)),
			Typ: sqltypes.Int64,
		},
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		res, err := cmp.CompareValue(ctx, row)
		require.NoError(b, err)
		require.Equal(b, 0, res)
	}
}
