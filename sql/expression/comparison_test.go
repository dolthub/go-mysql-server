package expression

import (
	"testing"

	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"github.com/stretchr/testify/require"
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
	sql.Text: {
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
	sql.Int32: {
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
	sql.Text: {
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
	sql.Int32: {
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

func TestComparisons_Equals(t *testing.T) {
	require := require.New(t)
	for resultType, cmpCase := range comparisonCases {
		get0 := NewGetField(0, resultType, "col1", true)
		require.NotNil(get0)
		get1 := NewGetField(1, resultType, "col2", true)
		require.NotNil(get1)
		eq := NewEquals(get0, get1)
		require.NotNil(eq)
		require.Equal(sql.Boolean, eq.Type())
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

func TestComparisons_LessThan(t *testing.T) {
	require := require.New(t)
	for resultType, cmpCase := range comparisonCases {
		get0 := NewGetField(0, resultType, "col1", true)
		require.NotNil(get0)
		get1 := NewGetField(1, resultType, "col2", true)
		require.NotNil(get1)
		eq := NewLessThan(get0, get1)
		require.NotNil(eq)
		require.Equal(sql.Boolean, eq.Type())
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

func TestComparisons_GreaterThan(t *testing.T) {
	require := require.New(t)
	for resultType, cmpCase := range comparisonCases {
		get0 := NewGetField(0, resultType, "col1", true)
		require.NotNil(get0)
		get1 := NewGetField(1, resultType, "col2", true)
		require.NotNil(get1)
		eq := NewGreaterThan(get0, get1)
		require.NotNil(eq)
		require.Equal(sql.Boolean, eq.Type())
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

func TestComparisons_Regexp(t *testing.T) {
	require := require.New(t)
	for resultType, cmpCase := range likeComparisonCases {
		get0 := NewGetField(0, resultType, "col1", true)
		require.NotNil(get0)
		get1 := NewGetField(1, resultType, "col2", true)
		require.NotNil(get1)
		eq := NewRegexp(get0, get1)
		require.NotNil(eq)
		require.Equal(sql.Boolean, eq.Type())
		for cmpResult, cases := range cmpCase {
			for _, pair := range cases {
				row := sql.NewRow(pair[0], pair[1])
				require.NotNil(row)
				cmp := eval(t, eq, row)
				if cmpResult == testRegexp {
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
