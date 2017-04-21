package expression

import (
	"testing"

	"gopkg.in/sqle/sqle.v0/sql"

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
	sql.String: {
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
	sql.Integer: {
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
	sql.String: {
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
	sql.Integer: {
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
	assert := require.New(t)
	for resultType, cmpCase := range comparisonCases {
		get0 := NewGetField(0, resultType, "col1", true)
		assert.NotNil(get0)
		get1 := NewGetField(1, resultType, "col2", true)
		assert.NotNil(get1)
		eq := NewEquals(get0, get1)
		assert.NotNil(eq)
		assert.Equal(sql.Boolean, eq.Type())
		for cmpResult, cases := range cmpCase {
			for _, pair := range cases {
				row := sql.NewRow(pair[0], pair[1])
				assert.NotNil(row)
				cmp := eq.Eval(row)
				if cmpResult == testEqual {
					assert.Equal(true, cmp)
				} else if cmpResult == testNil {
					assert.Nil(cmp)
				} else {
					assert.Equal(false, cmp)
				}
			}
		}
	}
}

func TestComparisons_LessThan(t *testing.T) {
	assert := require.New(t)
	for resultType, cmpCase := range comparisonCases {
		get0 := NewGetField(0, resultType, "col1", true)
		assert.NotNil(get0)
		get1 := NewGetField(1, resultType, "col2", true)
		assert.NotNil(get1)
		eq := NewLessThan(get0, get1)
		assert.NotNil(eq)
		assert.Equal(sql.Boolean, eq.Type())
		for cmpResult, cases := range cmpCase {
			for _, pair := range cases {
				row := sql.NewRow(pair[0], pair[1])
				assert.NotNil(row)
				cmp := eq.Eval(row)
				if cmpResult == testLess {
					assert.Equal(true, cmp, "%v < %v", pair[0], pair[1])
				} else if cmpResult == testNil {
					assert.Nil(cmp)
				} else {
					assert.Equal(false, cmp)
				}
			}
		}
	}
}

func TestComparisons_GreaterThan(t *testing.T) {
	assert := require.New(t)
	for resultType, cmpCase := range comparisonCases {
		get0 := NewGetField(0, resultType, "col1", true)
		assert.NotNil(get0)
		get1 := NewGetField(1, resultType, "col2", true)
		assert.NotNil(get1)
		eq := NewGreaterThan(get0, get1)
		assert.NotNil(eq)
		assert.Equal(sql.Boolean, eq.Type())
		for cmpResult, cases := range cmpCase {
			for _, pair := range cases {
				row := sql.NewRow(pair[0], pair[1])
				assert.NotNil(row)
				cmp := eq.Eval(row)
				if cmpResult == testGreater {
					assert.Equal(true, cmp)
				} else if cmpResult == testNil {
					assert.Nil(cmp)
				} else {
					assert.Equal(false, cmp)
				}
			}
		}
	}
}

func TestComparisons_Regexp(t *testing.T) {
	assert := require.New(t)
	for resultType, cmpCase := range likeComparisonCases {
		get0 := NewGetField(0, resultType, "col1", true)
		assert.NotNil(get0)
		get1 := NewGetField(1, resultType, "col2", true)
		assert.NotNil(get1)
		eq := NewRegexp(get0, get1)
		assert.NotNil(eq)
		assert.Equal(sql.Boolean, eq.Type())
		for cmpResult, cases := range cmpCase {
			for _, pair := range cases {
				row := sql.NewRow(pair[0], pair[1])
				assert.NotNil(row)
				cmp := eq.Eval(row)
				if cmpResult == testRegexp {
					assert.Equal(true, cmp)
				} else if cmpResult == testNil {
					assert.Nil(cmp)
				} else {
					assert.Equal(false, cmp)
				}
			}
		}
	}
}
