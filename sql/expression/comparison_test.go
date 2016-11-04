package expression

import (
	"testing"

	"github.com/gitql/gitql/sql"
	"github.com/stretchr/testify/require"
)

const (
	testEqual   = 1
	testLess    = 2
	testGreater = 3
)

var comparisonCases = map[sql.Type]map[int][][]interface{}{
	sql.String: map[int][][]interface{}{
		testEqual: [][]interface{}{
			[]interface{}{"foo", "foo"},
			[]interface{}{"", ""},
		},
		testLess: [][]interface{}{
			[]interface{}{"a", "b"},
			[]interface{}{"", "1"},
		},
		testGreater: [][]interface{}{
			[]interface{}{"b", "a"},
			[]interface{}{"1", ""},
		},
	},
	sql.Integer: map[int][][]interface{}{
		testEqual: [][]interface{}{
			[]interface{}{int32(1), int32(1)},
			[]interface{}{int32(0), int32(0)},
		},
		testLess: [][]interface{}{
			[]interface{}{int32(-1), int32(0)},
			[]interface{}{int32(1), int32(2)},
		},
		testGreater: [][]interface{}{
			[]interface{}{int32(2), int32(1)},
			[]interface{}{int32(0), int32(-1)},
		},
	},
}

func TestComparisons_Equals(t *testing.T) {
	assert := require.New(t)
	for resultType, cmpCase := range comparisonCases {
		get0 := NewGetField(0, resultType, "col1")
		assert.NotNil(get0)
		get1 := NewGetField(1, resultType, "col2")
		assert.NotNil(get1)
		eq := NewEquals(get0, get1)
		assert.NotNil(eq)
		assert.Equal(sql.Boolean, eq.Type())
		for cmpResult, cases := range cmpCase {
			for _, pair := range cases {
				row := sql.NewMemoryRow(pair[0], pair[1])
				assert.NotNil(row)
				cmp := eq.Eval(row)
				assert.NotNil(cmp)
				if cmpResult == testEqual {
					assert.Equal(true, cmp)
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
		get0 := NewGetField(0, resultType, "col1")
		assert.NotNil(get0)
		get1 := NewGetField(1, resultType, "col2")
		assert.NotNil(get1)
		eq := NewLessThan(get0, get1)
		assert.NotNil(eq)
		assert.Equal(sql.Boolean, eq.Type())
		for cmpResult, cases := range cmpCase {
			for _, pair := range cases {
				row := sql.NewMemoryRow(pair[0], pair[1])
				assert.NotNil(row)
				cmp := eq.Eval(row)
				assert.NotNil(cmp)
				if cmpResult == testLess {
					assert.Equal(true, cmp, "%v < %v", pair[0], pair[1])
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
		get0 := NewGetField(0, resultType, "col1")
		assert.NotNil(get0)
		get1 := NewGetField(1, resultType, "col2")
		assert.NotNil(get1)
		eq := NewGreaterThan(get0, get1)
		assert.NotNil(eq)
		assert.Equal(sql.Boolean, eq.Type())
		for cmpResult, cases := range cmpCase {
			for _, pair := range cases {
				row := sql.NewMemoryRow(pair[0], pair[1])
				assert.NotNil(row)
				cmp := eq.Eval(row)
				assert.NotNil(cmp)
				if cmpResult == testGreater {
					assert.Equal(true, cmp)
				} else {
					assert.Equal(false, cmp)
				}
			}
		}
	}
}
