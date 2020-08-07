package expression_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/internal/regex"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
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
	sql.LongText: {
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
	sql.LongText: {
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

func TestEquals(t *testing.T) {
	require := require.New(t)
	for resultType, cmpCase := range comparisonCases {
		get0 := expression.NewGetField(0, resultType, "col1", true)
		require.NotNil(get0)
		get1 := expression.NewGetField(1, resultType, "col2", true)
		require.NotNil(get1)
		eq := expression.NewEquals(get0, get1)
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

func TestLessThan(t *testing.T) {
	require := require.New(t)
	for resultType, cmpCase := range comparisonCases {
		get0 := expression.NewGetField(0, resultType, "col1", true)
		require.NotNil(get0)
		get1 := expression.NewGetField(1, resultType, "col2", true)
		require.NotNil(get1)
		eq := expression.NewLessThan(get0, get1)
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

func TestGreaterThan(t *testing.T) {
	require := require.New(t)
	for resultType, cmpCase := range comparisonCases {
		get0 := expression.NewGetField(0, resultType, "col1", true)
		require.NotNil(get0)
		get1 := expression.NewGetField(1, resultType, "col2", true)
		require.NotNil(get1)
		eq := expression.NewGreaterThan(get0, get1)
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

func TestRegexp(t *testing.T) {
	for _, engine := range regex.Engines() {
		regex.SetDefault(engine)
		t.Run(engine, testRegexpCases)
	}
}

func testRegexpCases(t *testing.T) {
	t.Helper()
	require := require.New(t)

	for resultType, cmpCase := range likeComparisonCases {
		get0 := expression.NewGetField(0, resultType, "col1", true)
		require.NotNil(get0)
		get1 := expression.NewGetField(1, resultType, "col2", true)
		require.NotNil(get1)
		for cmpResult, cases := range cmpCase {
			for _, pair := range cases {
				eq := expression.NewRegexp(get0, get1)
				require.NotNil(eq)
				require.Equal(sql.Boolean, eq.Type())

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

func TestInvalidRegexp(t *testing.T) {
	t.Helper()
	require := require.New(t)

	col1 := expression.NewGetField(0, sql.LongText, "col1", true)
	invalid := expression.NewLiteral("*col1", sql.LongText)
	r := expression.NewRegexp(col1, invalid)
	row := sql.NewRow("col1")

	_, err := r.Eval(sql.NewEmptyContext(), row)
	require.Error(err)
}

func eval(t *testing.T, e sql.Expression, row sql.Row) interface{} {
	t.Helper()
	v, err := e.Eval(sql.NewEmptyContext(), row)
	require.NoError(t, err)
	return v
}
