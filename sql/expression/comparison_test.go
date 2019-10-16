package expression_test

import (
	"testing"

	"github.com/src-d/go-mysql-server/internal/regex"
	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
	errors "gopkg.in/src-d/go-errors.v1"

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

	col1 := expression.NewGetField(0, sql.Text, "col1", true)
	invalid := expression.NewLiteral("*col1", sql.Text)
	r := expression.NewRegexp(col1, invalid)
	row := sql.NewRow("col1")

	_, err := r.Eval(sql.NewEmptyContext(), row)
	require.Error(err)
}

func TestIn(t *testing.T) {
	testCases := []struct {
		name   string
		left   sql.Expression
		right  sql.Expression
		row    sql.Row
		result interface{}
		err    *errors.Kind
	}{
		{
			"left is nil",
			expression.NewLiteral(nil, sql.Null),
			expression.NewTuple(
				expression.NewLiteral(int64(1), sql.Int64),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			nil,
			nil,
			nil,
		},
		{
			"left and right don't have the same cols",
			expression.NewLiteral(1, sql.Int64),
			expression.NewTuple(
				expression.NewTuple(
					expression.NewLiteral(int64(1), sql.Int64),
					expression.NewLiteral(int64(1), sql.Int64),
				),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			nil,
			nil,
			expression.ErrInvalidOperandColumns,
		},
		{
			"right is an unsupported operand",
			expression.NewLiteral(1, sql.Int64),
			expression.NewLiteral(int64(2), sql.Int64),
			nil,
			nil,
			expression.ErrUnsupportedInOperand,
		},
		{
			"left is in right",
			expression.NewGetField(0, sql.Int64, "foo", false),
			expression.NewTuple(
				expression.NewGetField(0, sql.Int64, "foo", false),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			sql.NewRow(int64(1)),
			true,
			nil,
		},
		{
			"left is not in right",
			expression.NewGetField(0, sql.Int64, "foo", false),
			expression.NewTuple(
				expression.NewGetField(1, sql.Int64, "bar", false),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			sql.NewRow(int64(1), int64(3)),
			false,
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := expression.NewIn(tt.left, tt.right).
				Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.result, result)
			}
		})
	}
}

func TestInSubquery(t *testing.T) {
	ctx := sql.NewEmptyContext()
	table := memory.NewTable("foo", sql.Schema{
		{Name: "t", Source: "foo", Type: sql.Text},
	})

	require.NoError(t, table.Insert(ctx, sql.Row{"one"}))
	require.NoError(t, table.Insert(ctx, sql.Row{"two"}))
	require.NoError(t, table.Insert(ctx, sql.Row{"three"}))

	project := func(expr sql.Expression) sql.Node {
		return plan.NewProject([]sql.Expression{
			expr,
		}, plan.NewResolvedTable(table))
	}

	testCases := []struct {
		name   string
		left   sql.Expression
		right  sql.Node
		row    sql.Row
		result interface{}
		err    *errors.Kind
	}{
		{
			"left is nil",
			expression.NewLiteral(nil, sql.Null),
			project(
				expression.NewLiteral(int64(1), sql.Int64),
			),
			nil,
			nil,
			nil,
		},
		{
			"left and right don't have the same cols",
			expression.NewTuple(
				expression.NewLiteral(int64(1), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			),
			project(
				expression.NewLiteral(int64(2), sql.Int64),
			),
			nil,
			nil,
			expression.ErrInvalidOperandColumns,
		},
		{
			"left is in right",
			expression.NewGetField(0, sql.Text, "foo", false),
			project(
				expression.NewGetField(0, sql.Text, "foo", false),
			),
			sql.NewRow("two"),
			true,
			nil,
		},
		{
			"left is not in right",
			expression.NewGetField(0, sql.Text, "foo", false),
			project(
				expression.NewGetField(0, sql.Text, "foo", false),
			),
			sql.NewRow("four"),
			false,
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := expression.NewIn(
				tt.left,
				expression.NewSubquery(tt.right),
			).Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.result, result)
			}
		})
	}
}

func TestNotIn(t *testing.T) {
	testCases := []struct {
		name   string
		left   sql.Expression
		right  sql.Expression
		row    sql.Row
		result interface{}
		err    *errors.Kind
	}{
		{
			"left is nil",
			expression.NewLiteral(nil, sql.Null),
			expression.NewTuple(
				expression.NewLiteral(int64(1), sql.Int64),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			nil,
			nil,
			nil,
		},
		{
			"left and right don't have the same cols",
			expression.NewLiteral(1, sql.Int64),
			expression.NewTuple(
				expression.NewTuple(
					expression.NewLiteral(int64(1), sql.Int64),
					expression.NewLiteral(int64(1), sql.Int64),
				),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			nil,
			nil,
			expression.ErrInvalidOperandColumns,
		},
		{
			"right is an unsupported operand",
			expression.NewLiteral(1, sql.Int64),
			expression.NewLiteral(int64(2), sql.Int64),
			nil,
			nil,
			expression.ErrUnsupportedInOperand,
		},
		{
			"left is in right",
			expression.NewGetField(0, sql.Int64, "foo", false),
			expression.NewTuple(
				expression.NewGetField(0, sql.Int64, "foo", false),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			sql.NewRow(int64(1)),
			false,
			nil,
		},
		{
			"left is not in right",
			expression.NewGetField(0, sql.Int64, "foo", false),
			expression.NewTuple(
				expression.NewGetField(1, sql.Int64, "bar", false),
				expression.NewLiteral(int64(2), sql.Int64),
			),
			sql.NewRow(int64(1), int64(3)),
			true,
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := expression.NewNotIn(tt.left, tt.right).
				Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.result, result)
			}
		})
	}
}

func TestNotInSubquery(t *testing.T) {
	ctx := sql.NewEmptyContext()
	table := memory.NewTable("foo", sql.Schema{
		{Name: "t", Source: "foo", Type: sql.Text},
	})

	require.NoError(t, table.Insert(ctx, sql.Row{"one"}))
	require.NoError(t, table.Insert(ctx, sql.Row{"two"}))
	require.NoError(t, table.Insert(ctx, sql.Row{"three"}))

	project := func(expr sql.Expression) sql.Node {
		return plan.NewProject([]sql.Expression{
			expr,
		}, plan.NewResolvedTable(table))
	}

	testCases := []struct {
		name   string
		left   sql.Expression
		right  sql.Node
		row    sql.Row
		result interface{}
		err    *errors.Kind
	}{
		{
			"left is nil",
			expression.NewLiteral(nil, sql.Null),
			project(
				expression.NewLiteral(int64(1), sql.Int64),
			),
			nil,
			nil,
			nil,
		},
		{
			"left and right don't have the same cols",
			expression.NewTuple(
				expression.NewLiteral(int64(1), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			),
			project(
				expression.NewLiteral(int64(2), sql.Int64),
			),
			nil,
			nil,
			expression.ErrInvalidOperandColumns,
		},
		{
			"left is in right",
			expression.NewGetField(0, sql.Text, "foo", false),
			project(
				expression.NewGetField(0, sql.Text, "foo", false),
			),
			sql.NewRow("two"),
			false,
			nil,
		},
		{
			"left is not in right",
			expression.NewGetField(0, sql.Text, "foo", false),
			project(
				expression.NewGetField(0, sql.Text, "foo", false),
			),
			sql.NewRow("four"),
			true,
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := expression.NewNotIn(
				tt.left,
				expression.NewSubquery(tt.right),
			).Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.result, result)
			}
		})
	}
}

func eval(t *testing.T, e sql.Expression, row sql.Row) interface{} {
	t.Helper()
	v, err := e.Eval(sql.NewEmptyContext(), row)
	require.NoError(t, err)
	return v
}
