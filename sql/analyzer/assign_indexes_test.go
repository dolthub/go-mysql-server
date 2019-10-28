package analyzer

import (
	"github.com/src-d/go-mysql-server/sql/test_util"
	"testing"

	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestNegateIndex(t *testing.T) {
	require := require.New(t)

	catalog := sql.NewCatalog()
	idx1 := &testutil.MergeableDummyIndex{
		"t1",
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "t1", "foo", false),
		},
	}
	done, ready, err := catalog.AddIndex(idx1)
	require.NoError(err)
	close(done)
	<-ready

	a := NewDefault(catalog)

	t1 := memory.NewTable("t1", sql.Schema{
		{Name: "foo", Type: sql.Int64, Source: "t1"},
	})

	node := plan.NewProject(
		[]sql.Expression{},
		plan.NewFilter(
			expression.NewNot(
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "t1", "foo", false),
					expression.NewLiteral(int64(1), sql.Int64),
				),
			),
			plan.NewResolvedTable(t1),
		),
	)

	result, err := assignIndexes(a, node)
	require.NoError(err)

	lookupIdxs, ok := result["t1"]
	require.True(ok)

	negate, ok := lookupIdxs.lookup.(*testutil.NegateIndexLookup)
	require.True(ok)
	require.True(negate.Value == "1")
}

func TestAssignIndexes(t *testing.T) {
	require := require.New(t)

	catalog := sql.NewCatalog()
	idx1 := &testutil.MergeableDummyIndex{
		"t2",
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "t2", "bar", false),
		},
	}
	done, ready, err := catalog.AddIndex(idx1)
	require.NoError(err)
	close(done)
	<-ready

	idx2 := &testutil.MergeableDummyIndex{
		"t1",
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "t1", "foo", false),
		},
	}
	done, ready, err = catalog.AddIndex(idx2)

	require.NoError(err)
	close(done)
	<-ready

	idx3 := &testutil.UnmergeableDummyIndex{
		"t1",
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
		},
	}

	done, ready, err = catalog.AddIndex(idx3)
	require.NoError(err)
	close(done)
	<-ready

	a := NewDefault(catalog)

	t1 := memory.NewTable("t1", sql.Schema{
		{Name: "foo", Type: sql.Int64, Source: "t1"},
	})

	t2 := memory.NewTable("t2", sql.Schema{
		{Name: "bar", Type: sql.Int64, Source: "t2"},
		{Name: "baz", Type: sql.Int64, Source: "t2"},
	})

	node := plan.NewProject(
		[]sql.Expression{},
		plan.NewFilter(
			expression.NewOr(
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "t2", "bar", false),
					expression.NewLiteral(int64(1), sql.Int64),
				),
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "t1", "foo", false),
					expression.NewLiteral(int64(2), sql.Int64),
				),
			),
			plan.NewInnerJoin(
				plan.NewResolvedTable(t1),
				plan.NewResolvedTable(t2),
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "t1", "foo", false),
					expression.NewGetFieldWithTable(0, sql.Int64, "t2", "baz", false),
				),
			),
		),
	)

	result, err := assignIndexes(a, node)
	require.NoError(err)

	lookupIdxs, ok := result["t1"]
	require.True(ok)

	mergeable, ok := lookupIdxs.lookup.(*testutil.MergeableIndexLookup)
	require.True(ok)
	require.True(mergeable.Id == "2")

	lookupIdxs, ok = result["t2"]
	require.True(ok)

	mergeable, ok = lookupIdxs.lookup.(*testutil.MergeableIndexLookup)
	require.True(ok)
	require.True(mergeable.Id == "1")

	node = plan.NewProject(
		[]sql.Expression{},
		plan.NewFilter(
			expression.NewOr(
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
					expression.NewLiteral(int64(1), sql.Int64),
				),
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
					expression.NewLiteral(int64(2), sql.Int64),
				),
			),
			plan.NewResolvedTable(t1),
		),
	)

	result, err = assignIndexes(a, node)
	require.NoError(err)

	lookupIdxs, ok = result["t1"]
	require.False(ok)

	node = plan.NewProject(
		[]sql.Expression{},
		plan.NewFilter(
			expression.NewIn(
				expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
				expression.NewTuple(expression.NewLiteral(int64(1), sql.Int64), expression.NewLiteral(int64(2), sql.Int64)),
			),
			plan.NewResolvedTable(t1),
		),
	)

	result, err = assignIndexes(a, node)
	require.NoError(err)

	lookupIdxs, ok = result["t1"]
	require.False(ok)
}

func TestGetIndexes(t *testing.T) {
	indexes := []*testutil.MergeableDummyIndex{
		{
			"t1",
			[]sql.Expression{
				col(0, "t1", "bar"),
			},
		},
		{
			"t2",
			[]sql.Expression{
				col(0, "t2", "foo"),
				col(0, "t2", "bar"),
			},
		},
		{
			"t2",
			[]sql.Expression{
				col(0, "t2", "bar"),
			},
		},
	}

	testCases := []struct {
		expr     sql.Expression
		expected map[string]*indexLookup
		ok       bool
	}{
		{
			eq(
				col(0, "t1", "bar"),
				col(1, "t1", "baz"),
			),
			map[string]*indexLookup{},
			true,
		},
		{
			eq(
				col(0, "t1", "bar"),
				lit(1),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.MergeableIndexLookup{Id: "1"},
					[]sql.Index{indexes[0]},
				},
			},
			true,
		},
		{
			or(
				eq(
					col(0, "t1", "bar"),
					lit(1),
				),
				eq(
					col(0, "t1", "bar"),
					lit(2),
				),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.MergeableIndexLookup{Id: "1", Unions: []string{"2"}},
					[]sql.Index{
						indexes[0],
						indexes[0],
					},
				},
			},
			true,
		},
		{
			and(
				eq(
					col(0, "t1", "bar"),
					lit(1),
				),
				eq(
					col(0, "t1", "bar"),
					lit(2),
				),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.MergeableIndexLookup{Id: "1", Intersections: []string{"2"}},
					[]sql.Index{
						indexes[0],
						indexes[0],
					},
				},
			},
			true,
		},
		{
			and(
				or(
					eq(
						col(0, "t1", "bar"),
						lit(1),
					),
					eq(
						col(0, "t1", "bar"),
						lit(2),
					),
				),
				or(
					eq(
						col(0, "t1", "bar"),
						lit(3),
					),
					eq(
						col(0, "t1", "bar"),
						lit(4),
					),
				),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.MergeableIndexLookup{Id: "1", Unions: []string{"2", "4"}, Intersections: []string{"3"}},
					[]sql.Index{
						indexes[0],
						indexes[0],
						indexes[0],
						indexes[0],
					},
				},
			},
			true,
		},
		{
			or(
				or(
					eq(
						col(0, "t1", "bar"),
						lit(1),
					),
					eq(
						col(0, "t1", "bar"),
						lit(2),
					),
				),
				or(
					eq(
						col(0, "t1", "bar"),
						lit(3),
					),
					eq(
						col(0, "t1", "bar"),
						lit(4),
					),
				),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.MergeableIndexLookup{Id: "1", Unions: []string{"2", "3", "4"}},
					[]sql.Index{
						indexes[0],
						indexes[0],
						indexes[0],
						indexes[0],
					},
				},
			},
			true,
		},
		{
			expression.NewIn(
				col(0, "t1", "bar"),
				expression.NewTuple(
					lit(1),
					lit(2),
					lit(3),
					lit(4),
				),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.MergeableIndexLookup{Id: "1", Unions: []string{"2", "3", "4"}},
					[]sql.Index{indexes[0]},
				},
			},
			true,
		},
		{
			and(
				eq(
					col(0, "t2", "foo"),
					lit(1),
				),
				and(
					eq(
						col(0, "t2", "baz"),
						lit(4),
					),
					and(
						eq(
							col(0, "t2", "bar"),
							lit(2),
						),
						eq(
							col(0, "t1", "bar"),
							lit(3),
						),
					),
				),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.MergeableIndexLookup{Id: "3"},
					[]sql.Index{indexes[0]},
				},
				"t2": &indexLookup{
					&testutil.MergeableIndexLookup{Id: "1, 2"},
					[]sql.Index{indexes[1]},
				},
			},
			true,
		},
		{
			or(
				eq(
					col(0, "t2", "bar"),
					lit(5),
				),
				and(
					eq(
						col(0, "t2", "foo"),
						lit(1),
					),
					and(
						eq(
							col(0, "t2", "baz"),
							lit(4),
						),
						and(
							eq(
								col(0, "t2", "bar"),
								lit(2),
							),
							eq(
								col(0, "t1", "bar"),
								lit(3),
							),
						),
					),
				),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.MergeableIndexLookup{Id: "3"},
					[]sql.Index{indexes[0]},
				},
				"t2": &indexLookup{
					&testutil.MergeableIndexLookup{Id: "5", Unions: []string{"1, 2"}},
					[]sql.Index{
						indexes[2],
						indexes[1],
					},
				},
			},
			true,
		},
		{
			gt(
				col(0, "t1", "bar"),
				lit(1),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.DescendIndexLookup{Gt: []interface{}{int64(1)}},
					[]sql.Index{indexes[0]},
				},
			},
			true,
		},
		{
			lt(
				col(0, "t1", "bar"),
				lit(1),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.AscendIndexLookup{Lt: []interface{}{int64(1)}},
					[]sql.Index{indexes[0]},
				},
			},
			true,
		},
		{
			gte(
				col(0, "t1", "bar"),
				lit(1),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.AscendIndexLookup{Gte: []interface{}{int64(1)}},
					[]sql.Index{indexes[0]},
				},
			},
			true,
		},
		{
			lte(
				col(0, "t1", "bar"),
				lit(1),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.DescendIndexLookup{Lte: []interface{}{int64(1)}},
					[]sql.Index{indexes[0]},
				},
			},
			true,
		},
		{
			expression.NewBetween(
				col(0, "t1", "bar"),
				lit(1),
				lit(5),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.MergedIndexLookup{
						[]sql.IndexLookup{
							&testutil.AscendIndexLookup{
								Gte: []interface{}{int64(1)},
								Lt:  []interface{}{int64(5)},
							},
							&testutil.DescendIndexLookup{
								Gt:  []interface{}{int64(1)},
								Lte: []interface{}{int64(5)},
							},
						},
					},
					[]sql.Index{indexes[0]},
				},
			},
			true,
		},
		{
			not(
				eq(
					col(0, "t1", "bar"),
					lit(1),
				),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.NegateIndexLookup{
						Value: "1",
					},
					[]sql.Index{indexes[0]},
				},
			},
			true,
		},
		{

			not(
				gt(
					col(0, "t1", "bar"),
					lit(10),
				),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.DescendIndexLookup{Lte: []interface{}{int64(10)}},
					[]sql.Index{indexes[0]},
				},
			},
			true,
		},
		{

			not(
				gte(
					col(0, "t1", "bar"),
					lit(10),
				),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.AscendIndexLookup{Lt: []interface{}{int64(10)}},
					[]sql.Index{indexes[0]},
				},
			},
			true,
		},
		{

			not(
				lte(
					col(0, "t1", "bar"),
					lit(10),
				),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.DescendIndexLookup{Gt: []interface{}{int64(10)}},
					[]sql.Index{indexes[0]},
				},
			},
			true,
		},
		{

			not(
				lt(
					col(0, "t1", "bar"),
					lit(10),
				),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.AscendIndexLookup{Gte: []interface{}{int64(10)}},
					[]sql.Index{indexes[0]},
				},
			},
			true,
		},
		{
			not(
				and(
					eq(
						col(0, "t1", "bar"),
						lit(10),
					),
					eq(
						col(0, "t1", "bar"),
						lit(11),
					),
				),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.MergedIndexLookup{
						Children: []sql.IndexLookup{
							&testutil.NegateIndexLookup{
								Value: "10",
							},
							&testutil.NegateIndexLookup{
								Value: "11",
							},
						},
					},
					[]sql.Index{
						indexes[0],
						indexes[0],
					},
				},
			},
			true,
		},
		{
			not(
				or(
					eq(
						col(0, "t1", "bar"),
						lit(10),
					),
					eq(
						col(0, "t1", "bar"),
						lit(11),
					),
				),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.MergeableIndexLookup{
						Id:            "not 10",
						Intersections: []string{"not 11"},
					},
					[]sql.Index{
						indexes[0],
						indexes[0],
					},
				},
			},
			true,
		},
		{
			// `NOT` doesn't work for multicolumn indexes, so the expression
			// will use indexes if there are indexes for the single columns
			// involved. In this case there is a index for the column `t2.bar`.
			not(
				or(
					eq(
						col(0, "t2", "foo"),
						lit(100),
					),
					eq(
						col(0, "t2", "bar"),
						lit(110),
					),
				),
			),
			map[string]*indexLookup{
				"t2": &indexLookup{
					&testutil.NegateIndexLookup{
						Value: "110",
					},
					[]sql.Index{
						indexes[2],
					},
				},
			},
			true,
		},
		{
			expression.NewNotIn(
				col(0, "t1", "bar"),
				expression.NewTuple(
					lit(1),
					lit(2),
					lit(3),
					lit(4),
				),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&testutil.MergeableIndexLookup{
						Id:            "not 1",
						Intersections: []string{"not 2", "not 3", "not 4"},
					},
					[]sql.Index{indexes[0]},
				},
			},
			true,
		},
	}

	catalog := sql.NewCatalog()
	for _, idx := range indexes {
		done, ready, err := catalog.AddIndex(idx)
		require.NoError(t, err)
		close(done)
		<-ready
	}

	a := NewDefault(catalog)

	for _, tt := range testCases {
		t.Run(tt.expr.String(), func(t *testing.T) {
			require := require.New(t)

			result, err := getIndexes(tt.expr, nil, a)
			if tt.ok {
				require.NoError(err)
				require.Equal(tt.expected, result)
			} else {
				require.Error(err)
			}
		})
	}
}

func TestGetMultiColumnIndexes(t *testing.T) {
	require := require.New(t)

	catalog := sql.NewCatalog()
	indexes := []*testutil.MergeableDummyIndex{
		{
			"t1",
			[]sql.Expression{
				col(1, "t1", "foo"),
				col(2, "t1", "bar"),
			},
		},
		{
			"t2",
			[]sql.Expression{
				col(0, "t2", "foo"),
				col(1, "t2", "bar"),
				col(2, "t2", "baz"),
			},
		},
		{
			"t2",
			[]sql.Expression{
				col(0, "t2", "foo"),
				col(0, "t2", "bar"),
			},
		},
		{
			"t3",
			[]sql.Expression{col(0, "t3", "foo")},
		},
		{
			"t4",
			[]sql.Expression{
				col(1, "t4", "foo"),
				col(2, "t4", "bar"),
			},
		},
	}

	for _, idx := range indexes {
		done, ready, err := catalog.AddIndex(idx)
		require.NoError(err)
		close(done)
		<-ready
	}

	a := NewDefault(catalog)

	used := make(map[sql.Expression]struct{})
	exprs := []sql.Expression{
		eq(
			col(2, "t2", "bar"),
			lit(2),
		),
		eq(
			col(2, "t2", "foo"),
			lit(1),
		),
		eq(
			lit(3),
			col(2, "t2", "baz"),
		),
		eq(
			col(2, "t3", "foo"),
			lit(4),
		),
		eq(
			col(2, "t1", "foo"),
			lit(5),
		),
		eq(
			col(2, "t1", "bar"),
			lit(6),
		),
		expression.NewBetween(
			col(2, "t4", "bar"),
			lit(2),
			lit(5),
		),
		expression.NewBetween(
			col(2, "t4", "foo"),
			lit(1),
			lit(6),
		),
	}
	result, err := getMultiColumnIndexes(exprs, a, used, nil)
	require.NoError(err)

	expected := map[string]*indexLookup{
		"t1": &indexLookup{
			&testutil.MergeableIndexLookup{Id: "5, 6"},
			[]sql.Index{indexes[0]},
		},
		"t2": &indexLookup{
			&testutil.MergeableIndexLookup{Id: "1, 2, 3"},
			[]sql.Index{indexes[1]},
		},
		"t4": &indexLookup{
			&testutil.MergedIndexLookup{[]sql.IndexLookup{
				&testutil.AscendIndexLookup{
					Gte: []interface{}{int64(1), int64(2)},
					Lt:  []interface{}{int64(6), int64(5)},
				},
				&testutil.DescendIndexLookup{
					Gt:  []interface{}{int64(1), int64(2)},
					Lte: []interface{}{int64(6), int64(5)},
				},
			}},
			[]sql.Index{indexes[4]},
		},
	}

	require.Equal(expected, result)

	expectedUsed := map[sql.Expression]struct{}{
		exprs[0]: struct{}{},
		exprs[1]: struct{}{},
		exprs[2]: struct{}{},
		exprs[4]: struct{}{},
		exprs[5]: struct{}{},
		exprs[6]: struct{}{},
		exprs[7]: struct{}{},
	}
	require.Equal(expectedUsed, used)
}

func TestContainsSources(t *testing.T) {
	testCases := []struct {
		name     string
		haystack []string
		needle   []string
		expected bool
	}{
		{
			"needle is in haystack",
			[]string{"a", "b", "c"},
			[]string{"c", "b"},
			true,
		},
		{
			"needle is not in haystack",
			[]string{"a", "b", "c"},
			[]string{"d", "b"},
			false,
		},
		{
			"no elements in needle",
			[]string{"a", "b", "c"},
			nil,
			true,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(
				t,
				containsSources(tt.haystack, tt.needle),
				tt.expected,
			)
		})
	}
}

func TestNodeSources(t *testing.T) {
	sources := nodeSources(
		plan.NewResolvedTable(
			memory.NewTable("foo", sql.Schema{
				{Source: "foo"},
				{Source: "foo"},
				{Source: "bar"},
				{Source: "baz"},
			}),
		),
	)

	expected := []string{"foo", "bar", "baz"}
	require.Equal(t, expected, sources)
}

func TestExpressionSources(t *testing.T) {
	sources := expressionSources(expression.JoinAnd(
		col(0, "foo", "bar"),
		col(0, "foo", "qux"),
		and(
			eq(
				col(0, "bar", "baz"),
				lit(1),
			),
			eq(
				col(0, "baz", "baz"),
				lit(2),
			),
		),
	))

	expected := []string{"foo", "bar", "baz"}
	require.Equal(t, expected, sources)
}

type DummyIndexLookup struct{}

func (DummyIndexLookup) Indexes() []string { return nil }

func (DummyIndexLookup) Values(sql.Partition) (sql.IndexValueIter, error) {
	return nil, nil
}

var _ sql.Index = (*testutil.MergeableDummyIndex)(nil)
var _ sql.Index = (*testutil.UnmergeableDummyIndex)(nil)
var _ sql.AscendIndex = (*testutil.MergeableDummyIndex)(nil)
var _ sql.DescendIndex = (*testutil.MergeableDummyIndex)(nil)
var _ sql.NegateIndex = (*testutil.MergeableDummyIndex)(nil)

func TestIndexesIntersection(t *testing.T) {
	require := require.New(t)

	idx1, idx2 := &testutil.MergeableDummyIndex{TableName: "bar"}, &testutil.MergeableDummyIndex{TableName: "foo"}

	left := map[string]*indexLookup{
		"a": &indexLookup{&testutil.MergeableIndexLookup{Id: "a"}, nil},
		"b": &indexLookup{&testutil.MergeableIndexLookup{Id: "b"}, []sql.Index{idx1}},
		"c": &indexLookup{new(DummyIndexLookup), nil},
	}

	right := map[string]*indexLookup{
		"b": &indexLookup{&testutil.MergeableIndexLookup{Id: "b2"}, []sql.Index{idx2}},
		"c": &indexLookup{&testutil.MergeableIndexLookup{Id: "c"}, nil},
		"d": &indexLookup{&testutil.MergeableIndexLookup{Id: "d"}, nil},
	}

	require.Equal(
		map[string]*indexLookup{
			"a": &indexLookup{&testutil.MergeableIndexLookup{Id: "a"}, nil},
			"b": &indexLookup{
				&testutil.MergeableIndexLookup{
					Id:            "b",
					Intersections: []string{"b2"},
				},
				[]sql.Index{idx1, idx2},
			},
			"c": &indexLookup{new(DummyIndexLookup), nil},
			"d": &indexLookup{&testutil.MergeableIndexLookup{Id: "d"}, nil},
		},
		indexesIntersection(NewDefault(sql.NewCatalog()), left, right),
	)
}

func TestCanMergeIndexes(t *testing.T) {
	require := require.New(t)

	require.False(canMergeIndexes(new(testutil.MergeableIndexLookup), new(DummyIndexLookup)))
	require.True(canMergeIndexes(new(testutil.MergeableIndexLookup), new(testutil.MergeableIndexLookup)))
}