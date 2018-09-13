package analyzer

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestNegateIndex(t *testing.T) {
	require := require.New(t)

	catalog := sql.NewCatalog()
	idx1 := &dummyIndex{
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

	t1 := mem.NewTable("t1", sql.Schema{
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

	negate, ok := lookupIdxs.lookup.(*negateIndexLookup)
	require.True(ok)
	require.True(negate.value == "1")
}

func TestAssignIndexes(t *testing.T) {
	require := require.New(t)

	catalog := sql.NewCatalog()
	idx1 := &dummyIndex{
		"t2",
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "t2", "bar", false),
		},
	}
	done, ready, err := catalog.AddIndex(idx1)
	require.NoError(err)
	close(done)
	<-ready

	idx2 := &dummyIndex{
		"t1",
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "t1", "foo", false),
		},
	}
	done, ready, err = catalog.AddIndex(idx2)

	require.NoError(err)
	close(done)
	<-ready

	a := NewDefault(catalog)

	t1 := mem.NewTable("t1", sql.Schema{
		{Name: "foo", Type: sql.Int64, Source: "t1"},
	})

	t2 := mem.NewTable("t2", sql.Schema{
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

	mergeable, ok := lookupIdxs.lookup.(*mergeableIndexLookup)
	require.True(ok)
	require.True(mergeable.id == "2")

	lookupIdxs, ok = result["t2"]
	require.True(ok)

	mergeable, ok = lookupIdxs.lookup.(*mergeableIndexLookup)
	require.True(ok)
	require.True(mergeable.id == "1")
}

func TestGetIndexes(t *testing.T) {
	indexes := []*dummyIndex{
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
					&mergeableIndexLookup{id: "1"},
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
					&mergeableIndexLookup{id: "1", unions: []string{"2"}},
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
					&mergeableIndexLookup{id: "1", intersections: []string{"2"}},
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
					&mergeableIndexLookup{id: "1", unions: []string{"2", "4"}, intersections: []string{"3"}},
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
					&mergeableIndexLookup{id: "1", unions: []string{"2", "3", "4"}},
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
					&mergeableIndexLookup{id: "1", unions: []string{"2", "3", "4"}},
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
					&mergeableIndexLookup{id: "3"},
					[]sql.Index{indexes[0]},
				},
				"t2": &indexLookup{
					&mergeableIndexLookup{id: "1, 2"},
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
					&mergeableIndexLookup{id: "3"},
					[]sql.Index{indexes[0]},
				},
				"t2": &indexLookup{
					&mergeableIndexLookup{id: "5", unions: []string{"1, 2"}},
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
					&descendIndexLookup{gt: []interface{}{int64(1)}},
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
					&ascendIndexLookup{lt: []interface{}{int64(1)}},
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
					&ascendIndexLookup{gte: []interface{}{int64(1)}},
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
					&descendIndexLookup{lte: []interface{}{int64(1)}},
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
					&mergedIndexLookup{
						[]sql.IndexLookup{
							&ascendIndexLookup{
								gte: []interface{}{int64(1)},
								lt:  []interface{}{int64(5)},
							},
							&descendIndexLookup{
								gt:  []interface{}{int64(1)},
								lte: []interface{}{int64(5)},
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
					&negateIndexLookup{
						value: "1",
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
					&descendIndexLookup{lte: []interface{}{int64(10)}},
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
					&ascendIndexLookup{lt: []interface{}{int64(10)}},
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
					&descendIndexLookup{gt: []interface{}{int64(10)}},
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
					&ascendIndexLookup{gte: []interface{}{int64(10)}},
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
					&mergedIndexLookup{
						children: []sql.IndexLookup{
							&negateIndexLookup{
								value: "10",
							},
							&negateIndexLookup{
								value: "11",
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
					&mergeableIndexLookup{
						id:            "not 10",
						intersections: []string{"not 11"},
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
					&negateIndexLookup{
						value: "110",
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
					&mergeableIndexLookup{
						id:            "not 1",
						intersections: []string{"not 2", "not 3", "not 4"},
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

			result, err := getIndexes(tt.expr, a)
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
	indexes := []*dummyIndex{
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
	result, err := getMultiColumnIndexes(exprs, a, used)
	require.NoError(err)

	expected := map[string]*indexLookup{
		"t1": &indexLookup{
			&mergeableIndexLookup{id: "5, 6"},
			[]sql.Index{indexes[0]},
		},
		"t2": &indexLookup{
			&mergeableIndexLookup{id: "1, 2, 3"},
			[]sql.Index{indexes[1]},
		},
		"t4": &indexLookup{
			&mergedIndexLookup{[]sql.IndexLookup{
				&ascendIndexLookup{
					gte: []interface{}{int64(1), int64(2)},
					lt:  []interface{}{int64(6), int64(5)},
				},
				&descendIndexLookup{
					gt:  []interface{}{int64(1), int64(2)},
					lte: []interface{}{int64(6), int64(5)},
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
			mem.NewTable("foo", sql.Schema{
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

type dummyIndexLookup struct{}

func (dummyIndexLookup) Indexes() []string { return nil }

func (dummyIndexLookup) Values(sql.Partition) (sql.IndexValueIter, error) {
	return nil, nil
}

type dummyIndex struct {
	table string
	expr  []sql.Expression
}

var _ sql.Index = (*dummyIndex)(nil)
var _ sql.AscendIndex = (*dummyIndex)(nil)
var _ sql.DescendIndex = (*dummyIndex)(nil)
var _ sql.NegateIndex = (*dummyIndex)(nil)

func (dummyIndex) Database() string { return "" }
func (dummyIndex) Driver() string   { return "" }
func (i dummyIndex) Expressions() []string {
	var exprs []string
	for _, e := range i.expr {
		exprs = append(exprs, e.String())
	}
	return exprs
}

func (i dummyIndex) AscendGreaterOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	return &ascendIndexLookup{gte: keys}, nil
}

func (i dummyIndex) AscendLessThan(keys ...interface{}) (sql.IndexLookup, error) {
	return &ascendIndexLookup{lt: keys}, nil
}

func (i dummyIndex) AscendRange(greaterOrEqual, lessThan []interface{}) (sql.IndexLookup, error) {
	return &ascendIndexLookup{gte: greaterOrEqual, lt: lessThan}, nil
}

func (i dummyIndex) DescendGreater(keys ...interface{}) (sql.IndexLookup, error) {
	return &descendIndexLookup{gt: keys}, nil
}

func (i dummyIndex) DescendLessOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	return &descendIndexLookup{lte: keys}, nil
}

func (i dummyIndex) DescendRange(lessOrEqual, greaterThan []interface{}) (sql.IndexLookup, error) {
	return &descendIndexLookup{gt: greaterThan, lte: lessOrEqual}, nil
}

func (i dummyIndex) Not(keys ...interface{}) (sql.IndexLookup, error) {
	lookup, err := i.Get(keys...)
	if err != nil {
		return nil, err
	}

	mergeable, _ := lookup.(*mergeableIndexLookup)
	return &negateIndexLookup{value: mergeable.id}, nil
}

func (i dummyIndex) Get(key ...interface{}) (sql.IndexLookup, error) {
	if len(key) != 1 {
		var parts = make([]string, len(key))
		for i, p := range key {
			parts[i] = fmt.Sprint(p)
		}

		return &mergeableIndexLookup{id: strings.Join(parts, ", ")}, nil
	}

	return &mergeableIndexLookup{id: fmt.Sprint(key[0])}, nil
}
func (i dummyIndex) Has(sql.Partition, ...interface{}) (bool, error) {
	panic("not implemented")
}
func (i dummyIndex) ID() string {
	if len(i.expr) == 1 {
		return i.expr[0].String()
	}
	var parts = make([]string, len(i.expr))
	for i, e := range i.expr {
		parts[i] = e.String()
	}

	return "(" + strings.Join(parts, ", ") + ")"
}
func (i dummyIndex) Table() string { return i.table }

type mergedIndexLookup struct {
	children []sql.IndexLookup
}

func (mergedIndexLookup) Values(sql.Partition) (sql.IndexValueIter, error) {
	panic("mergedIndexLookup.Values is a placeholder")
}

func (i *mergedIndexLookup) Indexes() []string {
	var indexes []string
	for _, c := range i.children {
		indexes = append(indexes, c.Indexes()...)
	}
	return indexes
}

func (i *mergedIndexLookup) IsMergeable(sql.IndexLookup) bool {
	return true
}

func (i *mergedIndexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	return &mergedIndexLookup{append(i.children, lookups...)}
}

func (mergedIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("mergedIndexLookup.Difference is not implemented")
}

func (mergedIndexLookup) Intersection(...sql.IndexLookup) sql.IndexLookup {
	panic("mergedIndexLookup.Intersection is not implemented")
}

type negateIndexLookup struct {
	value         string
	intersections []string
	unions        []string
}

func (l *negateIndexLookup) ID() string              { return "not " + l.value }
func (l *negateIndexLookup) Unions() []string        { return l.unions }
func (l *negateIndexLookup) Intersections() []string { return l.intersections }

func (*negateIndexLookup) Values(sql.Partition) (sql.IndexValueIter, error) {
	panic("negateIndexLookup.Values is a placeholder")
}

func (l *negateIndexLookup) Indexes() []string {
	return []string{l.ID()}
}

func (*negateIndexLookup) IsMergeable(sql.IndexLookup) bool {
	return true
}

func (l *negateIndexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	return &mergedIndexLookup{append([]sql.IndexLookup{l}, lookups...)}
}

func (*negateIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("negateIndexLookup.Difference is not implemented")
}

func (l *negateIndexLookup) Intersection(indexes ...sql.IndexLookup) sql.IndexLookup {
	var intersections, unions []string
	for _, idx := range indexes {
		intersections = append(intersections, idx.(mergeableLookup).ID())
		intersections = append(intersections, idx.(mergeableLookup).Intersections()...)
		unions = append(unions, idx.(mergeableLookup).Unions()...)
	}
	return &mergeableIndexLookup{
		l.ID(),
		append(l.unions, unions...),
		append(l.intersections, intersections...),
	}
}

type ascendIndexLookup struct {
	id  string
	gte []interface{}
	lt  []interface{}
}

func (ascendIndexLookup) Values(sql.Partition) (sql.IndexValueIter, error) {
	panic("ascendIndexLookup.Values is a placeholder")
}

func (l *ascendIndexLookup) Indexes() []string {
	return []string{l.id}
}

func (l *ascendIndexLookup) IsMergeable(sql.IndexLookup) bool {
	return true
}

func (l *ascendIndexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	return &mergedIndexLookup{append([]sql.IndexLookup{l}, lookups...)}
}

func (ascendIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("ascendIndexLookup.Difference is not implemented")
}

func (ascendIndexLookup) Intersection(...sql.IndexLookup) sql.IndexLookup {
	panic("ascendIndexLookup.Intersection is not implemented")
}

type descendIndexLookup struct {
	id  string
	gt  []interface{}
	lte []interface{}
}

func (descendIndexLookup) Values(sql.Partition) (sql.IndexValueIter, error) {
	panic("descendIndexLookup.Values is a placeholder")
}

func (l *descendIndexLookup) Indexes() []string {
	return []string{l.id}
}

func (l *descendIndexLookup) IsMergeable(sql.IndexLookup) bool {
	return true
}

func (l *descendIndexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	return &mergedIndexLookup{append([]sql.IndexLookup{l}, lookups...)}
}

func (descendIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("descendIndexLookup.Difference is not implemented")
}

func (descendIndexLookup) Intersection(...sql.IndexLookup) sql.IndexLookup {
	panic("descendIndexLookup.Intersection is not implemented")
}

func TestIndexesIntersection(t *testing.T) {
	require := require.New(t)

	idx1, idx2 := &dummyIndex{table: "bar"}, &dummyIndex{table: "foo"}

	left := map[string]*indexLookup{
		"a": &indexLookup{&mergeableIndexLookup{id: "a"}, nil},
		"b": &indexLookup{&mergeableIndexLookup{id: "b"}, []sql.Index{idx1}},
		"c": &indexLookup{new(dummyIndexLookup), nil},
	}

	right := map[string]*indexLookup{
		"b": &indexLookup{&mergeableIndexLookup{id: "b2"}, []sql.Index{idx2}},
		"c": &indexLookup{&mergeableIndexLookup{id: "c"}, nil},
		"d": &indexLookup{&mergeableIndexLookup{id: "d"}, nil},
	}

	require.Equal(
		map[string]*indexLookup{
			"a": &indexLookup{&mergeableIndexLookup{id: "a"}, nil},
			"b": &indexLookup{
				&mergeableIndexLookup{
					id:            "b",
					intersections: []string{"b2"},
				},
				[]sql.Index{idx1, idx2},
			},
			"c": &indexLookup{new(dummyIndexLookup), nil},
			"d": &indexLookup{&mergeableIndexLookup{id: "d"}, nil},
		},
		indexesIntersection(NewDefault(sql.NewCatalog()), left, right),
	)
}

func TestCanMergeIndexes(t *testing.T) {
	require := require.New(t)

	require.False(canMergeIndexes(new(mergeableIndexLookup), new(dummyIndexLookup)))
	require.True(canMergeIndexes(new(mergeableIndexLookup), new(mergeableIndexLookup)))
}

type mergeableLookup interface {
	ID() string
	Unions() []string
	Intersections() []string
}

type mergeableIndexLookup struct {
	id            string
	unions        []string
	intersections []string
}

var _ sql.Mergeable = (*mergeableIndexLookup)(nil)
var _ sql.SetOperations = (*mergeableIndexLookup)(nil)

func (i *mergeableIndexLookup) ID() string              { return i.id }
func (i *mergeableIndexLookup) Unions() []string        { return i.unions }
func (i *mergeableIndexLookup) Intersections() []string { return i.intersections }

func (i *mergeableIndexLookup) IsMergeable(lookup sql.IndexLookup) bool {
	_, ok := lookup.(mergeableLookup)
	return ok
}

func (i *mergeableIndexLookup) Values(sql.Partition) (sql.IndexValueIter, error) {
	panic("not implemented")
}

func (i *mergeableIndexLookup) Indexes() []string {
	return []string{i.ID()}
}

func (i *mergeableIndexLookup) Difference(indexes ...sql.IndexLookup) sql.IndexLookup {
	panic("not implemented")
}

func (i *mergeableIndexLookup) Intersection(indexes ...sql.IndexLookup) sql.IndexLookup {
	var intersections, unions []string
	for _, idx := range indexes {
		intersections = append(intersections, idx.(mergeableLookup).ID())
		intersections = append(intersections, idx.(mergeableLookup).Intersections()...)
		unions = append(unions, idx.(mergeableLookup).Unions()...)
	}
	return &mergeableIndexLookup{
		i.id,
		append(i.unions, unions...),
		append(i.intersections, intersections...),
	}
}

func (i *mergeableIndexLookup) Union(indexes ...sql.IndexLookup) sql.IndexLookup {
	var intersections, unions []string
	for _, idx := range indexes {
		unions = append(unions, idx.(*mergeableIndexLookup).id)
		unions = append(unions, idx.(*mergeableIndexLookup).unions...)
		intersections = append(intersections, idx.(*mergeableIndexLookup).intersections...)
	}
	return &mergeableIndexLookup{
		i.id,
		append(i.unions, unions...),
		append(i.intersections, intersections...),
	}
}
