package analyzer

import (
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
	idx1 := &memory.MergeableIndex{
		TableName: "t1",
		Exprs: []sql.Expression{
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

	result, err := assignIndexes(sql.NewEmptyContext(), a, node)
	require.NoError(err)

	lookupIdxs, ok := result["t1"]
	require.True(ok)

	negate, ok := lookupIdxs.lookup.(*memory.NegateIndexLookup)
	require.True(ok)
	require.Equal("not 1", negate.ID())
}

func TestAssignIndexes(t *testing.T) {
	require := require.New(t)

	catalog := sql.NewCatalog()
	idx1 := &memory.MergeableIndex{
		TableName: "t2",
		Exprs: []sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "t2", "bar", false),
		},
	}
	done, ready, err := catalog.AddIndex(idx1)
	require.NoError(err)
	close(done)
	<-ready

	idx2 := &memory.MergeableIndex{
		TableName: "t1",
		Exprs: []sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "t1", "foo", false),
		},
	}
	done, ready, err = catalog.AddIndex(idx2)

	require.NoError(err)
	close(done)
	<-ready

	idx3 := &memory.UnmergeableIndex{
		TableName: "t1",
		Exprs: []sql.Expression{
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
			expression.NewAnd(
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

	result, err := assignIndexes(sql.NewEmptyContext(), a, node)
	require.NoError(err)

	lookupIdxs, ok := result["t1"]
	require.True(ok)

	mergeable, ok := lookupIdxs.lookup.(*memory.MergeableIndexLookup)
	require.True(ok)
	require.Equal("2", mergeable.ID())

	lookupIdxs, ok = result["t2"]
	require.True(ok)

	mergeable, ok = lookupIdxs.lookup.(*memory.MergeableIndexLookup)
	require.True(ok)
	require.Equal("1", mergeable.ID())

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

	_, ok = result["t1"]
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

	_, ok = result["t1"]
	require.False(ok)
}

func intersectionLookupWithKeys(table string, column string, colIdx int, keys ...interface{}) *memory.MergedIndexLookup {
	var lookups []sql.IndexLookup
	for _, key := range keys {
		lookups = append(lookups, mergeableIndexLookup(table, column, colIdx, key))
	}
	return &memory.MergedIndexLookup{
		Intersections: lookups,
		Index: mergeableIndex(table, column, colIdx),
	}
}

func unionLookupWithKeys(table string, column string, colIdx int, keys ...interface{}) *memory.MergedIndexLookup {
	var lookups []sql.IndexLookup
	for _, key := range keys {
		lookups = append(lookups, mergeableIndexLookup(table, column, colIdx, key))
	}
	return &memory.MergedIndexLookup{
		Unions: lookups,
		Index: mergeableIndex(table, column, colIdx),
	}
}

func unionLookup(table string, column string, colIdx int, lookups ...sql.IndexLookup) *memory.MergedIndexLookup {
	return &memory.MergedIndexLookup{
		Unions: lookups,
		Index: mergeableIndex(table, column, colIdx),
	}
}

func intersectionLookup(table string, column string, colIdx int, lookups ...sql.IndexLookup) *memory.MergedIndexLookup {
	return &memory.MergedIndexLookup{
		Intersections: lookups,
		Index: mergeableIndex(table, column, colIdx),
	}
}

func mergeableIndexLookup(table string, column string, colIdx int, key ...interface{}) *memory.MergeableIndexLookup {
	return &memory.MergeableIndexLookup{
		Key: key,
		Index: mergeableIndex(table, column, colIdx),
	}
}

func mergeableIndex(table string, column string, colIdx int) *memory.MergeableIndex {
	return &memory.MergeableIndex{
		TableName: table,
		Exprs:     []sql.Expression{col(colIdx, table, column)},
	}
}

func TestGetIndexes(t *testing.T) {
	indexes := []sql.Index {
		&memory.MergeableIndex{
			TableName: "t1",
			Exprs: []sql.Expression{
				col(0, "t1", "bar"),
			},
		},
		&memory.MergeableIndex{
			TableName: "t2",
			Exprs: []sql.Expression{
				col(0, "t2", "foo"),
				col(0, "t2", "bar"),
			},
		},
		&memory.MergeableIndex{
			TableName: "t2",
			Exprs: []sql.Expression{
				col(0, "t2", "bar"),
			},
		},
		&memory.UnmergeableIndex{
			TableName: "t3",
			Exprs: []sql.Expression{
				col(0, "t3", "foo"),
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
					mergeableIndexLookup("t1", "bar", 0, int64(1)),
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
					&memory.MergedIndexLookup{
						Unions: []sql.IndexLookup{
							mergeableIndexLookup("t1", "bar", 0, int64(1)),
							mergeableIndexLookup("t1", "bar", 0, int64(2)),
						},
						Index: mergeableIndex("t1", "bar", 0),
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
			or(
				eq(
					col(0, "t3", "foo"),
					lit(1),
				),
				eq(
					col(0, "t3", "foo"),
					lit(2),
				),
			),
			nil,
			true,
		},
		{
			in(
				col(0, "t3", "foo"),
				tuple(lit(1), lit(2)),
			),
			nil,
			true,
		},
		{
			in(
				col(0, "t1", "bar"),
				tuple(lit(1), lit(2)),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					unionLookupWithKeys("t1", "bar", 0, int64(1),  int64(2)),
					[]sql.Index{
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
					intersectionLookupWithKeys("t1", "bar", 0, int64(1), int64(2)),
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
					intersectionLookup("t1", "bar", 0,
						unionLookup("t1", "bar", 0,
									mergeableIndexLookup("t1", "bar", 0, int64(1)),
									mergeableIndexLookup("t1", "bar", 0, int64(2)),
						),
						unionLookup("t1", "bar", 0,
									mergeableIndexLookup("t1", "bar", 0, int64(3)),
									mergeableIndexLookup("t1", "bar", 0, int64(4)),
						),
					),
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
					unionLookupWithKeys("t1", "bar", 0, int64(1), int64(2), int64(3), int64(4)),
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
			in(
				col(0, "t1", "bar"),
				tuple(lit(1), lit(2), lit(3), lit(4)),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					unionLookupWithKeys("t1", "bar", 0, int64(1), int64(2), int64(3), int64(4)),
					[]sql.Index{indexes[0]},
				},
			},
			true,
		},
		{
			or(
				eq(
					col(0, "t1", "bar"),
					lit(3),
				),
				eq(
					col(0, "t2", "bar"),
					lit(4),
				),
			),
			nil,
			true,
		},
		{
			and(
				eq(
					col(0, "t1", "bar"),
					lit(3),
				),
				eq(
					col(0, "t2", "bar"),
					lit(4),
				),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					mergeableIndexLookup("t1", "bar", 0, int64(3)),
					[]sql.Index{indexes[0]},
				},
				"t2": &indexLookup{
					mergeableIndexLookup("t2", "bar", 0, int64(4)),
					[]sql.Index{indexes[2]},
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
					mergeableIndexLookup("t1", "bar", 0, int64(3)),
					[]sql.Index{indexes[0]},
				},
				"t2": &indexLookup{
					&memory.MergeableIndexLookup{
						Key: []interface{}{int64(1), int64(2)},
						Index: &memory.MergeableIndex{
							TableName: "t2",
							Exprs:     []sql.Expression{
								col(0, "t2", "foo"),
								col(0, "t2", "bar"),
							},
						},
					},
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
			nil,
			true,
		},
		{
			gt(
				col(0, "t1", "bar"),
				lit(1),
			),
			map[string]*indexLookup{
				"t1": &indexLookup{
					&memory.DescendIndexLookup{
						Gt: []interface{}{int64(1)},
						Index: mergeableIndex("t1", "bar", 0),
					},
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
					&memory.AscendIndexLookup{
						Lt: []interface{}{int64(1)},
						Index: mergeableIndex("t1", "bar", 0),
					},
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
					&memory.AscendIndexLookup{
						Gte: []interface{}{int64(1)},
						Index: mergeableIndex("t1", "bar", 0),
					},
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
					&memory.DescendIndexLookup{
						Lte: []interface{}{int64(1)},
						Index: mergeableIndex("t1", "bar", 0),
					},
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
					unionLookup("t1", "bar", 0,
						&memory.AscendIndexLookup{
							Gte:   []interface{}{int64(1)},
							Lt:    []interface{}{int64(5)},
							Index: mergeableIndex("t1", "bar", 0),
						},
						&memory.DescendIndexLookup{
							Gt:    []interface{}{int64(1)},
							Lte:   []interface{}{int64(5)},
							Index: mergeableIndex("t1", "bar", 0),
						},
					),
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
					&memory.NegateIndexLookup{
						Lookup: mergeableIndexLookup("t1", "bar", 0, int64(1)),
						Index: mergeableIndex("t1", "bar", 0),
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
					&memory.DescendIndexLookup{
						Lte: []interface{}{int64(10)},
						Index: mergeableIndex("t1", "bar", 0),
					},
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
					&memory.AscendIndexLookup{
						Lt: []interface{}{int64(10)},
						Index: mergeableIndex("t1", "bar", 0),
					},
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
					&memory.DescendIndexLookup{
						Gt: []interface{}{int64(10)},
						Index: mergeableIndex("t1", "bar", 0),
					},
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
					&memory.AscendIndexLookup{
						Gte: []interface{}{int64(10)},
						Index: mergeableIndex("t1", "bar", 0),
					},
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
					unionLookup("t1", "bar", 0,
						&memory.NegateIndexLookup{
							Lookup: mergeableIndexLookup("t1", "bar", 0, int64(10)),
							Index:  mergeableIndex("t1", "bar", 0),
						},
						&memory.NegateIndexLookup{
							Lookup: mergeableIndexLookup("t1", "bar", 0, int64(11)),
							Index:  mergeableIndex("t1", "bar", 0),
						},
					),
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
					intersectionLookup("t1", "bar", 0,
						&memory.NegateIndexLookup{
							Lookup: mergeableIndexLookup("t1", "bar", 0, int64(10)),
							Index:  mergeableIndex("t1", "bar", 0),
						},
						&memory.NegateIndexLookup{
							Lookup: mergeableIndexLookup("t1", "bar", 0, int64(11)),
							Index:  mergeableIndex("t1", "bar", 0),
						},
					),
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
					&memory.NegateIndexLookup{
						Lookup: mergeableIndexLookup("t2", "bar", 0, int64(110)),
						Index: mergeableIndex("t2", "bar", 0),
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
					intersectionLookup("t1", "bar", 0,
						&memory.NegateIndexLookup{
							Lookup: mergeableIndexLookup("t1", "bar", 0, int64(1)),
							Index:  mergeableIndex("t1", "bar", 0),
						},
						&memory.NegateIndexLookup{
							Lookup: mergeableIndexLookup("t1", "bar", 0, int64(2)),
							Index:  mergeableIndex("t1", "bar", 0),
						},
						&memory.NegateIndexLookup{
							Lookup: mergeableIndexLookup("t1", "bar", 0, int64(3)),
							Index:  mergeableIndex("t1", "bar", 0),
						},
						&memory.NegateIndexLookup{
							Lookup: mergeableIndexLookup("t1", "bar", 0, int64(4)),
							Index:  mergeableIndex("t1", "bar", 0),
						},
					),
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

	var i int
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
			i++
		})
	}
}

func TestGetMultiColumnIndexes(t *testing.T) {
	require := require.New(t)

	catalog := sql.NewCatalog()
	indexes := []*memory.MergeableIndex{
		{
			TableName: "t1",
			Exprs: []sql.Expression{
				col(1, "t1", "foo"),
				col(2, "t1", "bar"),
			},
		},
		{
			TableName: "t2",
			Exprs: []sql.Expression{
				col(0, "t2", "foo"),
				col(1, "t2", "bar"),
				col(2, "t2", "baz"),
			},
		},
		{
			TableName: "t2",
			Exprs: []sql.Expression{
				col(0, "t2", "foo"),
				col(0, "t2", "bar"),
			},
		},
		{
			TableName: "t3",
			Exprs: []sql.Expression{col(0, "t3", "foo")},
		},
		{
			TableName: "t4",
			Exprs: []sql.Expression{
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
			&memory.MergeableIndexLookup{
				Key: []interface{}{int64(5), int64(6)},
				Index: indexes[0],
			},
			[]sql.Index{indexes[0]},
		},
		"t2": &indexLookup{
			&memory.MergeableIndexLookup{
				Key: []interface{}{int64(1), int64(2), int64(3)},
				Index: indexes[1],
			},
			[]sql.Index{indexes[1]},
		},
		"t4": &indexLookup{
			&memory.MergedIndexLookup{
				Unions: []sql.IndexLookup{
					&memory.AscendIndexLookup{
						Gte: []interface{}{int64(1), int64(2)},
						Lt:  []interface{}{int64(6), int64(5)},
						Index: indexes[4],
					},
					&memory.DescendIndexLookup{
						Gt:  []interface{}{int64(1), int64(2)},
						Lte: []interface{}{int64(6), int64(5)},
						Index: indexes[4],
					},
				},
				Index: indexes[4],
			},
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

func TestIndexesIntersection(t *testing.T) {
	require := require.New(t)

	idx1, idx2 := &memory.MergeableIndex{TableName: "bar"}, &memory.MergeableIndex{TableName: "foo"}

	left := map[string]*indexLookup{
		"a": &indexLookup{&memory.MergeableIndexLookup{Key: []interface{}{"a"}}, nil},
		"b": &indexLookup{&memory.MergeableIndexLookup{Key: []interface{}{"b"}}, []sql.Index{idx1}},
		"c": &indexLookup{new(DummyIndexLookup), nil},
	}

	right := map[string]*indexLookup{
		"b": &indexLookup{&memory.MergeableIndexLookup{Key: []interface{}{"b2"}}, []sql.Index{idx2}},
		"c": &indexLookup{&memory.MergeableIndexLookup{Key: []interface{}{"c"}}, nil},
		"d": &indexLookup{&memory.MergeableIndexLookup{Key: []interface{}{"d"}}, nil},
	}

	require.Equal(
		map[string]*indexLookup{
			"a": &indexLookup{&memory.MergeableIndexLookup{Key: []interface{}{"a"}}, nil},
			"b": &indexLookup{
				&memory.MergedIndexLookup {
					Intersections: []sql.IndexLookup {
						&memory.MergeableIndexLookup{
							Key:   []interface{}{"b"},
						},
						&memory.MergeableIndexLookup{
							Key:   []interface{}{"b2"},
						},
					},
				},
				[]sql.Index{idx1, idx2},
			},
			"c": &indexLookup{new(DummyIndexLookup), nil},
			"d": &indexLookup{&memory.MergeableIndexLookup{Key: []interface{}{"d"}}, nil},
		},
		indexesIntersection(NewDefault(sql.NewCatalog()), left, right),
	)
}

func TestCanMergeIndexes(t *testing.T) {
	require := require.New(t)

	require.False(canMergeIndexes(new(memory.MergeableIndexLookup), new(DummyIndexLookup)))
	require.True(canMergeIndexes(new(memory.MergeableIndexLookup), new(memory.MergeableIndexLookup)))
}