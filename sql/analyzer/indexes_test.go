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

package analyzer

/*
import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestNegateIndex(t *testing.T) {
	t.Skip("Updated how index lookups work, will switch these over to the new format before merge")
	require := require.New(t)

	provider := sql.NewDatabaseProvider()
	idxReg := sql.NewIndexRegistry()
	idx1 := &memory.Index{
		TableName: "t1",
		Exprs: []sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "t1", "foo", false),
		},
	}
	done, ready, err := idxReg.AddIndex(idx1)
	require.NoError(err)
	close(done)
	<-ready

	a := NewDefault(provider)

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
			plan.NewResolvedTable(t1, nil, nil),
		),
	)

	sess := sql.NewBaseSession()
	sess.SetIndexRegistry(idxReg)
	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	result, err := getIndexesByTable(ctx, a, node, nil)
	require.NoError(err)

	lookupIdxs, ok := result["t1"]
	require.True(ok)

	negate, ok := lookupIdxs.lookup.(*memory.NegateIndexLookup)
	require.True(ok)
	require.Equal("not 1", negate.ID())
}

func TestAssignIndexes(t *testing.T) {
	t.Skip("Updated how index lookups work, will switch these over to the new format before merge")
	require := require.New(t)

	provider := sql.NewDatabaseProvider()
	idxReg := sql.NewIndexRegistry()
	idx1 := &memory.Index{
		TableName: "t2",
		Exprs: []sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "t2", "bar", false),
		},
	}
	done, ready, err := idxReg.AddIndex(idx1)
	require.NoError(err)
	close(done)
	<-ready

	idx2 := &memory.Index{
		TableName: "t1",
		Exprs: []sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "t1", "foo", false),
		},
	}
	done, ready, err = idxReg.AddIndex(idx2)

	require.NoError(err)
	close(done)
	<-ready

	idx3 := &memory.UnmergeableIndex{
		memory.Index{
			TableName: "t1",
			Exprs: []sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
			},
		},
	}

	done, ready, err = idxReg.AddIndex(idx3)
	require.NoError(err)
	close(done)
	<-ready

	a := NewDefault(provider)

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
				plan.NewResolvedTable(t1, nil, nil),
				plan.NewResolvedTable(t2, nil, nil),
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "t1", "foo", false),
					expression.NewGetFieldWithTable(0, sql.Int64, "t2", "baz", false),
				),
			),
		),
	)

	sess := sql.NewBaseSession()
	sess.SetIndexRegistry(idxReg)
	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	result, err := getIndexesByTable(ctx, a, node, nil)
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
			plan.NewResolvedTable(t1, nil, nil),
		),
	)

	result, err = getIndexesByTable(ctx, a, node, nil)
	require.NoError(err)

	_, ok = result["t1"]
	require.False(ok)

	node = plan.NewProject(
		[]sql.Expression{},
		plan.NewFilter(
			expression.NewInTuple(
				expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
				expression.NewTuple(expression.NewLiteral(int64(1), sql.Int64), expression.NewLiteral(int64(2), sql.Int64)),
			),
			plan.NewResolvedTable(t1, nil, nil),
		),
	)

	result, err = getIndexesByTable(ctx, a, node, nil)
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
		Index:         mergeableIndex(table, column, colIdx),
	}
}

func unionLookupWithKeys(table string, column string, colIdx int, keys ...interface{}) *memory.MergedIndexLookup {
	var lookups []sql.IndexLookup
	for _, key := range keys {
		lookups = append(lookups, mergeableIndexLookup(table, column, colIdx, key))
	}
	return &memory.MergedIndexLookup{
		Unions: lookups,
		Index:  mergeableIndex(table, column, colIdx),
	}
}

func unionLookup(table string, column string, colIdx int, lookups ...sql.IndexLookup) *memory.MergedIndexLookup {
	return &memory.MergedIndexLookup{
		Unions: lookups,
		Index:  mergeableIndex(table, column, colIdx),
	}
}

func intersectionLookup(table string, column string, colIdx int, lookups ...sql.IndexLookup) *memory.MergedIndexLookup {
	return &memory.MergedIndexLookup{
		Intersections: lookups,
		Index:         mergeableIndex(table, column, colIdx),
	}
}

func mergeableIndexLookup(table string, column string, colIdx int, key ...interface{}) *memory.MergeableIndexLookup {
	return &memory.MergeableIndexLookup{
		Key:   key,
		Index: mergeableIndex(table, column, colIdx),
	}
}

func mergeableIndex(table string, column string, colIdx int) *memory.Index {
	return &memory.Index{
		TableName: table,
		Exprs:     []sql.Expression{col(colIdx, table, column)},
	}
}

func TestGetIndexes(t *testing.T) {
	t.Skip("Updated how index lookups work, will switch these over to the new format before merge")
	indexes := []sql.DriverIndex{
		&memory.Index{
			TableName: "t1",
			Exprs: []sql.Expression{
				col(0, "t1", "bar"),
			},
		},
		&memory.Index{
			TableName: "t2",
			Exprs: []sql.Expression{
				col(0, "t2", "bar"),
			},
		},
		&memory.Index{
			TableName: "t2",
			Exprs: []sql.Expression{
				col(0, "t2", "foo"),
				col(0, "t2", "bar"),
			},
		},
		&memory.UnmergeableIndex{
			memory.Index{
				TableName: "t3",
				Exprs: []sql.Expression{
					col(0, "t3", "foo"),
				},
			},
		},
	}

	t1bar := mergeableIndex("t1", "bar", 0)

	testCases := []struct {
		expr     sql.Expression
		expected indexLookupsByTable
		ok       bool
	}{
		{
			expr: null(
				col(0, "t2", "bar"),
			),
			expected: indexLookupsByTable{
				"t2": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t2", "bar"),
					},
					lookup:  mergeableIndexLookup("t2", "bar", 0, nil),
					indexes: []sql.Index{indexes[1]},
				},
			},
			ok: true,
		},
		{
			expr: and(
				null(
					col(0, "t2", "bar"),
				),
				null(
					col(0, "t2", "foo"),
				),
			),
			expected: indexLookupsByTable{
				"t2": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t2", "foo"),
						col(0, "t2", "bar"),
					},
					lookup: &memory.MergeableIndexLookup{
						Key: []interface{}{nil, nil},
						Index: &memory.Index{
							TableName: "t2",
							Exprs: []sql.Expression{
								col(0, "t2", "foo"),
								col(0, "t2", "bar"),
							},
						},
					},
					indexes: []sql.Index{indexes[2]},
				},
			},
			ok: true,
		},
		{
			expr: not(
				null(
					col(0, "t2", "bar"),
				),
			),
			expected: indexLookupsByTable{
				"t2": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t2", "bar"),
					},
					lookup: &memory.NegateIndexLookup{
						Lookup: mergeableIndexLookup("t2", "bar", 0, nil),
						Index:  mergeableIndex("t2", "bar", 0),
					},
					indexes: []sql.Index{indexes[1]},
				},
			},
			ok: true,
		},
		{
			eq(
				col(0, "t1", "bar"),
				col(1, "t1", "baz"),
			),
			indexLookupsByTable{},
			true,
		},
		{
			expr: eq(
				col(0, "t1", "bar"),
				lit(1),
			),
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup:  mergeableIndexLookup("t1", "bar", 0, int64(1)),
					indexes: []sql.Index{indexes[0]},
				},
			},
			ok: true,
		},
		{
			expr: or(
				eq(
					col(0, "t1", "bar"),
					lit(1),
				),
				eq(
					col(0, "t1", "bar"),
					lit(2),
				),
			),
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup: &memory.MergedIndexLookup{
						Unions: []sql.IndexLookup{
							mergeableIndexLookup("t1", "bar", 0, int64(1)),
							mergeableIndexLookup("t1", "bar", 0, int64(2)),
						},
						Index: mergeableIndex("t1", "bar", 0),
					},
					indexes: []sql.Index{
						indexes[0],
						indexes[0],
					},
				},
			},
			ok: true,
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
			expr: in(
				col(0, "t1", "bar"),
				tuple(lit(1), lit(2)),
			),
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						in(
							col(0, "t1", "bar"),
							tuple(lit(1), lit(2)),
						),
					},
					lookup: unionLookupWithKeys("t1", "bar", 0, int64(1), int64(2)),
					indexes: []sql.Index{
						indexes[0],
					},
				},
			},
			ok: true,
		},
		{
			expr: and(
				eq(
					col(0, "t1", "bar"),
					lit(1),
				),
				eq(
					col(0, "t1", "bar"),
					lit(10),
				),
			),
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup: intersectionLookupWithKeys("t1", "bar", 0, int64(1), int64(10)),
					indexes: []sql.Index{
						indexes[0],
						indexes[0],
					},
				},
			},
			ok: true,
		},
		{
			expr: and(
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
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup: intersectionLookup("t1", "bar", 0,
						unionLookup("t1", "bar", 0,
							mergeableIndexLookup("t1", "bar", 0, int64(1)),
							mergeableIndexLookup("t1", "bar", 0, int64(2)),
						),
						unionLookup("t1", "bar", 0,
							mergeableIndexLookup("t1", "bar", 0, int64(3)),
							mergeableIndexLookup("t1", "bar", 0, int64(4)),
						),
					),
					indexes: []sql.Index{
						indexes[0],
						indexes[0],
						indexes[0],
						indexes[0],
					},
				},
			},
			ok: true,
		},
		{
			expr: or(
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
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup: unionLookupWithKeys("t1", "bar", 0, int64(1), int64(2), int64(3), int64(4)),
					indexes: []sql.Index{
						indexes[0],
						indexes[0],
						indexes[0],
						indexes[0],
					},
				},
			},
			ok: true,
		},
		{
			expr: in(
				col(0, "t1", "bar"),
				tuple(lit(1), lit(2), lit(3), lit(4)),
			),
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						in(
							col(0, "t1", "bar"),
							tuple(lit(1), lit(2), lit(3), lit(4)),
						),
					},
					lookup:  unionLookupWithKeys("t1", "bar", 0, int64(1), int64(2), int64(3), int64(4)),
					indexes: []sql.Index{indexes[0]},
				},
			},
			ok: true,
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
			expr: and(
				eq(
					col(0, "t1", "bar"),
					lit(3),
				),
				eq(
					col(0, "t2", "bar"),
					lit(4),
				),
			),
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup:  mergeableIndexLookup("t1", "bar", 0, int64(3)),
					indexes: []sql.Index{indexes[0]},
				},
				"t2": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t2", "bar"),
					},
					lookup:  mergeableIndexLookup("t2", "bar", 0, int64(4)),
					indexes: []sql.Index{indexes[1]},
				},
			},
			ok: true,
		},
		{
			expr: and(
				eq(
					col(0, "t2", "bar"),
					lit(2),
				),
				eq(
					col(0, "t2", "foo"),
					lit(1),
				),
			),
			expected: indexLookupsByTable{
				"t2": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t2", "foo"),
						col(0, "t2", "bar"),
					},
					lookup: &memory.MergeableIndexLookup{
						Key: []interface{}{int64(1), int64(2)},
						Index: &memory.Index{
							TableName: "t2",
							Exprs: []sql.Expression{
								col(0, "t2", "foo"),
								col(0, "t2", "bar"),
							},
						},
					},
					indexes: []sql.Index{indexes[2]},
				},
			},
			ok: true,
		},
		{
			expr: and(
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
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup:  mergeableIndexLookup("t1", "bar", 0, int64(3)),
					indexes: []sql.Index{indexes[0]},
				},
				"t2": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t2", "foo"),
						col(0, "t2", "bar"),
					},
					lookup: &memory.MergeableIndexLookup{
						Key: []interface{}{int64(1), int64(2)},
						Index: &memory.Index{
							TableName: "t2",
							Exprs: []sql.Expression{
								col(0, "t2", "foo"),
								col(0, "t2", "bar"),
							},
						},
					},
					indexes: []sql.Index{indexes[2]},
				},
			},
			ok: true,
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
			expr: gt(
				col(0, "t1", "bar"),
				lit(1),
			),
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup: &memory.DescendIndexLookup{
						Gt:    []interface{}{int64(1)},
						Index: mergeableIndex("t1", "bar", 0),
					},
					indexes: []sql.Index{indexes[0]},
				},
			},
			ok: true,
		},
		{
			expr: lt(
				col(0, "t1", "bar"),
				lit(1),
			),
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup: &memory.AscendIndexLookup{
						Lt:    []interface{}{int64(1)},
						Index: mergeableIndex("t1", "bar", 0),
					},
					indexes: []sql.Index{indexes[0]},
				},
			},
			ok: true,
		},
		{
			expr: gte(
				col(0, "t1", "bar"),
				lit(1),
			),
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup: &memory.AscendIndexLookup{
						Gte:   []interface{}{int64(1)},
						Index: mergeableIndex("t1", "bar", 0),
					},
					indexes: []sql.Index{indexes[0]},
				},
			},
			ok: true,
		},
		{
			expr: lte(
				col(0, "t1", "bar"),
				lit(1),
			),
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup: &memory.DescendIndexLookup{
						Lte:   []interface{}{int64(1)},
						Index: mergeableIndex("t1", "bar", 0),
					},
					indexes: []sql.Index{indexes[0]},
				},
			},
			ok: true,
		},
		{
			expr: expression.NewBetween(
				col(0, "t1", "bar"),
				lit(1),
				lit(5),
			),
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup: &memory.IndexLookup{
						Expr: expression.NewAnd(
							expression.NewNullSafeGreaterThanOrEqual(t1bar.ColumnExpressions()[0], expression.NewLiteral(int64(1), sql.Int64)),
							expression.NewNullSafeLessThanOrEqual(t1bar.ColumnExpressions()[0], expression.NewLiteral(int64(5), sql.Int64)),
						),
						//Gte:   []interface{}{int64(1)},
						//Lte:   []interface{}{int64(5)},
						Index: mergeableIndex("t1", "bar", 0),
					},
					indexes: []sql.Index{indexes[0]},
				},
			},
			ok: true,
		},
		{
			expr: not(
				eq(
					col(0, "t1", "bar"),
					lit(1),
				),
			),
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup: &memory.NegateIndexLookup{
						Lookup: mergeableIndexLookup("t1", "bar", 0, int64(1)),
						Index:  mergeableIndex("t1", "bar", 0),
					},
					indexes: []sql.Index{indexes[0]},
				},
			},
			ok: true,
		},
		{

			expr: not(
				gt(
					col(0, "t1", "bar"),
					lit(10),
				),
			),
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup: &memory.DescendIndexLookup{
						Lte:   []interface{}{int64(10)},
						Index: mergeableIndex("t1", "bar", 0),
					},
					indexes: []sql.Index{indexes[0]},
				},
			},
			ok: true,
		},
		{

			expr: not(
				gte(
					col(0, "t1", "bar"),
					lit(10),
				),
			),
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup: &memory.AscendIndexLookup{
						Lt:    []interface{}{int64(10)},
						Index: mergeableIndex("t1", "bar", 0),
					},
					indexes: []sql.Index{indexes[0]},
				},
			},
			ok: true,
		},
		{

			expr: not(
				lte(
					col(0, "t1", "bar"),
					lit(10),
				),
			),
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup: &memory.DescendIndexLookup{
						Gt:    []interface{}{int64(10)},
						Index: mergeableIndex("t1", "bar", 0),
					},
					indexes: []sql.Index{indexes[0]},
				},
			},
			ok: true,
		},
		{

			expr: not(
				lt(
					col(0, "t1", "bar"),
					lit(10),
				),
			),
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup: &memory.AscendIndexLookup{
						Gte:   []interface{}{int64(10)},
						Index: mergeableIndex("t1", "bar", 0),
					},
					indexes: []sql.Index{indexes[0]},
				},
			},
			ok: true,
		},
		{
			expr: not(
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
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup: unionLookup("t1", "bar", 0,
						&memory.NegateIndexLookup{
							Lookup: mergeableIndexLookup("t1", "bar", 0, int64(10)),
							Index:  mergeableIndex("t1", "bar", 0),
						},
						&memory.NegateIndexLookup{
							Lookup: mergeableIndexLookup("t1", "bar", 0, int64(11)),
							Index:  mergeableIndex("t1", "bar", 0),
						},
					),
					indexes: []sql.Index{
						indexes[0],
						indexes[0],
					},
				},
			},
			ok: true,
		},
		{
			expr: not(
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
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup: intersectionLookup("t1", "bar", 0,
						&memory.NegateIndexLookup{
							Lookup: mergeableIndexLookup("t1", "bar", 0, int64(10)),
							Index:  mergeableIndex("t1", "bar", 0),
						},
						&memory.NegateIndexLookup{
							Lookup: mergeableIndexLookup("t1", "bar", 0, int64(11)),
							Index:  mergeableIndex("t1", "bar", 0),
						},
					),
					indexes: []sql.Index{
						indexes[0],
						indexes[0],
					},
				},
			},
			ok: true,
		},
		{
			expr: not(
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
			expected: indexLookupsByTable{
				"t2": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t2", "foo"),
					},
					lookup: &memory.MergedIndexLookup{
						Unions: nil,
						Intersections: []sql.IndexLookup{
							&memory.NegateIndexLookup{
								Lookup: mergeableIndexLookup("t2", "foo", 0, int64(100)),
								Index:  mergeableIndex("t2", "foo", 0),
							},
							&memory.NegateIndexLookup{
								Lookup: mergeableIndexLookup("t2", "bar", 0, int64(110)),
								Index:  mergeableIndex("t2", "bar", 0),
							},
						},
						Index: mergeableIndex("t2", "foo", 0),
					},
					indexes: []sql.Index{
						indexes[2],
						indexes[1],
					},
				},
			},
			ok: true,
		},
		{
			expr: expression.NewNotInTuple(
				col(0, "t1", "bar"),
				expression.NewTuple(
					lit(1),
					lit(2),
					lit(3),
					lit(4),
				),
			),
			expected: indexLookupsByTable{
				"t1": &indexLookup{
					exprs: []sql.Expression{
						col(0, "t1", "bar"),
					},
					lookup: intersectionLookup("t1", "bar", 0,
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
					indexes: []sql.Index{indexes[0]},
				},
			},
			ok: true,
		},
	}

	provider := sql.NewDatabaseProvider()
	idxReg := sql.NewIndexRegistry()
	for _, idx := range indexes {
		done, ready, err := idxReg.AddIndex(idx)
		require.NoError(t, err)
		close(done)
		<-ready
	}

	a := NewDefault(provider)

	var i int
	for _, tt := range testCases {
		t.Run(tt.expr.String(), func(t *testing.T) {
			require := require.New(t)

			sess := sql.NewBaseSession()
			sess.SetIndexRegistry(idxReg)
			ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
			ia, err := getIndexesForNode(ctx, a, nil)
			require.NoError(err)
			testExpr := convertIsNullForIndexes(ctx, tt.expr)

			result, err := getIndexes(ctx, a, ia, testExpr, nil)
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
	t.Skip("Updated how index lookups work, will switch these over to the new format before merge")
	require := require.New(t)

	provider := sql.NewDatabaseProvider()
	idxReg := sql.NewIndexRegistry()
	indexes := []*memory.Index{
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
			Exprs:     []sql.Expression{col(0, "t3", "foo")},
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
		done, ready, err := idxReg.AddIndex(idx)
		require.NoError(err)
		close(done)
		<-ready
	}

	a := NewDefault(provider)

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

	sess := sql.NewBaseSession()
	sess.SetIndexRegistry(idxReg)
	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	ia, err := getIndexesForNode(ctx, a, nil)
	require.NoError(err)

	result, err := getMultiColumnIndexes(ctx, exprs, a, ia, nil)
	require.NoError(err)

	expected := indexLookupsByTable{
		"t1": &indexLookup{
			exprs: []sql.Expression{
				col(2, "t1", "foo"),
				col(2, "t1", "bar"),
			},
			lookup: &memory.MergeableIndexLookup{
				Key:   []interface{}{int64(5), int64(6)},
				Index: indexes[0],
			},
			indexes: []sql.Index{indexes[0]},
		},
		"t2": &indexLookup{
			exprs: []sql.Expression{
				col(2, "t2", "foo"),
				col(2, "t2", "bar"),
				col(2, "t2", "baz"),
			},
			lookup: &memory.MergeableIndexLookup{
				Key:   []interface{}{int64(1), int64(2), int64(3)},
				Index: indexes[1],
			},
			indexes: []sql.Index{indexes[1]},
		},
		"t4": &indexLookup{
			exprs: []sql.Expression{
				col(2, "t4", "foo"),
				col(2, "t4", "bar"),
			},
			lookup: &memory.IndexLookup{
				Expr: expression.NewAnd(
					expression.NewAnd(
						expression.NewNullSafeGreaterThanOrEqual(indexes[4].ColumnExpressions()[0], expression.NewLiteral(int64(1), sql.Int64)),
						expression.NewNullSafeLessThanOrEqual(indexes[4].ColumnExpressions()[0], expression.NewLiteral(int64(6), sql.Int64)),
					),
					expression.NewAnd(
						expression.NewNullSafeGreaterThanOrEqual(indexes[4].ColumnExpressions()[1], expression.NewLiteral(int64(2), sql.Int64)),
						expression.NewNullSafeLessThanOrEqual(indexes[4].ColumnExpressions()[1], expression.NewLiteral(int64(5), sql.Int64)),
					),
				),
				//Gte:   []interface{}{int64(1), int64(2)},
				//Lte:   []interface{}{int64(6), int64(5)},
				Index: indexes[4],
			},
			indexes: []sql.Index{indexes[4]},
		},
	}

	require.Equal(expected, result)
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
		plan.NewResolvedTable(memory.NewTable("foo", sql.Schema{
			{Source: "foo"},
			{Source: "foo"},
			{Source: "bar"},
			{Source: "baz"},
		}), nil, nil),
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

func (l DummyIndexLookup) String() string {
	return "DummyIndexLookup"
}

var _ sql.IndexLookup = DummyIndexLookup{}

func (DummyIndexLookup) Indexes() []string { return nil }

func (DummyIndexLookup) Values(sql.Partition) (sql.IndexValueIter, error) {
	return nil, nil
}

func TestIndexesIntersection(t *testing.T) {
	require := require.New(t)

	idx1, idx2 := &memory.Index{TableName: "bar"}, &memory.Index{TableName: "foo"}

	left := indexLookupsByTable{
		"a": &indexLookup{lookup: &memory.MergeableIndexLookup{Key: []interface{}{"a"}}},
		"b": &indexLookup{lookup: &memory.MergeableIndexLookup{Key: []interface{}{"b"}}, indexes: []sql.Index{idx1}},
		"c": &indexLookup{lookup: new(DummyIndexLookup)},
	}

	right := indexLookupsByTable{
		"b": &indexLookup{lookup: &memory.MergeableIndexLookup{Key: []interface{}{"b2"}}, indexes: []sql.Index{idx2}},
		"c": &indexLookup{lookup: &memory.MergeableIndexLookup{Key: []interface{}{"c"}}},
		"d": &indexLookup{lookup: &memory.MergeableIndexLookup{Key: []interface{}{"d"}}},
	}

	lookupsByTable, err := indexesIntersection(left, right)
	require.NoError(err)
	require.Equal(
		indexLookupsByTable{
			"a": &indexLookup{lookup: &memory.MergeableIndexLookup{Key: []interface{}{"a"}}},
			"b": &indexLookup{
				lookup: &memory.MergedIndexLookup{
					Intersections: []sql.IndexLookup{
						&memory.MergeableIndexLookup{
							Key: []interface{}{"b"},
						},
						&memory.MergeableIndexLookup{
							Key: []interface{}{"b2"},
						},
					},
				},
				indexes: []sql.Index{idx1, idx2},
			},
			"c": &indexLookup{lookup: new(DummyIndexLookup)},
			"d": &indexLookup{lookup: &memory.MergeableIndexLookup{Key: []interface{}{"d"}}},
		},
		lookupsByTable,
	)
}

func TestCanMergeIndexes(t *testing.T) {
	require := require.New(t)

	require.False(canMergeIndexes(new(memory.MergeableIndexLookup), new(DummyIndexLookup)))
	require.True(canMergeIndexes(new(memory.MergeableIndexLookup), new(memory.MergeableIndexLookup)))
}*/
