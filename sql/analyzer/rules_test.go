package analyzer

import (
	"crypto/sha1"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestResolveSubqueries(t *testing.T) {
	require := require.New(t)

	table1 := mem.NewTable("foo", sql.Schema{{Name: "a", Type: sql.Int64, Source: "foo"}})
	table2 := mem.NewTable("bar", sql.Schema{
		{Name: "b", Type: sql.Int64, Source: "bar"},
		{Name: "k", Type: sql.Int64, Source: "bar"},
	})
	table3 := mem.NewTable("baz", sql.Schema{{Name: "c", Type: sql.Int64, Source: "baz"}})
	db := mem.NewDatabase("mydb")
	db.AddTable("foo", table1)
	db.AddTable("bar", table2)
	db.AddTable("baz", table3)

	catalog := &sql.Catalog{Databases: []sql.Database{db}}
	a := New(catalog)
	a.CurrentDatabase = "mydb"

	// SELECT * FROM
	// 	(SELECT a FROM foo) t1,
	// 	(SELECT b FROM (SELECT b FROM bar) t2alias) t2,
	//  baz
	node := plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewCrossJoin(
			plan.NewCrossJoin(
				plan.NewSubqueryAlias(
					"t1",
					plan.NewProject(
						[]sql.Expression{expression.NewUnresolvedColumn("a")},
						plan.NewUnresolvedTable("foo"),
					),
				),
				plan.NewSubqueryAlias(
					"t2",
					plan.NewProject(
						[]sql.Expression{expression.NewUnresolvedColumn("b")},
						plan.NewSubqueryAlias(
							"t2alias",
							plan.NewProject(
								[]sql.Expression{expression.NewUnresolvedColumn("b")},
								plan.NewUnresolvedTable("bar"),
							),
						),
					),
				),
			),
			plan.NewUnresolvedTable("baz"),
		),
	)

	subquery := plan.NewSubqueryAlias(
		"t2alias",
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "bar", "b", false),
			},
			plan.NewPushdownProjectionAndFiltersTable([]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "bar", "b", false),
			}, nil, table2),
		),
	)
	_ = subquery.Schema()

	expected := plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewCrossJoin(
			plan.NewCrossJoin(
				plan.NewSubqueryAlias(
					"t1",
					plan.NewPushdownProjectionAndFiltersTable([]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					}, nil, table1),
				),
				plan.NewSubqueryAlias(
					"t2",
					subquery,
				),
			),
			plan.NewUnresolvedTable("baz"),
		),
	)

	result, err := resolveSubqueries(sql.NewEmptyContext(), a, node)
	require.NoError(err)

	require.Equal(expected, result)
}

func TestResolveTables(t *testing.T) {
	require := require.New(t)

	f := getRule("resolve_tables")

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)

	catalog := &sql.Catalog{Databases: []sql.Database{db}}

	a := New(catalog)
	a.Rules = []Rule{f}

	a.CurrentDatabase = "mydb"
	var notAnalyzed sql.Node = plan.NewUnresolvedTable("mytable")
	analyzed, err := f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.NoError(err)
	require.Equal(table, analyzed)

	notAnalyzed = plan.NewUnresolvedTable("nonexistant")
	analyzed, err = f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.Error(err)
	require.Nil(analyzed)

	analyzed, err = f.Apply(sql.NewEmptyContext(), a, table)
	require.NoError(err)
	require.Equal(table, analyzed)

	notAnalyzed = plan.NewUnresolvedTable("dual")
	analyzed, err = f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.NoError(err)
	require.Equal(dualTable, analyzed)
}

func TestResolveTablesNested(t *testing.T) {
	require := require.New(t)

	f := getRule("resolve_tables")

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)

	catalog := &sql.Catalog{Databases: []sql.Database{db}}

	a := New(catalog)
	a.Rules = []Rule{f}
	a.CurrentDatabase = "mydb"

	notAnalyzed := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", true)},
		plan.NewUnresolvedTable("mytable"),
	)
	analyzed, err := f.Apply(sql.NewEmptyContext(), a, notAnalyzed)
	require.NoError(err)
	expected := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", true)},
		table,
	)
	require.Equal(expected, analyzed)
}

func TestResolveNaturalJoins(t *testing.T) {
	require := require.New(t)

	left := mem.NewTable("t1", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t1"},
		{Name: "b", Type: sql.Int64, Source: "t1"},
		{Name: "c", Type: sql.Int64, Source: "t1"},
	})

	right := mem.NewTable("t2", sql.Schema{
		{Name: "d", Type: sql.Int64, Source: "t2"},
		{Name: "c", Type: sql.Int64, Source: "t2"},
		{Name: "b", Type: sql.Int64, Source: "t2"},
		{Name: "e", Type: sql.Int64, Source: "t2"},
	})

	node := plan.NewNaturalJoin(left, right)
	rule := getRule("resolve_natural_joins")

	result, err := rule.Apply(sql.NewEmptyContext(), New(nil), node)
	require.NoError(err)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(1, sql.Int64, "t1", "b", false),
			expression.NewGetFieldWithTable(2, sql.Int64, "t1", "c", false),
			expression.NewGetFieldWithTable(0, sql.Int64, "t1", "a", false),
			expression.NewGetFieldWithTable(3, sql.Int64, "t2", "d", false),
			expression.NewGetFieldWithTable(6, sql.Int64, "t2", "e", false),
		},
		plan.NewInnerJoin(
			left,
			right,
			expression.JoinAnd(
				expression.NewEquals(
					expression.NewGetFieldWithTable(1, sql.Int64, "t1", "b", false),
					expression.NewGetFieldWithTable(5, sql.Int64, "t2", "b", false),
				),
				expression.NewEquals(
					expression.NewGetFieldWithTable(2, sql.Int64, "t1", "c", false),
					expression.NewGetFieldWithTable(4, sql.Int64, "t2", "c", false),
				),
			),
		),
	)

	require.Equal(expected, result)
}

func TestResolveNaturalJoinsEqual(t *testing.T) {
	require := require.New(t)

	left := mem.NewTable("t1", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t1"},
		{Name: "b", Type: sql.Int64, Source: "t1"},
		{Name: "c", Type: sql.Int64, Source: "t1"},
	})

	right := mem.NewTable("t2", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t2"},
		{Name: "b", Type: sql.Int64, Source: "t2"},
		{Name: "c", Type: sql.Int64, Source: "t2"},
	})

	node := plan.NewNaturalJoin(left, right)
	rule := getRule("resolve_natural_joins")

	result, err := rule.Apply(sql.NewEmptyContext(), New(nil), node)
	require.NoError(err)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "t1", "a", false),
			expression.NewGetFieldWithTable(1, sql.Int64, "t1", "b", false),
			expression.NewGetFieldWithTable(2, sql.Int64, "t1", "c", false),
		},
		plan.NewInnerJoin(
			left,
			right,
			expression.JoinAnd(
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "t1", "a", false),
					expression.NewGetFieldWithTable(3, sql.Int64, "t2", "a", false),
				),
				expression.NewEquals(
					expression.NewGetFieldWithTable(1, sql.Int64, "t1", "b", false),
					expression.NewGetFieldWithTable(4, sql.Int64, "t2", "b", false),
				),
				expression.NewEquals(
					expression.NewGetFieldWithTable(2, sql.Int64, "t1", "c", false),
					expression.NewGetFieldWithTable(5, sql.Int64, "t2", "c", false),
				),
			),
		),
	)

	require.Equal(expected, result)
}

func TestResolveNaturalJoinsDisjoint(t *testing.T) {
	require := require.New(t)

	left := mem.NewTable("t1", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t1"},
		{Name: "b", Type: sql.Int64, Source: "t1"},
		{Name: "c", Type: sql.Int64, Source: "t1"},
	})

	right := mem.NewTable("t2", sql.Schema{
		{Name: "d", Type: sql.Int64, Source: "t2"},
		{Name: "e", Type: sql.Int64, Source: "t2"},
	})

	node := plan.NewNaturalJoin(left, right)
	rule := getRule("resolve_natural_joins")

	result, err := rule.Apply(sql.NewEmptyContext(), New(nil), node)
	require.NoError(err)

	expected := plan.NewCrossJoin(left, right)
	require.Equal(expected, result)
}

func TestResolveOrderByLiterals(t *testing.T) {
	require := require.New(t)
	f := getRule("resolve_orderby_literals")

	table := mem.NewTable("t", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t"},
		{Name: "b", Type: sql.Int64, Source: "t"},
	})

	node := plan.NewSort(
		[]plan.SortField{
			{Column: expression.NewLiteral(int64(2), sql.Int64)},
			{Column: expression.NewLiteral(int64(1), sql.Int64)},
		},
		table,
	)

	result, err := f.Apply(sql.NewEmptyContext(), New(nil), node)
	require.NoError(err)

	require.Equal(
		plan.NewSort(
			[]plan.SortField{
				{Column: expression.NewUnresolvedColumn("b")},
				{Column: expression.NewUnresolvedColumn("a")},
			},
			table,
		),
		result,
	)

	node = plan.NewSort(
		[]plan.SortField{
			{Column: expression.NewLiteral(int64(3), sql.Int64)},
			{Column: expression.NewLiteral(int64(1), sql.Int64)},
		},
		table,
	)

	_, err = f.Apply(sql.NewEmptyContext(), New(nil), node)
	require.Error(err)
	require.True(ErrOrderByColumnIndex.Is(err))
}

func TestResolveStar(t *testing.T) {
	f := getRule("resolve_star")

	table := mem.NewTable("mytable", sql.Schema{
		{Name: "a", Type: sql.Int32, Source: "mytable"},
		{Name: "b", Type: sql.Int32, Source: "mytable"},
	})

	table2 := mem.NewTable("mytable2", sql.Schema{
		{Name: "c", Type: sql.Int32, Source: "mytable2"},
		{Name: "d", Type: sql.Int32, Source: "mytable2"},
	})

	testCases := []struct {
		name     string
		node     sql.Node
		expected sql.Node
	}{
		{
			"unqualified star",
			plan.NewProject(
				[]sql.Expression{expression.NewStar()},
				table,
			),
			plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
				},
				table,
			),
		},
		{
			"qualified star",
			plan.NewProject(
				[]sql.Expression{expression.NewQualifiedStar("mytable2")},
				plan.NewCrossJoin(table, table2),
			),
			plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(2, sql.Int32, "mytable2", "c", false),
					expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "d", false),
				},
				plan.NewCrossJoin(table, table2),
			),
		},
		{
			"qualified star and unqualified star",
			plan.NewProject(
				[]sql.Expression{
					expression.NewStar(),
					expression.NewQualifiedStar("mytable2"),
				},
				plan.NewCrossJoin(table, table2),
			),
			plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
					expression.NewGetFieldWithTable(2, sql.Int32, "mytable2", "c", false),
					expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "d", false),
					expression.NewGetFieldWithTable(2, sql.Int32, "mytable2", "c", false),
					expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "d", false),
				},
				plan.NewCrossJoin(table, table2),
			),
		},
		{
			"stars mixed with other expressions",
			plan.NewProject(
				[]sql.Expression{
					expression.NewStar(),
					expression.NewUnresolvedColumn("foo"),
					expression.NewQualifiedStar("mytable2"),
				},
				plan.NewCrossJoin(table, table2),
			),
			plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
					expression.NewGetFieldWithTable(2, sql.Int32, "mytable2", "c", false),
					expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "d", false),
					expression.NewUnresolvedColumn("foo"),
					expression.NewGetFieldWithTable(2, sql.Int32, "mytable2", "c", false),
					expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "d", false),
				},
				plan.NewCrossJoin(table, table2),
			),
		},
		{
			"star in groupby",
			plan.NewGroupBy(
				[]sql.Expression{
					expression.NewStar(),
				},
				nil,
				table,
			),
			plan.NewGroupBy(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
				},
				nil,
				table,
			),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			result, err := f.Apply(sql.NewEmptyContext(), nil, tt.node)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestQualifyColumns(t *testing.T) {
	require := require.New(t)
	f := getRule("qualify_columns")

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	table2 := mem.NewTable("mytable2", sql.Schema{{Name: "i", Type: sql.Int32}})

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("i"),
		},
		table,
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		table,
	)

	result, err := f.Apply(sql.NewEmptyContext(), nil, node)
	require.NoError(err)
	require.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		table,
	)

	result, err = f.Apply(sql.NewEmptyContext(), nil, node)
	require.NoError(err)
	require.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("a", "i"),
		},
		plan.NewTableAlias("a", table),
	)

	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		plan.NewTableAlias("a", table),
	)

	result, err = f.Apply(sql.NewEmptyContext(), nil, node)
	require.NoError(err)
	require.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("z"),
		},
		plan.NewTableAlias("a", table),
	)

	result, err = f.Apply(sql.NewEmptyContext(), nil, node)
	require.NoError(err)
	require.Equal(node, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("foo", "i"),
		},
		plan.NewTableAlias("a", table),
	)

	result, err = f.Apply(sql.NewEmptyContext(), nil, node)
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("i"),
		},
		plan.NewCrossJoin(table, table2),
	)

	_, err = f.Apply(sql.NewEmptyContext(), nil, node)
	require.Error(err)
	require.True(ErrAmbiguousColumnName.Is(err))

	subquery := plan.NewSubqueryAlias(
		"b",
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
			},
			table,
		),
	)
	// preload schema
	_ = subquery.Schema()

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("a", "i"),
		},
		plan.NewCrossJoin(
			plan.NewTableAlias("a", table),
			subquery,
		),
	)

	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		plan.NewCrossJoin(
			plan.NewTableAlias("a", table),
			subquery,
		),
	)

	result, err = f.Apply(sql.NewEmptyContext(), nil, node)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestCatalogIndex(t *testing.T) {
	require := require.New(t)
	f := getRule("index_catalog")

	c := sql.NewCatalog()
	a := New(c)
	a.CurrentDatabase = "foo"

	tbl := mem.NewTable("foo", nil)

	node, err := f.Apply(sql.NewEmptyContext(), a, plan.NewCreateIndex("", tbl, nil, "", make(map[string]string)))
	require.NoError(err)

	ci, ok := node.(*plan.CreateIndex)
	require.True(ok)
	require.Equal(c, ci.Catalog)
	require.Equal("foo", ci.CurrentDatabase)

	node, err = f.Apply(sql.NewEmptyContext(), a, plan.NewDropIndex("foo", tbl))
	require.NoError(err)

	di, ok := node.(*plan.DropIndex)
	require.True(ok)
	require.Equal(c, di.Catalog)
	require.Equal("foo", di.CurrentDatabase)
}

func TestReorderProjection(t *testing.T) {
	require := require.New(t)
	f := getRule("reorder_projection")

	table := mem.NewTable("mytable", sql.Schema{{
		Name: "i", Source: "mytable", Type: sql.Int64,
	}})

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
			expression.NewAlias(expression.NewLiteral(1, sql.Int64), "foo"),
			expression.NewAlias(expression.NewLiteral(2, sql.Int64), "bar"),
		},
		plan.NewSort(
			[]plan.SortField{
				{Column: expression.NewUnresolvedColumn("foo")},
			},
			plan.NewFilter(
				expression.NewEquals(
					expression.NewLiteral(1, sql.Int64),
					expression.NewUnresolvedColumn("bar"),
				),
				table,
			),
		),
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
			expression.NewGetField(2, sql.Int64, "foo", false),
			expression.NewGetField(1, sql.Int64, "bar", false),
		},
		plan.NewSort(
			[]plan.SortField{{Column: expression.NewGetField(2, sql.Int64, "foo", false)}},
			plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
					expression.NewGetField(1, sql.Int64, "bar", false),
					expression.NewAlias(expression.NewLiteral(1, sql.Int64), "foo"),
				},
				plan.NewFilter(
					expression.NewEquals(
						expression.NewLiteral(1, sql.Int64),
						expression.NewGetField(1, sql.Int64, "bar", false),
					),
					plan.NewProject(
						[]sql.Expression{
							expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
							expression.NewAlias(expression.NewLiteral(2, sql.Int64), "bar"),
						},
						table,
					),
				),
			),
		),
	)

	result, err := f.Apply(sql.NewEmptyContext(), New(nil), node)
	require.NoError(err)

	require.Equal(expected, result)
}

func TestEraseProjection(t *testing.T) {
	require := require.New(t)
	f := getRule("erase_projection")

	table := mem.NewTable("mytable", sql.Schema{{
		Name: "i", Source: "mytable", Type: sql.Int64,
	}})

	expected := plan.NewSort(
		[]plan.SortField{{Column: expression.NewGetField(2, sql.Int64, "foo", false)}},
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
				expression.NewGetField(1, sql.Int64, "bar", false),
				expression.NewAlias(expression.NewLiteral(1, sql.Int64), "foo"),
			},
			plan.NewFilter(
				expression.NewEquals(
					expression.NewLiteral(1, sql.Int64),
					expression.NewGetField(1, sql.Int64, "bar", false),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
						expression.NewAlias(expression.NewLiteral(2, sql.Int64), "bar"),
					},
					table,
				),
			),
		),
	)

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
			expression.NewGetField(1, sql.Int64, "bar", false),
			expression.NewGetField(2, sql.Int64, "foo", false),
		},
		expected,
	)

	result, err := f.Apply(sql.NewEmptyContext(), New(nil), node)
	require.NoError(err)

	require.Equal(expected, result)

	result, err = f.Apply(sql.NewEmptyContext(), New(nil), expected)
	require.NoError(err)

	require.Equal(expected, result)
}

func TestOptimizeDistinct(t *testing.T) {
	require := require.New(t)
	notSorted := plan.NewDistinct(mem.NewTable("foo", nil))
	sorted := plan.NewDistinct(plan.NewSort(nil, mem.NewTable("foo", nil)))

	rule := getRule("optimize_distinct")

	analyzedNotSorted, err := rule.Apply(sql.NewEmptyContext(), nil, notSorted)
	require.NoError(err)

	analyzedSorted, err := rule.Apply(sql.NewEmptyContext(), nil, sorted)
	require.NoError(err)

	require.Equal(notSorted, analyzedNotSorted)
	require.Equal(plan.NewOrderedDistinct(sorted.Child), analyzedSorted)
}

func TestPushdownProjection(t *testing.T) {
	require := require.New(t)
	f := getRule("pushdown")

	table := &pushdownProjectionTable{mem.NewTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int32},
		{Name: "f", Type: sql.Float64},
		{Name: "t", Type: sql.Text},
	})}

	table2 := &pushdownProjectionTable{mem.NewTable("mytable2", sql.Schema{
		{Name: "i2", Type: sql.Int32},
		{Name: "f2", Type: sql.Float64},
		{Name: "t2", Type: sql.Text},
	})}

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
		},
		plan.NewFilter(
			expression.NewAnd(
				expression.NewEquals(
					expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
					expression.NewLiteral(3.14, sql.Float64),
				),
				expression.NewIsNull(
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
				),
			),
			plan.NewCrossJoin(table, table2),
		),
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
		},
		plan.NewFilter(
			expression.NewAnd(
				expression.NewEquals(
					expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
					expression.NewLiteral(3.14, sql.Float64),
				),
				expression.NewIsNull(
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
				),
			),
			plan.NewCrossJoin(
				plan.NewPushdownProjectionTable([]string{"i", "f"}, table),
				plan.NewPushdownProjectionTable([]string{"i2"}, table2),
			),
		),
	)

	result, err := f.Apply(sql.NewEmptyContext(), nil, node)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestPushdownProjectionAndFilters(t *testing.T) {
	require := require.New(t)
	a := New(sql.NewCatalog())

	table := &pushdownProjectionAndFiltersTable{mem.NewTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "mytable"},
		{Name: "f", Type: sql.Float64, Source: "mytable"},
		{Name: "t", Type: sql.Text, Source: "mytable"},
	})}

	table2 := &pushdownProjectionAndFiltersTable{mem.NewTable("mytable2", sql.Schema{
		{Name: "i2", Type: sql.Int32, Source: "mytable2"},
		{Name: "f2", Type: sql.Float64, Source: "mytable2"},
		{Name: "t2", Type: sql.Text, Source: "mytable2"},
	})}

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		plan.NewFilter(
			expression.NewAnd(
				expression.NewAnd(
					expression.NewEquals(
						expression.NewUnresolvedQualifiedColumn("mytable", "f"),
						expression.NewLiteral(3.14, sql.Float64),
					),
					expression.NewGreaterThan(
						expression.NewUnresolvedQualifiedColumn("mytable", "f"),
						expression.NewLiteral(3., sql.Float64),
					),
				),
				expression.NewIsNull(
					expression.NewUnresolvedQualifiedColumn("mytable2", "i2"),
				),
			),
			plan.NewCrossJoin(table, table2),
		),
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
		},
		plan.NewFilter(
			expression.NewAnd(
				expression.NewGreaterThan(
					expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
					expression.NewLiteral(3., sql.Float64),
				),
				expression.NewIsNull(
					expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "i2", false),
				),
			),
			plan.NewCrossJoin(
				plan.NewPushdownProjectionAndFiltersTable(
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
						expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
					},
					[]sql.Expression{
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
							expression.NewLiteral(3.14, sql.Float64),
						),
					},
					table,
				),
				plan.NewPushdownProjectionAndFiltersTable(
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
					},
					nil,
					table2,
				),
			),
		),
	)

	result, err := a.Analyze(sql.NewEmptyContext(), node)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestPushdownIndexable(t *testing.T) {
	require := require.New(t)
	a := New(sql.NewCatalog())

	var index1, index2, index3 dummyIndex
	var lookup, lookup2 dummyIndexLookup

	table := &indexable{
		&indexLookup{lookup, []sql.Index{index1, index2}},
		&indexableTable{&pushdownProjectionAndFiltersTable{mem.NewTable("mytable", sql.Schema{
			{Name: "i", Type: sql.Int32, Source: "mytable"},
			{Name: "f", Type: sql.Float64, Source: "mytable"},
			{Name: "t", Type: sql.Text, Source: "mytable"},
		})}},
	}

	table2 := &indexable{
		&indexLookup{lookup2, []sql.Index{index3}},
		&indexableTable{&pushdownProjectionAndFiltersTable{mem.NewTable("mytable2", sql.Schema{
			{Name: "i2", Type: sql.Int32, Source: "mytable2"},
			{Name: "f2", Type: sql.Float64, Source: "mytable2"},
			{Name: "t2", Type: sql.Text, Source: "mytable2"},
		})}},
	}

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		plan.NewFilter(
			expression.NewAnd(
				expression.NewAnd(
					expression.NewEquals(
						expression.NewUnresolvedQualifiedColumn("mytable", "f"),
						expression.NewLiteral(3.14, sql.Float64),
					),
					expression.NewGreaterThan(
						expression.NewUnresolvedQualifiedColumn("mytable", "f"),
						expression.NewLiteral(3., sql.Float64),
					),
				),
				expression.NewIsNull(
					expression.NewUnresolvedQualifiedColumn("mytable2", "i2"),
				),
			),
			plan.NewCrossJoin(table, table2),
		),
	)

	expected := &releaser{plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
		},
		plan.NewFilter(
			expression.NewAnd(
				expression.NewGreaterThan(
					expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
					expression.NewLiteral(3., sql.Float64),
				),
				expression.NewIsNull(
					expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "i2", false),
				),
			),
			plan.NewCrossJoin(
				plan.NewIndexableTable(
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
						expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
					},
					[]sql.Expression{
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
							expression.NewLiteral(3.14, sql.Float64),
						),
					},
					lookup,
					table.Indexable,
				),
				plan.NewIndexableTable(
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
					},
					nil,
					lookup2,
					table2.Indexable,
				),
			),
		),
	),
		nil,
	}

	result, err := a.Analyze(sql.NewEmptyContext(), node)
	require.NoError(err)

	// we need to remove the release function to compare, otherwise it will fail
	result, err = result.TransformUp(func(node sql.Node) (sql.Node, error) {
		switch node := node.(type) {
		case *releaser:
			return &releaser{Child: node.Child}, nil
		default:
			return node, nil
		}
	})
	require.NoError(err)

	require.Equal(expected, result)
}

type pushdownProjectionTable struct {
	sql.Table
}

var _ sql.PushdownProjectionTable = (*pushdownProjectionTable)(nil)

func (pushdownProjectionTable) WithProject(*sql.Context, []string) (sql.RowIter, error) {
	panic("not implemented")
}

func (t *pushdownProjectionTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(t)
}

func (t *pushdownProjectionTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return t, nil
}

type pushdownProjectionAndFiltersTable struct {
	sql.Table
}

var _ sql.PushdownProjectionAndFiltersTable = (*pushdownProjectionAndFiltersTable)(nil)

func (pushdownProjectionAndFiltersTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	var handled []sql.Expression
	for _, f := range filters {
		if eq, ok := f.(*expression.Equals); ok {
			handled = append(handled, eq)
		}
	}
	return handled
}

func (pushdownProjectionAndFiltersTable) WithProjectAndFilters(_ *sql.Context, cols, filters []sql.Expression) (sql.RowIter, error) {
	panic("not implemented")
}

func (t *pushdownProjectionAndFiltersTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(t)
}

func (t *pushdownProjectionAndFiltersTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return t, nil
}

type dummyIndexLookup struct{}

func (dummyIndexLookup) Values() (sql.IndexValueIter, error) {
	return nil, nil
}

type indexableTable struct {
	sql.PushdownProjectionAndFiltersTable
}

func (i *indexableTable) IndexKeyValueIter(_ *sql.Context, colNames []string) (sql.IndexKeyValueIter, error) {
	panic("not implemented")
}

func (i *indexableTable) WithProjectFiltersAndIndex(
	ctx *sql.Context,
	columns, filters []sql.Expression,
	index sql.IndexValueIter,
) (sql.RowIter, error) {
	panic("not implemented")
}

func (i *indexableTable) TransformUp(fn sql.TransformNodeFunc) (sql.Node, error) {
	return fn(i)
}

func (i *indexableTable) TransformExpressionsUp(fn sql.TransformExprFunc) (sql.Node, error) {
	return i, nil
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
	done, err := catalog.AddIndex(idx1)
	require.NoError(err)
	close(done)

	idx2 := &dummyIndex{
		"t1",
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "t1", "foo", false),
		},
	}
	done, err = catalog.AddIndex(idx2)

	require.NoError(err)
	close(done)

	time.Sleep(50 * time.Millisecond)
	a := New(catalog)

	t1 := &indexableTable{
		&pushdownProjectionAndFiltersTable{
			mem.NewTable("t1", sql.Schema{
				{Name: "foo", Type: sql.Int64, Source: "t1"},
			}),
		},
	}

	t2 := &indexableTable{
		&pushdownProjectionAndFiltersTable{
			mem.NewTable("t2", sql.Schema{
				{Name: "bar", Type: sql.Int64, Source: "t2"},
				{Name: "baz", Type: sql.Int64, Source: "t2"},
			}),
		},
	}

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
				t1,
				t2,
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "t1", "foo", false),
					expression.NewGetFieldWithTable(0, sql.Int64, "t2", "baz", false),
				),
			),
		),
	)

	expected := plan.NewProject(
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
				&indexable{&indexLookup{
					&mergeableIndexLookup{id: "2"},
					[]sql.Index{idx2},
				}, t1},
				&indexable{&indexLookup{
					&mergeableIndexLookup{id: "1"},
					[]sql.Index{idx1},
				}, t2},
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "t1", "foo", false),
					expression.NewGetFieldWithTable(0, sql.Int64, "t2", "baz", false),
				),
			),
		),
	)

	result, err := assignIndexes(sql.NewEmptyContext(), a, node)
	require.NoError(err)

	require.Equal(expected, result)
}

func TestGetIndexes(t *testing.T) {
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
					[]sql.Index{
						&dummyIndex{
							table: "t1",
							expr: []sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
							},
						},
					},
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
						&dummyIndex{
							table: "t1",
							expr: []sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
							},
						},
						&dummyIndex{
							table: "t1",
							expr: []sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
							},
						},
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
						&dummyIndex{
							table: "t1",
							expr: []sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
							},
						},
						&dummyIndex{
							table: "t1",
							expr: []sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
							},
						},
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
						&dummyIndex{
							table: "t1",
							expr: []sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
							},
						},
						&dummyIndex{
							table: "t1",
							expr: []sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
							},
						},
						&dummyIndex{
							table: "t1",
							expr: []sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
							},
						},
						&dummyIndex{
							table: "t1",
							expr: []sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
							},
						},
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
						&dummyIndex{
							table: "t1",
							expr: []sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
							},
						},
						&dummyIndex{
							table: "t1",
							expr: []sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
							},
						},
						&dummyIndex{
							table: "t1",
							expr: []sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
							},
						},
						&dummyIndex{
							table: "t1",
							expr: []sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
							},
						},
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
					[]sql.Index{&dummyIndex{
						table: "t1",
						expr: []sql.Expression{
							expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
						},
					}},
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
					[]sql.Index{&dummyIndex{
						table: "t1",
						expr: []sql.Expression{
							expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
						},
					}},
				},
				"t2": &indexLookup{
					&mergeableIndexLookup{id: "1, 2"},
					[]sql.Index{&dummyIndex{
						table: "t2",
						expr: []sql.Expression{
							expression.NewGetFieldWithTable(0, sql.Int64, "t2", "foo", false),
							expression.NewGetFieldWithTable(0, sql.Int64, "t2", "bar", false),
						},
					}},
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
					[]sql.Index{&dummyIndex{
						table: "t1",
						expr: []sql.Expression{
							expression.NewGetFieldWithTable(0, sql.Int64, "t1", "bar", false),
						},
					}},
				},
				"t2": &indexLookup{
					&mergeableIndexLookup{id: "5", unions: []string{"1, 2"}},
					[]sql.Index{
						&dummyIndex{
							table: "t2",
							expr: []sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "t2", "bar", false),
							},
						},
						&dummyIndex{
							table: "t2",
							expr: []sql.Expression{
								expression.NewGetFieldWithTable(0, sql.Int64, "t2", "foo", false),
								expression.NewGetFieldWithTable(0, sql.Int64, "t2", "bar", false),
							},
						},
					},
				},
			},
			true,
		},
	}

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

	catalog := sql.NewCatalog()
	for _, idx := range indexes {
		done, err := catalog.AddIndex(idx)
		require.NoError(t, err)
		close(done)
	}

	time.Sleep(50 * time.Millisecond)
	a := New(catalog)

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
	}

	for _, idx := range indexes {
		done, err := catalog.AddIndex(idx)
		require.NoError(err)
		close(done)
	}

	time.Sleep(50 * time.Millisecond)
	a := New(catalog)

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
	}

	require.Equal(expected, result)

	expectedUsed := map[sql.Expression]struct{}{
		exprs[0]: struct{}{},
		exprs[1]: struct{}{},
		exprs[2]: struct{}{},
		exprs[4]: struct{}{},
		exprs[5]: struct{}{},
	}
	require.Equal(expectedUsed, used)
}

func or(left, right sql.Expression) sql.Expression {
	return expression.NewOr(left, right)
}

func and(left, right sql.Expression) sql.Expression {
	return expression.NewAnd(left, right)
}

func col(idx int, table, col string) sql.Expression {
	return expression.NewGetFieldWithTable(idx, sql.Int64, table, col, false)
}

func eq(left, right sql.Expression) sql.Expression {
	return expression.NewEquals(left, right)
}

func lit(n int64) sql.Expression {
	return expression.NewLiteral(n, sql.Int64)
}

type dummyIndex struct {
	table string
	expr  []sql.Expression
}

var _ sql.Index = (*dummyIndex)(nil)

func (dummyIndex) Database() string { return "" }
func (dummyIndex) Driver() string   { return "" }
func (i dummyIndex) ExpressionHashes() []sql.ExpressionHash {
	var hashes []sql.ExpressionHash
	for _, e := range i.expr {
		h := sha1.New()
		h.Write([]byte(e.String()))
		hashes = append(hashes, h.Sum(nil))
	}
	return hashes
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
func (i dummyIndex) Has(key ...interface{}) (bool, error) {
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
		indexesIntersection(left, right),
	)
}

func TestCanMergeIndexes(t *testing.T) {
	require := require.New(t)

	require.False(canMergeIndexes(new(mergeableIndexLookup), new(dummyIndexLookup)))
	require.True(canMergeIndexes(new(mergeableIndexLookup), new(mergeableIndexLookup)))
}

type mergeableIndexLookup struct {
	id            string
	unions        []string
	intersections []string
}

var _ sql.Mergeable = (*mergeableIndexLookup)(nil)
var _ sql.SetOperations = (*mergeableIndexLookup)(nil)

func (i *mergeableIndexLookup) IsMergeable(lookup sql.IndexLookup) bool {
	_, ok := lookup.(*mergeableIndexLookup)
	return ok
}

func (i *mergeableIndexLookup) Values() (sql.IndexValueIter, error) {
	panic("not implemented")
}

func (i *mergeableIndexLookup) Difference(indexes ...sql.IndexLookup) sql.IndexLookup {
	panic("not implemented")
}

func (i *mergeableIndexLookup) Intersection(indexes ...sql.IndexLookup) sql.IndexLookup {
	var intersections, unions []string
	for _, idx := range indexes {
		intersections = append(intersections, idx.(*mergeableIndexLookup).id)
		intersections = append(intersections, idx.(*mergeableIndexLookup).intersections...)
		unions = append(unions, idx.(*mergeableIndexLookup).unions...)
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

func getRule(name string) Rule {
	for _, rule := range DefaultRules {
		if rule.Name == name {
			return rule
		}
	}
	panic("missing rule")
}
