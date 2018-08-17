package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

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
	a := NewDefault(sql.NewCatalog())

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
	a := NewDefault(sql.NewCatalog())

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

func (dummyIndexLookup) Indexes() []string {
	return nil
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
