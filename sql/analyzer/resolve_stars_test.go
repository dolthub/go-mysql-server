package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

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
				plan.NewResolvedTable(table),
			),
			plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
				},
				plan.NewResolvedTable(table),
			),
		},
		{
			"qualified star",
			plan.NewProject(
				[]sql.Expression{expression.NewQualifiedStar("mytable2")},
				plan.NewCrossJoin(
					plan.NewResolvedTable(table),
					plan.NewResolvedTable(table2),
				),
			),
			plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(2, sql.Int32, "mytable2", "c", false),
					expression.NewGetFieldWithTable(3, sql.Int32, "mytable2", "d", false),
				},
				plan.NewCrossJoin(
					plan.NewResolvedTable(table),
					plan.NewResolvedTable(table2),
				),
			),
		},
		{
			"qualified star and unqualified star",
			plan.NewProject(
				[]sql.Expression{
					expression.NewStar(),
					expression.NewQualifiedStar("mytable2"),
				},
				plan.NewCrossJoin(
					plan.NewResolvedTable(table),
					plan.NewResolvedTable(table2),
				),
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
				plan.NewCrossJoin(
					plan.NewResolvedTable(table),
					plan.NewResolvedTable(table2),
				),
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
				plan.NewCrossJoin(
					plan.NewResolvedTable(table),
					plan.NewResolvedTable(table2),
				),
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
				plan.NewCrossJoin(
					plan.NewResolvedTable(table),
					plan.NewResolvedTable(table2),
				),
			),
		},
		{
			"star in groupby",
			plan.NewGroupBy(
				[]sql.Expression{
					expression.NewStar(),
				},
				nil,
				plan.NewResolvedTable(table),
			),
			plan.NewGroupBy(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
				},
				nil,
				plan.NewResolvedTable(table),
			),
		},
		{ // note that this behaviour deviates from MySQL
			"star after some expressions",
			plan.NewProject(
				[]sql.Expression{
					expression.NewUnresolvedColumn("foo"),
					expression.NewStar(),
				},
				plan.NewResolvedTable(table),
			),
			plan.NewProject(
				[]sql.Expression{
					expression.NewUnresolvedColumn("foo"),
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
				},
				plan.NewResolvedTable(table),
			),
		},
		{ // note that this behaviour deviates from MySQL
			"unqualified star used multiple times",
			plan.NewProject(
				[]sql.Expression{
					expression.NewStar(),
					expression.NewStar(),
				},
				plan.NewResolvedTable(table),
			),
			plan.NewProject(
				[]sql.Expression{
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "a", false),
					expression.NewGetFieldWithTable(1, sql.Int32, "mytable", "b", false),
				},
				plan.NewResolvedTable(table),
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
