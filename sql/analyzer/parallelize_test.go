package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestParallelize(t *testing.T) {
	require := require.New(t)
	table := mem.NewTable("t", nil)
	rule := getRule("parallelize")
	node := plan.NewProject(
		nil,
		plan.NewInnerJoin(
			plan.NewFilter(
				expression.NewLiteral(1, sql.Int64),
				plan.NewResolvedTable("bar", table),
			),
			plan.NewFilter(
				expression.NewLiteral(1, sql.Int64),
				plan.NewResolvedTable("foo", table),
			),
			expression.NewLiteral(1, sql.Int64),
		),
	)

	expected := plan.NewProject(
		nil,
		plan.NewInnerJoin(
			plan.NewExchange(
				2,
				plan.NewFilter(
					expression.NewLiteral(1, sql.Int64),
					plan.NewResolvedTable("bar", table),
				),
			),
			plan.NewExchange(
				2,
				plan.NewFilter(
					expression.NewLiteral(1, sql.Int64),
					plan.NewResolvedTable("foo", table),
				),
			),
			expression.NewLiteral(1, sql.Int64),
		),
	)

	result, err := rule.Apply(sql.NewEmptyContext(), &Analyzer{Parallelism: 2}, node)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestParallelizeCreateIndex(t *testing.T) {
	require := require.New(t)
	table := mem.NewTable("t", nil)
	rule := getRule("parallelize")
	node := plan.NewCreateIndex(
		"",
		plan.NewResolvedTable("bar", table),
		nil,
		"",
		nil,
	)

	result, err := rule.Apply(sql.NewEmptyContext(), &Analyzer{Parallelism: 1}, node)
	require.NoError(err)
	require.Equal(node, result)
}

func TestIsParallelizable(t *testing.T) {
	table := mem.NewTable("t", nil)

	testCases := []struct {
		name string
		node sql.Node
		ok   bool
	}{
		{
			"just table",
			plan.NewResolvedTable("foo", table),
			true,
		},
		{
			"filter",
			plan.NewFilter(
				expression.NewLiteral(1, sql.Int64),
				plan.NewResolvedTable("foo", table),
			),
			true,
		},
		{
			"project",
			plan.NewProject(
				nil,
				plan.NewFilter(
					expression.NewLiteral(1, sql.Int64),
					plan.NewResolvedTable("foo", table),
				),
			),
			true,
		},
		{
			"join",
			plan.NewInnerJoin(
				plan.NewResolvedTable("foo", table),
				plan.NewResolvedTable("bar", table),
				expression.NewLiteral(1, sql.Int64),
			),
			false,
		},
		{
			"group by",
			plan.NewGroupBy(
				nil,
				nil,
				plan.NewResolvedTable("foo", nil),
			),
			false,
		},
		{
			"limit",
			plan.NewLimit(
				5,
				plan.NewResolvedTable("foo", nil),
			),
			false,
		},
		{
			"offset",
			plan.NewOffset(
				5,
				plan.NewResolvedTable("foo", nil),
			),
			false,
		},
		{
			"sort",
			plan.NewSort(
				nil,
				plan.NewResolvedTable("foo", nil),
			),
			false,
		},
		{
			"distinct",
			plan.NewDistinct(
				plan.NewResolvedTable("foo", nil),
			),
			false,
		},
		{
			"ordered distinct",
			plan.NewOrderedDistinct(
				plan.NewResolvedTable("foo", nil),
			),
			false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.ok, isParallelizable(tt.node))
		})
	}
}

func TestRemoveRedundantExchanges(t *testing.T) {
	require := require.New(t)

	table := mem.NewTable("t", nil)

	node := plan.NewProject(
		nil,
		plan.NewInnerJoin(
			plan.NewExchange(
				1,
				plan.NewFilter(
					expression.NewLiteral(1, sql.Int64),
					plan.NewExchange(
						1,
						plan.NewResolvedTable("bar", table),
					),
				),
			),
			plan.NewExchange(
				1,
				plan.NewFilter(
					expression.NewLiteral(1, sql.Int64),
					plan.NewExchange(
						1,
						plan.NewResolvedTable("foo", table),
					),
				),
			),
			expression.NewLiteral(1, sql.Int64),
		),
	)

	expected := plan.NewProject(
		nil,
		plan.NewInnerJoin(
			plan.NewExchange(
				1,
				plan.NewFilter(
					expression.NewLiteral(1, sql.Int64),
					plan.NewResolvedTable("bar", table),
				),
			),
			plan.NewExchange(
				1,
				plan.NewFilter(
					expression.NewLiteral(1, sql.Int64),
					plan.NewResolvedTable("foo", table),
				),
			),
			expression.NewLiteral(1, sql.Int64),
		),
	)

	result, err := node.TransformUp(removeRedundantExchanges)
	require.NoError(err)
	require.Equal(expected, result)
}
