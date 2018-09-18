package analyzer

import (
	"testing"

	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression/function/aggregation"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"

	"github.com/stretchr/testify/require"
)

func TestValidateResolved(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule(validateResolvedRule)

	_, err := vr.Apply(sql.NewEmptyContext(), nil, dummyNode{true})
	require.NoError(err)

	_, err = vr.Apply(sql.NewEmptyContext(), nil, dummyNode{false})
	require.Error(err)
}

func TestValidateOrderBy(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule(validateOrderByRule)

	_, err := vr.Apply(sql.NewEmptyContext(), nil, dummyNode{true})
	require.NoError(err)
	_, err = vr.Apply(sql.NewEmptyContext(), nil, dummyNode{false})
	require.NoError(err)

	_, err = vr.Apply(sql.NewEmptyContext(), nil, plan.NewSort(
		[]plan.SortField{{Column: aggregation.NewCount(nil), Order: plan.Descending}},
		nil,
	))
	require.Error(err)
}

func TestValidateGroupBy(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule(validateGroupByRule)

	_, err := vr.Apply(sql.NewEmptyContext(), nil, dummyNode{true})
	require.NoError(err)
	_, err = vr.Apply(sql.NewEmptyContext(), nil, dummyNode{false})
	require.NoError(err)

	childSchema := sql.Schema{
		{Name: "col1", Type: sql.Text},
		{Name: "col2", Type: sql.Int64},
	}

	child := mem.NewTable("test", childSchema)

	rows := []sql.Row{
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_1", int64(2222)),
		sql.NewRow("col1_2", int64(4444)),
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_2", int64(4444)),
	}

	for _, r := range rows {
		require.NoError(child.Insert(sql.NewEmptyContext(), r))
	}

	p := plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias(expression.NewGetField(0, sql.Text, "col1", true), "alias"),
			expression.NewGetField(0, sql.Text, "col1", true),
			aggregation.NewCount(expression.NewGetField(1, sql.Int64, "col2", true)),
		},
		[]sql.Expression{
			expression.NewGetField(0, sql.Text, "col1", true),
		},
		plan.NewResolvedTable(child),
	)

	_, err = vr.Apply(sql.NewEmptyContext(), nil, p)
	require.NoError(err)
}

func TestValidateGroupByErr(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule(validateGroupByRule)

	_, err := vr.Apply(sql.NewEmptyContext(), nil, dummyNode{true})
	require.NoError(err)
	_, err = vr.Apply(sql.NewEmptyContext(), nil, dummyNode{false})
	require.NoError(err)

	childSchema := sql.Schema{
		{Name: "col1", Type: sql.Text},
		{Name: "col2", Type: sql.Int64},
	}

	child := mem.NewTable("test", childSchema)

	rows := []sql.Row{
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_1", int64(2222)),
		sql.NewRow("col1_2", int64(4444)),
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_2", int64(4444)),
	}

	for _, r := range rows {
		require.NoError(child.Insert(sql.NewEmptyContext(), r))
	}

	p := plan.NewGroupBy(
		[]sql.Expression{
			expression.NewGetField(0, sql.Text, "col1", true),
			expression.NewGetField(1, sql.Int64, "col2", true),
		},
		[]sql.Expression{
			expression.NewGetField(0, sql.Text, "col1", true),
		},
		plan.NewResolvedTable(child),
	)

	_, err = vr.Apply(sql.NewEmptyContext(), nil, p)
	require.Error(err)
}

func TestValidateSchemaSource(t *testing.T) {
	testCases := []struct {
		name string
		node sql.Node
		ok   bool
	}{
		{
			"some random node",
			plan.NewProject(nil, nil),
			true,
		},
		{
			"table with valid schema",
			plan.NewResolvedTable(
				mem.NewTable(
					"mytable",
					sql.Schema{
						{Name: "foo", Source: "mytable"},
						{Name: "bar", Source: "mytable"},
					},
				),
			),
			true,
		},
		{
			"table with invalid schema",
			plan.NewResolvedTable(
				mem.NewTable(
					"mytable",
					sql.Schema{
						{Name: "foo", Source: ""},
						{Name: "bar", Source: "something"},
					},
				),
			),
			false,
		},
		{
			"table alias with table",
			plan.NewTableAlias("foo", plan.NewResolvedTable(
				mem.NewTable("mytable", sql.Schema{
					{Name: "foo", Source: "mytable"},
				}),
			)),
			true,
		},
		{
			"table alias with subquery",
			plan.NewTableAlias(
				"foo",
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, sql.Text, "bar", false),
						expression.NewGetField(1, sql.Int64, "baz", false),
					},
					nil,
				),
			),
			true,
		},
	}

	rule := getValidationRule(validateSchemaSourceRule)
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			_, err := rule.Apply(sql.NewEmptyContext(), nil, tt.node)
			if tt.ok {
				require.NoError(err)
			} else {
				require.Error(err)
				require.True(ErrValidationSchemaSource.Is(err))
			}
		})
	}
}

func TestValidateProjectTuples(t *testing.T) {
	testCases := []struct {
		name string
		node sql.Node
		ok   bool
	}{
		{
			"project with no tuple",
			plan.NewProject([]sql.Expression{
				expression.NewLiteral(1, sql.Int64),
			}, nil),
			true,
		},
		{
			"project with a 1 elem tuple",
			plan.NewProject([]sql.Expression{
				expression.NewTuple(
					expression.NewLiteral(1, sql.Int64),
				),
			}, nil),
			true,
		},
		{
			"project with a 2 elem tuple",
			plan.NewProject([]sql.Expression{
				expression.NewTuple(
					expression.NewLiteral(1, sql.Int64),
					expression.NewLiteral(1, sql.Int64),
				),
			}, nil),
			false,
		},
		{
			"groupby with no tuple",
			plan.NewGroupBy([]sql.Expression{
				expression.NewLiteral(1, sql.Int64),
			}, nil, nil),
			true,
		},
		{
			"groupby with a 1 elem tuple",
			plan.NewGroupBy([]sql.Expression{
				expression.NewTuple(
					expression.NewLiteral(1, sql.Int64),
				),
			}, nil, nil),
			true,
		},
		{
			"groupby with a 2 elem tuple",
			plan.NewGroupBy([]sql.Expression{
				expression.NewTuple(
					expression.NewLiteral(1, sql.Int64),
					expression.NewLiteral(1, sql.Int64),
				),
			}, nil, nil),
			false,
		},
	}

	rule := getValidationRule(validateProjectTuplesRule)
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			_, err := rule.Apply(sql.NewEmptyContext(), nil, tt.node)
			if tt.ok {
				require.NoError(err)
			} else {
				require.Error(err)
				require.True(ErrProjectTuple.Is(err))
			}
		})
	}
}

func TestValidateIndexCreation(t *testing.T) {
	table := mem.NewTable("foo", sql.Schema{
		{Name: "a", Source: "foo"},
		{Name: "b", Source: "foo"},
	})

	testCases := []struct {
		name string
		node sql.Node
		ok   bool
	}{
		{
			"columns from another table",
			plan.NewCreateIndex(
				"idx", plan.NewResolvedTable(table),
				[]sql.Expression{expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					expression.NewGetFieldWithTable(0, sql.Int64, "bar", "b", false),
				)},
				"",
				make(map[string]string),
			),
			false,
		},
		{
			"columns that don't exist",
			plan.NewCreateIndex(
				"idx", plan.NewResolvedTable(table),
				[]sql.Expression{expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "c", false),
				)},
				"",
				make(map[string]string),
			),
			false,
		},
		{
			"columns only from table",
			plan.NewCreateIndex(
				"idx", plan.NewResolvedTable(table),
				[]sql.Expression{expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "b", false),
				)},
				"",
				make(map[string]string),
			),
			true,
		},
	}

	rule := getValidationRule(validateIndexCreationRule)
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			_, err := rule.Apply(sql.NewEmptyContext(), nil, tt.node)
			if tt.ok {
				require.NoError(err)
			} else {
				require.Error(err)
				require.True(ErrUnknownIndexColumns.Is(err))
			}
		})
	}
}

type dummyNode struct{ resolved bool }

func (n dummyNode) String() string                                    { return "dummynode" }
func (n dummyNode) Resolved() bool                                    { return n.resolved }
func (dummyNode) Schema() sql.Schema                                  { return nil }
func (dummyNode) Children() []sql.Node                                { return nil }
func (dummyNode) RowIter(*sql.Context) (sql.RowIter, error)           { return nil, nil }
func (dummyNode) TransformUp(sql.TransformNodeFunc) (sql.Node, error) { return nil, nil }
func (dummyNode) TransformExpressionsUp(
	sql.TransformExprFunc,
) (sql.Node, error) {
	return nil, nil
}

func getValidationRule(name string) Rule {
	for _, rule := range DefaultValidationRules {
		if rule.Name == name {
			return rule
		}
	}
	panic("missing rule")
}
