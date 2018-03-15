package analyzer

import (
	"testing"

	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"

	"github.com/stretchr/testify/require"
)

func TestValidateResolved(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule(validateResolvedRule)

	err := vr.Apply(dummyNode{true})
	require.NoError(err)

	err = vr.Apply(dummyNode{false})
	require.Error(err)
}

func TestValidateOrderBy(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule(validateOrderByRule)

	err := vr.Apply(dummyNode{true})
	require.NoError(err)
	err = vr.Apply(dummyNode{false})
	require.NoError(err)

	err = vr.Apply(plan.NewSort(
		[]plan.SortField{{Column: expression.NewCount(nil), Order: plan.Descending}},
		nil,
	))
	require.Error(err)
}

func TestValidateGroupBy(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule(validateGroupByRule)

	err := vr.Apply(dummyNode{true})
	require.NoError(err)
	err = vr.Apply(dummyNode{false})
	require.NoError(err)

	childSchema := sql.Schema{
		{Name: "col1", Type: sql.Text},
		{Name: "col2", Type: sql.Int64},
	}

	child := mem.NewTable("test", childSchema)
	child.Insert(sql.NewRow("col1_1", int64(1111)))
	child.Insert(sql.NewRow("col1_1", int64(2222)))
	child.Insert(sql.NewRow("col1_2", int64(4444)))
	child.Insert(sql.NewRow("col1_1", int64(1111)))
	child.Insert(sql.NewRow("col1_2", int64(4444)))

	p := plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias(expression.NewGetField(0, sql.Text, "col1", true), "alias"),
			expression.NewGetField(0, sql.Text, "col1", true),
			expression.NewCount(expression.NewGetField(1, sql.Int64, "col2", true)),
		},
		[]sql.Expression{
			expression.NewGetField(0, sql.Text, "col1", true),
		},
		child,
	)

	err = vr.Apply(p)
	require.NoError(err)
}

func TestValidateGroupByErr(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule(validateGroupByRule)

	err := vr.Apply(dummyNode{true})
	require.NoError(err)
	err = vr.Apply(dummyNode{false})
	require.NoError(err)

	childSchema := sql.Schema{
		{Name: "col1", Type: sql.Text},
		{Name: "col2", Type: sql.Int64},
	}

	child := mem.NewTable("test", childSchema)
	child.Insert(sql.NewRow("col1_1", int64(1111)))
	child.Insert(sql.NewRow("col1_1", int64(2222)))
	child.Insert(sql.NewRow("col1_2", int64(4444)))
	child.Insert(sql.NewRow("col1_1", int64(1111)))
	child.Insert(sql.NewRow("col1_2", int64(4444)))

	p := plan.NewGroupBy(
		[]sql.Expression{
			expression.NewGetField(0, sql.Text, "col1", true),
			expression.NewGetField(1, sql.Int64, "col2", true),
		},
		[]sql.Expression{
			expression.NewGetField(0, sql.Text, "col1", true),
		},
		child,
	)

	err = vr.Apply(p)
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
			mem.NewTable("mytable", sql.Schema{
				{Name: "foo", Source: "mytable"},
				{Name: "bar", Source: "mytable"},
			}),
			true,
		},
		{
			"table with invalid schema",
			mem.NewTable("mytable", sql.Schema{
				{Name: "foo", Source: "mytable"},
				{Name: "bar", Source: "something"},
			}),
			false,
		},
		{
			"table alias with table",
			plan.NewTableAlias("foo", mem.NewTable("mytable", sql.Schema{
				{Name: "foo", Source: "mytable"},
			})),
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
			err := rule.Apply(tt.node)
			if tt.ok {
				require.NoError(err)
			} else {
				require.Error(err)
				require.True(ErrValidationSchemaSource.Is(err))
			}
		})
	}
}

type dummyNode struct{ resolved bool }

func (n dummyNode) String() string                                               { return "dummynode" }
func (n dummyNode) Resolved() bool                                               { return n.resolved }
func (dummyNode) Schema() sql.Schema                                             { return sql.Schema{} }
func (dummyNode) Children() []sql.Node                                           { return nil }
func (dummyNode) RowIter(sql.Session) (sql.RowIter, error)                       { return nil, nil }
func (dummyNode) TransformUp(func(sql.Node) (sql.Node, error)) (sql.Node, error) { return nil, nil }
func (dummyNode) TransformExpressionsUp(
	func(sql.Expression) (sql.Expression, error),
) (sql.Node, error) {
	return nil, nil
}

func getValidationRule(name string) ValidationRule {
	for _, rule := range DefaultValidationRules {
		if rule.Name == name {
			return rule
		}
	}
	panic("missing rule")
}
