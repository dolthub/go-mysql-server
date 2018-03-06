package analyzer_test

import (
	"testing"

	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/analyzer"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"

	"github.com/stretchr/testify/require"
)

func Test_resolved(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule("validate_resolved")

	require.Equal(vr.Name, "validate_resolved")

	err := vr.Apply(dummyNode{true})
	require.NoError(err)

	err = vr.Apply(dummyNode{false})
	require.Error(err)

}

func Test_orderBy(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule("validate_order_by")

	require.Equal(vr.Name, "validate_order_by")

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

func Test_GroupBy(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule("validate_group_by")
	require.Equal(vr.Name, "validate_group_by")

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

func Test_GroupBy_Err(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule("validate_group_by")
	require.Equal(vr.Name, "validate_group_by")

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

type dummyNode struct{ resolved bool }

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

func getValidationRule(name string) analyzer.ValidationRule {
	for _, rule := range analyzer.DefaultValidationRules {
		if rule.Name == name {
			return rule
		}
	}
	panic("missing rule")
}
