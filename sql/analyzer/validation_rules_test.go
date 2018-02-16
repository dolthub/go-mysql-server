package analyzer_test

import (
	"testing"

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

	err := vr.Apply(nil, dummyNode{true})
	require.NoError(err)

	err = vr.Apply(nil, dummyNode{false})
	require.Error(err)

}

func Test_orderBy(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule("validate_order_by")

	require.Equal(vr.Name, "validate_order_by")

	err := vr.Apply(nil, dummyNode{true})
	require.NoError(err)
	err = vr.Apply(nil, dummyNode{false})
	require.NoError(err)

	err = vr.Apply(nil, plan.NewSort(
		[]plan.SortField{{Column: expression.NewCount(nil), Order: plan.Descending}},
		nil,
	))
	require.Error(err)
}

type dummyNode struct{ resolved bool }

func (n dummyNode) Resolved() bool                             { return n.resolved }
func (dummyNode) Schema() sql.Schema                           { return sql.Schema{} }
func (dummyNode) Children() []sql.Node                         { return nil }
func (dummyNode) RowIter() (sql.RowIter, error)                { return nil, nil }
func (dummyNode) TransformUp(func(sql.Node) sql.Node) sql.Node { return nil }
func (dummyNode) TransformExpressionsUp(
	func(sql.Expression) sql.Expression) sql.Node {
	return nil
}

func getValidationRule(name string) analyzer.ValidationRule {
	for _, rule := range analyzer.DefaultValidationRules {
		if rule.Name == name {
			return rule
		}
	}
	panic("missing rule")
}
