package analyzer_test

import (
	"testing"

	"gopkg.in/sqle/sqle.v0/sql"
	"gopkg.in/sqle/sqle.v0/sql/analyzer"
	"gopkg.in/sqle/sqle.v0/sql/expression"
	"gopkg.in/sqle/sqle.v0/sql/plan"

	"github.com/stretchr/testify/require"
)

func Test_resolved(t *testing.T) {
	assert := require.New(t)

	vr := getValidationRule("validate_resolved")

	assert.Equal(vr.Name, "validate_resolved")

	err := vr.Apply(nil, dummyNode{true})
	assert.NoError(err)

	err = vr.Apply(nil, dummyNode{false})
	assert.Error(err)

}

func Test_orderBy(t *testing.T) {
	assert := require.New(t)

	vr := getValidationRule("validate_order_by")

	assert.Equal(vr.Name, "validate_order_by")

	err := vr.Apply(nil, dummyNode{true})
	assert.NoError(err)
	err = vr.Apply(nil, dummyNode{false})
	assert.NoError(err)

	err = vr.Apply(nil, plan.NewSort(
		[]plan.SortField{{Column: expression.NewCount(nil), Order: plan.Descending}},
		nil,
	))
	assert.Error(err)
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
