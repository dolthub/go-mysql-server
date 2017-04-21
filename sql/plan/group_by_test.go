package plan

import (
	"testing"

	"gopkg.in/sqle/sqle.v0/mem"
	"gopkg.in/sqle/sqle.v0/sql"
	"gopkg.in/sqle/sqle.v0/sql/expression"

	"github.com/stretchr/testify/assert"
)

func TestGroupBy_Schema(t *testing.T) {
	assert := assert.New(t)

	child := mem.NewTable("test", sql.Schema{})
	agg := []sql.Expression{
		expression.NewAlias(expression.NewLiteral("s", sql.String), "c1"),
		expression.NewAlias(expression.NewCount(expression.NewStar()), "c2"),
	}
	gb := NewGroupBy(agg, nil, child)
	assert.Equal(sql.Schema{
		{Name: "c1", Type: sql.String},
		{Name: "c2", Type: sql.Integer},
	}, gb.Schema())
}

func TestGroupBy_Resolved(t *testing.T) {
	assert := assert.New(t)

	child := mem.NewTable("test", sql.Schema{})
	agg := []sql.Expression{
		expression.NewAlias(expression.NewCount(expression.NewStar()), "c2"),
	}
	gb := NewGroupBy(agg, nil, child)
	assert.True(gb.Resolved())

	agg = []sql.Expression{
		expression.NewStar(),
	}
	gb = NewGroupBy(agg, nil, child)
	assert.False(gb.Resolved())
}

func TestGroupBy_RowIter(t *testing.T) {
	assert := assert.New(t)
	childSchema := sql.Schema{
		{Name: "col1", Type: sql.String},
		{Name: "col2", Type: sql.BigInteger},
	}
	child := mem.NewTable("test", childSchema)
	child.Insert(sql.NewRow("col1_1", int64(1111)))
	child.Insert(sql.NewRow("col1_1", int64(1111)))
	child.Insert(sql.NewRow("col1_2", int64(4444)))
	child.Insert(sql.NewRow("col1_1", int64(1111)))
	child.Insert(sql.NewRow("col1_2", int64(4444)))

	p := NewSort(
		[]SortField{
			{
				Column: expression.NewGetField(0, sql.String, "col1", true),
				Order:  Ascending,
			}, {
				Column: expression.NewGetField(1, sql.BigInteger, "col2", true),
				Order:  Ascending,
			},
		},
		NewGroupBy(
			[]sql.Expression{
				expression.NewGetField(0, sql.String, "col1", true),
				expression.NewGetField(1, sql.BigInteger, "col2", true),
			},
			[]sql.Expression{
				expression.NewGetField(0, sql.String, "col1", true),
				expression.NewGetField(1, sql.BigInteger, "col2", true),
			},
			child,
		))

	assert.Equal(1, len(p.Children()))

	rows, err := sql.NodeToRows(p)
	assert.NoError(err)
	assert.Len(rows, 2)

	assert.Equal(sql.NewRow("col1_1", int64(1111)), rows[0])
	assert.Equal(sql.NewRow("col1_2", int64(4444)), rows[1])
}
