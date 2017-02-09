package plan

import (
	"testing"

	"github.com/gitql/gitql/mem"
	"github.com/gitql/gitql/sql"
	"github.com/gitql/gitql/sql/expression"

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
		sql.Column{Name: "c1", Type: sql.String},
		sql.Column{Name: "c2", Type: sql.Integer},
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
		sql.Column{"col1", sql.String},
		sql.Column{"col2", sql.BigInteger},
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
				Column: expression.NewGetField(0, sql.String, "col1"),
				Order:  Ascending,
			}, {
				Column: expression.NewGetField(1, sql.BigInteger, "col2"),
				Order:  Ascending,
			},
		},
		NewGroupBy(
			[]sql.Expression{
				expression.NewGetField(0, sql.String, "col1"),
				expression.NewGetField(1, sql.BigInteger, "col2"),
			},
			[]sql.Expression{
				expression.NewGetField(0, sql.String, "col1"),
				expression.NewGetField(1, sql.BigInteger, "col2"),
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
