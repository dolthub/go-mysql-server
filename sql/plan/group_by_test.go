package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestGroupBy_Schema(t *testing.T) {
	require := require.New(t)

	child := mem.NewTable("test", sql.Schema{})
	agg := []sql.Expression{
		expression.NewAlias(expression.NewLiteral("s", sql.Text), "c1"),
		expression.NewAlias(expression.NewCount(expression.NewStar()), "c2"),
	}
	gb := NewGroupBy(agg, nil, child)
	require.Equal(sql.Schema{
		{Name: "c1", Type: sql.Text},
		{Name: "c2", Type: sql.Int32},
	}, gb.Schema())
}

func TestGroupBy_Resolved(t *testing.T) {
	require := require.New(t)

	child := mem.NewTable("test", sql.Schema{})
	agg := []sql.Expression{
		expression.NewAlias(expression.NewCount(expression.NewStar()), "c2"),
	}
	gb := NewGroupBy(agg, nil, child)
	require.True(gb.Resolved())

	agg = []sql.Expression{
		expression.NewStar(),
	}
	gb = NewGroupBy(agg, nil, child)
	require.False(gb.Resolved())
}

func TestGroupBy_RowIter(t *testing.T) {
	require := require.New(t)
	session := sql.NewBaseSession(context.TODO())

	childSchema := sql.Schema{
		{Name: "col1", Type: sql.Text},
		{Name: "col2", Type: sql.Int64},
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
				Column: expression.NewGetField(0, sql.Text, "col1", true),
				Order:  Ascending,
			}, {
				Column: expression.NewGetField(1, sql.Int64, "col2", true),
				Order:  Ascending,
			},
		},
		NewGroupBy(
			[]sql.Expression{
				expression.NewGetField(0, sql.Text, "col1", true),
				expression.NewGetField(1, sql.Int64, "col2", true),
			},
			[]sql.Expression{
				expression.NewGetField(0, sql.Text, "col1", true),
				expression.NewGetField(1, sql.Int64, "col2", true),
			},
			child,
		))

	require.Equal(1, len(p.Children()))

	rows, err := sql.NodeToRows(session, p)
	require.NoError(err)
	require.Len(rows, 2)

	require.Equal(sql.NewRow("col1_1", int64(1111)), rows[0])
	require.Equal(sql.NewRow("col1_2", int64(4444)), rows[1])
}

func TestGroupBy_Error(t *testing.T) {
	require := require.New(t)
	session := sql.NewBaseSession(context.TODO())

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

	p := NewGroupBy(
		[]sql.Expression{
			expression.NewCount(expression.NewGetField(0, sql.Text, "col1", true)),
			expression.NewIsNull(expression.NewGetField(1, sql.Int64, "col2", true)),
		},
		[]sql.Expression{
			expression.NewCount(expression.NewGetField(0, sql.Text, "col1", true)),
			expression.NewGetField(1, sql.Int64, "col2", true),
		},
		child,
	)

	_, err := sql.NodeToRows(session, p)
	require.Error(err)
}
