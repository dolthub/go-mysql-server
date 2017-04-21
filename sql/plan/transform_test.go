package plan

import (
	"testing"

	"gopkg.in/sqle/sqle.v0/mem"
	"gopkg.in/sqle/sqle.v0/sql"
	"gopkg.in/sqle/sqle.v0/sql/expression"

	"github.com/stretchr/testify/require"
)

func TestTransformUp(t *testing.T) {
	require := require.New(t)

	aCol := expression.NewUnresolvedColumn("a")
	bCol := expression.NewUnresolvedColumn("a")
	ur := &UnresolvedTable{"unresolved"}
	p := NewProject([]sql.Expression{aCol, bCol}, NewFilter(expression.NewEquals(aCol, bCol), ur))

	schema := sql.Schema{
		{Name: "a", Type: sql.String},
		{Name: "b", Type: sql.String},
	}
	table := mem.NewTable("resolved", schema)

	pt := p.TransformUp(func(n sql.Node) sql.Node {
		switch n.(type) {
		case *UnresolvedTable:
			return table
		default:
			return n
		}
	})

	ep := NewProject([]sql.Expression{aCol, bCol}, NewFilter(expression.NewEquals(aCol, bCol), table))
	require.Equal(ep, pt)
}
