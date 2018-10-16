package plan

import (
	"testing"

	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"

	"github.com/stretchr/testify/require"
)

func TestTransformUp(t *testing.T) {
	require := require.New(t)

	aCol := expression.NewUnresolvedColumn("a")
	bCol := expression.NewUnresolvedColumn("a")
	ur := NewUnresolvedTable("unresolved", "")
	p := NewProject([]sql.Expression{aCol, bCol}, NewFilter(expression.NewEquals(aCol, bCol), ur))

	schema := sql.Schema{
		{Name: "a", Type: sql.Text},
		{Name: "b", Type: sql.Text},
	}
	table := mem.NewTable("resolved", schema)

	pt, err := p.TransformUp(func(n sql.Node) (sql.Node, error) {
		switch n.(type) {
		case *UnresolvedTable:
			return NewResolvedTable(table), nil
		default:
			return n, nil
		}
	})
	require.NoError(err)

	ep := NewProject(
		[]sql.Expression{aCol, bCol},
		NewFilter(expression.NewEquals(aCol, bCol),
			NewResolvedTable(table),
		),
	)
	require.Equal(ep, pt)
}
