package plan

import (
	"fmt"
	"testing"

	"github.com/mvader/gitql/mem"
	"github.com/mvader/gitql/sql"
	"github.com/mvader/gitql/sql/expression"
	"github.com/stretchr/testify/require"
)

func TestTransform(t *testing.T) {
	require := require.New(t)

	aCol := expression.NewUnresolvedColumn("a")
	bCol := expression.NewUnresolvedColumn("a")
	ur := UnresolvedRelation{"unresolved"}
	p := NewProject([]sql.Expression{aCol, bCol}, NewFilter(expression.NewEquals(aCol, bCol), ur))

	schema := sql.Schema{
		sql.Field{"a", sql.String},
		sql.Field{"b", sql.String},
	}
	table := mem.NewTable("resolved", schema)

	pt := p.TransformUp(func(n sql.Node) sql.Node {
		switch t := n.(type) {
		case *UnresolvedRelation:
			return table
		default:
			fmt.Printf("unexpected type %T\n", t)
			return n
		}
	})

	ep := NewProject([]sql.Expression{aCol, bCol}, NewFilter(expression.NewEquals(aCol, bCol), table))
	require.Equal(ep, pt)
}
