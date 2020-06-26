package expression

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

func TestWalk(t *testing.T) {
	lit1 := NewLiteral(1, sql.Int64)
	lit2 := NewLiteral(2, sql.Int64)
	col := NewUnresolvedColumn("foo")
	fn := NewUnresolvedFunction(
		"bar",
		false,
		lit1,
		lit2,
	)
	and := NewAnd(col, fn)
	e := NewNot(and)

	var f visitor
	var visited []sql.Expression
	f = func(node sql.Expression) sql.Visitor {
		visited = append(visited, node)
		return f
	}

	sql.Walk(f, e)

	require.Equal(t,
		[]sql.Expression{e, and, col, nil, fn, lit1, nil, lit2, nil, nil, nil, nil},
		visited,
	)

	visited = nil
	f = func(node sql.Expression) sql.Visitor {
		visited = append(visited, node)
		if _, ok := node.(*UnresolvedFunction); ok {
			return nil
		}
		return f
	}

	sql.Walk(f, e)

	require.Equal(t,
		[]sql.Expression{e, and, col, nil, fn, nil, nil},
		visited,
	)
}

type visitor func(sql.Expression) sql.Visitor

func (f visitor) Visit(n sql.Expression) sql.Visitor {
	return f(n)
}

func TestInspect(t *testing.T) {
	lit1 := NewLiteral(1, sql.Int64)
	lit2 := NewLiteral(2, sql.Int64)
	col := NewUnresolvedColumn("foo")
	fn := NewUnresolvedFunction(
		"bar",
		false,
		lit1,
		lit2,
	)
	and := NewAnd(col, fn)
	e := NewNot(and)

	var f func(sql.Expression) bool
	var visited []sql.Expression
	f = func(node sql.Expression) bool {
		visited = append(visited, node)
		return true
	}

	sql.Inspect(e, f)

	require.Equal(t,
		[]sql.Expression{e, and, col, nil, fn, lit1, nil, lit2, nil, nil, nil, nil},
		visited,
	)

	visited = nil
	f = func(node sql.Expression) bool {
		visited = append(visited, node)
		if _, ok := node.(*UnresolvedFunction); ok {
			return false
		}
		return true
	}

	sql.Inspect(e, f)

	require.Equal(t,
		[]sql.Expression{e, and, col, nil, fn, nil, nil},
		visited,
	)
}
