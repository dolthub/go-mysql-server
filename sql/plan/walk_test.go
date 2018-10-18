package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestWalk(t *testing.T) {
	t1 := NewUnresolvedTable("foo", "")
	t2 := NewUnresolvedTable("bar", "")
	join := NewCrossJoin(t1, t2)
	filter := NewFilter(nil, join)
	project := NewProject(nil, filter)

	var f visitor
	var visited []sql.Node
	f = func(node sql.Node) Visitor {
		visited = append(visited, node)
		return f
	}

	Walk(f, project)

	require.Equal(t,
		[]sql.Node{project, filter, join, t1, nil, t2, nil, nil, nil, nil},
		visited,
	)

	visited = nil
	f = func(node sql.Node) Visitor {
		visited = append(visited, node)
		if _, ok := node.(*CrossJoin); ok {
			return nil
		}
		return f
	}

	Walk(f, project)

	require.Equal(t,
		[]sql.Node{project, filter, join, nil, nil},
		visited,
	)
}

type visitor func(sql.Node) Visitor

func (f visitor) Visit(n sql.Node) Visitor {
	return f(n)
}

func TestInspect(t *testing.T) {
	t1 := NewUnresolvedTable("foo", "")
	t2 := NewUnresolvedTable("bar", "")
	join := NewCrossJoin(t1, t2)
	filter := NewFilter(nil, join)
	project := NewProject(nil, filter)

	var f func(sql.Node) bool
	var visited []sql.Node
	f = func(node sql.Node) bool {
		visited = append(visited, node)
		return true
	}

	Inspect(project, f)

	require.Equal(t,
		[]sql.Node{project, filter, join, t1, nil, t2, nil, nil, nil, nil},
		visited,
	)

	visited = nil
	f = func(node sql.Node) bool {
		visited = append(visited, node)
		if _, ok := node.(*CrossJoin); ok {
			return false
		}
		return true
	}

	Inspect(project, f)

	require.Equal(t,
		[]sql.Node{project, filter, join, nil, nil},
		visited,
	)
}
