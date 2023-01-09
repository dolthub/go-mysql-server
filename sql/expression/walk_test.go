// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestWalk(t *testing.T) {
	lit1 := NewLiteral(1, types.Int64)
	lit2 := NewLiteral(2, types.Int64)
	col := NewUnresolvedColumn("foo")
	fn := NewUnresolvedFunction(
		"bar",
		false,
		nil,
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
		[]sql.Expression{e, and, col, fn, lit1, lit2},
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
		[]sql.Expression{e, and, col, fn},
		visited,
	)
}

type visitor func(sql.Expression) sql.Visitor

func (f visitor) Visit(n sql.Expression) sql.Visitor {
	return f(n)
}

func TestInspect(t *testing.T) {
	lit1 := NewLiteral(1, types.Int64)
	lit2 := NewLiteral(2, types.Int64)
	col := NewUnresolvedColumn("foo")
	fn := NewUnresolvedFunction(
		"bar",
		false,
		nil,
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
		[]sql.Expression{e, and, col, fn, lit1, lit2},
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
		[]sql.Expression{e, and, col, fn},
		visited,
	)
}
