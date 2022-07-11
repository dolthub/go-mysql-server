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

package sql

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSessionConfig(t *testing.T) {
	require := require.New(t)
	ctx := NewEmptyContext()

	sess := NewBaseSessionWithClientServer("foo", Client{Address: "baz", User: "bar"}, 1)
	typ, v, err := sess.GetUserVariable(ctx, "foo")
	require.NoError(err)
	require.Equal(Null, typ)
	require.Equal(nil, v)

	err = sess.SetUserVariable(ctx, "foo", int64(1))
	require.NoError(err)

	typ, v, err = sess.GetUserVariable(ctx, "foo")
	require.NoError(err)
	require.Equal(Int64, typ)
	require.Equal(int64(1), v)

	require.Equal(uint16(0), sess.WarningCount())

	sess.Warn(&Warning{Code: 1})
	sess.Warn(&Warning{Code: 2})
	sess.Warn(&Warning{Code: 3})

	require.Equal(uint16(3), sess.WarningCount())

	require.Equal(3, sess.Warnings()[0].Code)
	require.Equal(2, sess.Warnings()[1].Code)
	require.Equal(1, sess.Warnings()[2].Code)
}

func TestHasDefaultValue(t *testing.T) {
	require := require.New(t)
	ctx := NewEmptyContext()
	sess := NewBaseSessionWithClientServer("foo", Client{Address: "baz", User: "bar"}, 1)

	err := sess.SetSessionVariable(ctx, "auto_increment_increment", 123)
	require.NoError(err)
	require.False(HasDefaultValue(ctx, sess, "auto_increment_increment"))
	require.True(HasDefaultValue(ctx, sess, "non_existing_key")) // Returns true for non-existent keys
}

func TestInitReadonlySessionVariable(t *testing.T) {
	const readonlyVariable = "external_user"
	const variableValue = "aoeu"

	require := require.New(t)
	ctx := NewEmptyContext()
	sess := NewBaseSessionWithClientServer("foo", Client{Address: "baz", User: "bar"}, 1)

	err := sess.SetSessionVariable(ctx, readonlyVariable, variableValue)
	require.Error(err)

	val, err := sess.GetSessionVariable(ctx, readonlyVariable)
	require.NoError(err)
	require.NotEqual(variableValue, val.(string))

	err = sess.InitSessionVariable(ctx, readonlyVariable, variableValue)
	require.NoError(err)

	val, err = sess.GetSessionVariable(ctx, readonlyVariable)
	require.NoError(err)
	require.Equal(variableValue, val.(string))

	err = sess.InitSessionVariable(ctx, readonlyVariable, variableValue)
	require.Error(err)
	require.True(ErrSystemVariableReinitialized.Is(err))
}

type testNode struct{}

func (*testNode) Resolved() bool {
	panic("not implemented")
}
func (*testNode) WithChildren(...Node) (Node, error) {
	panic("not implemented")
}

func (*testNode) Schema() Schema {
	panic("not implemented")
}

func (*testNode) Children() []Node {
	panic("not implemented")
}

func (*testNode) RowIter(ctx *Context) (RowIter, error) {
	return newTestNodeIterator(), nil
}

type testNodeIterator struct {
	Counter int
}

func newTestNodeIterator() RowIter {
	return &testNodeIterator{
		Counter: 0,
	}
}

func (t *testNodeIterator) Next(ctx *Context) (Row, error) {
	select {
	case <-ctx.Done():
		return nil, io.EOF

	default:
		t.Counter++
		return NewRow(true), nil
	}
}

func (t *testNodeIterator) Close(*Context) error {
	panic("not implemented")
}

func TestSessionIterator(t *testing.T) {
	require := require.New(t)
	octx, cancelFunc := context.WithCancel(context.TODO())
	defer cancelFunc()
	ctx := NewContext(octx)

	node := &testNode{}
	iter, err := node.RowIter(ctx)
	require.NoError(err)

	counter := 0
	for {
		if counter > 5 {
			cancelFunc()
		}

		_, err := iter.Next(ctx)

		if counter > 5 {
			require.Equal(io.EOF, err)
			rowIter, ok := iter.(*testNodeIterator)
			require.True(ok)

			require.Equal(counter, rowIter.Counter)
			break
		}

		counter++
	}
}
