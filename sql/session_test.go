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

	sess := NewSession("foo", Client{Address: "baz", User: "bar"}, 1)
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
	sess := NewSession("foo", Client{Address: "baz", User: "bar"}, 1)

	err := sess.SetSessionVariable(NewEmptyContext(), "auto_increment_increment", 123)
	require.NoError(err)
	require.False(HasDefaultValue(ctx, sess, "auto_increment_increment"))
	require.True(HasDefaultValue(ctx, sess, "non_existing_key")) // Returns true for non-existent keys
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
	return newTestNodeIterator(ctx), nil
}

type testNodeIterator struct {
	ctx     context.Context
	Counter int
}

func newTestNodeIterator(ctx *Context) RowIter {
	return &testNodeIterator{
		ctx:     ctx,
		Counter: 0,
	}
}

func (t *testNodeIterator) Next() (Row, error) {
	select {
	case <-t.ctx.Done():
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
	ctx, cancelFunc := context.WithCancel(context.TODO())

	node := &testNode{}
	iter, err := node.RowIter(NewContext(ctx))
	require.NoError(err)

	counter := 0
	for {
		if counter > 5 {
			cancelFunc()
		}

		_, err := iter.Next()

		if counter > 5 {
			require.Equal(io.EOF, err)
			rowIter, ok := iter.(*testNodeIterator)
			require.True(ok)

			require.Equal(counter, rowIter.Counter)
			break
		}

		counter++
	}

	cancelFunc()
}
