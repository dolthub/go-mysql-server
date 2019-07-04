package sql

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSessionConfig(t *testing.T) {
	require := require.New(t)

	sess := NewSession("foo", "baz", "bar", 1)
	typ, v := sess.Get("foo")
	require.Equal(Null, typ)
	require.Equal(nil, v)

	sess.Set("foo", Int64, 1)

	typ, v = sess.Get("foo")
	require.Equal(Int64, typ)
	require.Equal(1, v)

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
	sess := NewSession("foo", "baz", "bar", 1)

	for key := range DefaultSessionConfig() {
		require.True(HasDefaultValue(sess, key))
	}

	sess.Set("auto_increment_increment", Int64, 123)
	require.False(HasDefaultValue(sess, "auto_increment_increment"))

	require.False(HasDefaultValue(sess, "non_existing_key"))
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

func (t *testNodeIterator) Close() error {
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
