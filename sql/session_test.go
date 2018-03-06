package sql

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type testNode struct{}

func (t *testNode) Resolved() bool {
	panic("not implemented")
}

func (t *testNode) TransformUp(func(Node) Node) Node {
	panic("not implemented")
}

func (t *testNode) TransformExpressionsUp(func(Expression) Expression) Node {
	panic("not implemented")
}

func (t *testNode) Schema() Schema {
	panic("not implemented")
}

func (t *testNode) Children() []Node {
	panic("not implemented")
}

func (t *testNode) RowIter(session Session) (RowIter, error) {
	return newTestNodeIterator(session), nil
}

type testNodeIterator struct {
	session context.Context
	Counter int
}

func newTestNodeIterator(session Session) RowIter {
	return &testNodeIterator{
		session: session,
		Counter: 0,
	}
}

func (t *testNodeIterator) Next() (Row, error) {
	select {
	case <-t.session.Done():
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
	session := NewBaseSession(ctx)

	node := &testNode{}
	iter, err := node.RowIter(session)
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
