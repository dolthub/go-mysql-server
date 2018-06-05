package sql

import (
	"crypto/sha1"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIndexByExpression(t *testing.T) {
	require := require.New(t)

	r := NewIndexRegistry()
	r.indexes[indexKey{"foo", ""}] = &dummyIdx{
		database: "foo",
		expr:     dummyExpr{1, "2"},
	}

	idx := r.IndexByExpression("bar", dummyExpr{1, "2"})
	require.Nil(idx)

	idx = r.IndexByExpression("foo", dummyExpr{1, "2"})
	require.NotNil(idx)

	idx = r.IndexByExpression("foo", dummyExpr{2, "2"})
	require.Nil(idx)
}

func TestAddIndex(t *testing.T) {
	require := require.New(t)
	r := NewIndexRegistry()
	idx := &dummyIdx{
		id:       "foo",
		expr:     new(dummyExpr),
		database: "foo",
		table:    "foo",
	}

	done, err := r.AddIndex(idx)
	require.NoError(err)

	i := r.Index("foo", "foo")
	require.False(r.CanUseIndex(i))

	done <- struct{}{}

	<-time.After(25 * time.Millisecond)
	i = r.Index("foo", "foo")
	require.True(r.CanUseIndex(i))

	_, err = r.AddIndex(idx)
	require.Error(err)
	require.True(ErrIndexIDAlreadyRegistered.Is(err))

	_, err = r.AddIndex(&dummyIdx{
		id:       "another",
		expr:     new(dummyExpr),
		database: "foo",
		table:    "foo",
	})
	require.Error(err)
	require.True(ErrIndexExpressionAlreadyRegistered.Is(err))
}

func TestDeleteIndex(t *testing.T) {
	require := require.New(t)
	r := NewIndexRegistry()

	idx := &dummyIdx{"foo", nil, "foo", "foo"}
	r.indexes[indexKey{"foo", "foo"}] = idx

	_, err := r.DeleteIndex("foo", "foo")
	require.Error(err)
	require.True(ErrIndexDeleteInvalidStatus.Is(err))

	r.setStatus(idx, IndexReady)

	_, err = r.DeleteIndex("foo", "foo")
	require.NoError(err)

	require.Len(r.indexes, 0)
}

func TestDeleteIndex_InUse(t *testing.T) {
	require := require.New(t)
	r := NewIndexRegistry()
	idx := &dummyIdx{
		"foo", nil, "foo", "foo",
	}
	r.indexes[indexKey{"foo", "foo"}] = idx
	r.setStatus(idx, IndexReady)
	r.retainIndex("foo", "foo")

	done, err := r.DeleteIndex("foo", "foo")
	require.NoError(err)

	require.Len(r.indexes, 1)
	require.False(r.CanUseIndex(idx))

	go func() {
		r.ReleaseIndex(idx)
	}()

	<-done
	require.Len(r.indexes, 0)
}

type dummyIdx struct {
	id       string
	expr     Expression
	database string
	table    string
}

var _ Index = (*dummyIdx)(nil)

func (i dummyIdx) ExpressionHashes() []ExpressionHash {
	h := sha1.New()
	h.Write([]byte(i.expr.String()))
	return []ExpressionHash{h.Sum(nil)}
}
func (i dummyIdx) ID() string                              { return i.id }
func (i dummyIdx) Get(...interface{}) (IndexLookup, error) { panic("not implemented") }
func (i dummyIdx) Has(...interface{}) (bool, error)        { panic("not implemented") }
func (i dummyIdx) Database() string                        { return i.database }
func (i dummyIdx) Table() string                           { return i.table }

type dummyExpr struct {
	foo int
	bar string
}

var _ Expression = (*dummyExpr)(nil)

func (dummyExpr) Children() []Expression                               { return nil }
func (dummyExpr) Eval(*Context, Row) (interface{}, error)              { panic("not implemented") }
func (dummyExpr) TransformUp(fn TransformExprFunc) (Expression, error) { panic("not implemented") }
func (d dummyExpr) String() string {
	return fmt.Sprintf("dummyExpr{foo: %d, bar: %s}", d.foo, d.bar)
}
func (dummyExpr) IsNullable() bool { return false }
func (dummyExpr) Resolved() bool   { return false }
func (dummyExpr) Type() Type       { panic("not implemented") }
