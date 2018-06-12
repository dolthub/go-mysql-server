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
	r.indexOrder = []indexKey{{"foo", ""}}
	r.indexes[indexKey{"foo", ""}] = &dummyIdx{
		database: "foo",
		expr:     []Expression{dummyExpr{1, "2"}},
	}

	idx := r.IndexByExpression("bar", dummyExpr{1, "2"})
	require.Nil(idx)

	idx = r.IndexByExpression("foo", dummyExpr{1, "2"})
	require.NotNil(idx)

	idx = r.IndexByExpression("foo", dummyExpr{2, "3"})
	require.Nil(idx)
}

func TestAddIndex(t *testing.T) {
	require := require.New(t)
	r := NewIndexRegistry()
	idx := &dummyIdx{
		id:       "foo",
		expr:     []Expression{new(dummyExpr)},
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
		expr:     []Expression{new(dummyExpr)},
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

func TestExpressionsWithIndexes(t *testing.T) {
	require := require.New(t)

	r := NewIndexRegistry()

	var indexes = []*dummyIdx{
		{
			"idx1",
			[]Expression{
				&dummyExpr{0, "foo"},
				&dummyExpr{1, "bar"},
			},
			"foo",
			"foo",
		},
		{
			"idx2",
			[]Expression{
				&dummyExpr{0, "foo"},
				&dummyExpr{1, "bar"},
				&dummyExpr{3, "baz"},
			},
			"foo",
			"foo",
		},
		{
			"idx3",
			[]Expression{
				&dummyExpr{0, "foo"},
			},
			"foo",
			"foo",
		},
	}

	for _, idx := range indexes {
		done, err := r.AddIndex(idx)
		require.NoError(err)
		close(done)
	}

	time.Sleep(50 * time.Millisecond)

	exprs := r.ExpressionsWithIndexes(
		"foo",
		&dummyExpr{0, "foo"},
		&dummyExpr{1, "bar"},
		&dummyExpr{3, "baz"},
	)

	expected := [][]Expression{
		{
			&dummyExpr{0, "foo"},
			&dummyExpr{1, "bar"},
			&dummyExpr{3, "baz"},
		},
		{
			&dummyExpr{0, "foo"},
			&dummyExpr{1, "bar"},
		},
	}

	require.ElementsMatch(expected, exprs)
}

type dummyIdx struct {
	id       string
	expr     []Expression
	database string
	table    string
}

var _ Index = (*dummyIdx)(nil)

func (i dummyIdx) ExpressionHashes() []ExpressionHash {
	var hashes []ExpressionHash
	for _, e := range i.expr {
		h := sha1.New()
		h.Write([]byte(e.String()))
		hashes = append(hashes, h.Sum(nil))
	}
	return hashes
}
func (i dummyIdx) ID() string                              { return i.id }
func (i dummyIdx) Get(...interface{}) (IndexLookup, error) { panic("not implemented") }
func (i dummyIdx) Has(...interface{}) (bool, error)        { panic("not implemented") }
func (i dummyIdx) Database() string                        { return i.database }
func (i dummyIdx) Table() string                           { return i.table }

type dummyExpr struct {
	index   int
	colName string
}

var _ Expression = (*dummyExpr)(nil)

func (dummyExpr) Children() []Expression                  { return nil }
func (dummyExpr) Eval(*Context, Row) (interface{}, error) { panic("not implemented") }
func (e dummyExpr) TransformUp(fn TransformExprFunc) (Expression, error) {
	return fn(e)
}
func (e dummyExpr) String() string { return fmt.Sprintf("dummyExpr{%d, %s}", e.index, e.colName) }
func (dummyExpr) IsNullable() bool { return false }
func (dummyExpr) Resolved() bool   { return false }
func (dummyExpr) Type() Type       { panic("not implemented") }
func (e dummyExpr) WithIndex(idx int) Expression {
	return &dummyExpr{idx, e.colName}
}
