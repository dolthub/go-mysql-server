package analyzer

import (
	"github.com/liquidata-inc/go-mysql-server/sql"
	"reflect"
	"sync"
)

type Releaser struct {
	Child   sql.Node
	Release func()
}

func (r *Releaser) Resolved() bool {
	return r.Child.Resolved()
}

func (r *Releaser) Children() []sql.Node {
	return []sql.Node{r.Child}
}

func (r *Releaser) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	iter, err := r.Child.RowIter(ctx)
	if err != nil {
		r.Release()
		return nil, err
	}

	return &releaseIter{child: iter, release: r.Release}, nil
}

func (r *Releaser) Schema() sql.Schema {
	return r.Child.Schema()
}

func (r *Releaser) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 1)
	}
	return &Releaser{children[0], r.Release}, nil
}

func (r *Releaser) String() string {
	return r.Child.String()
}

func (r *Releaser) Equal(n sql.Node) bool {
	if r2, ok := n.(*Releaser); ok {
		return reflect.DeepEqual(r.Child, r2.Child)
	}
	return false
}

type releaseIter struct {
	child   sql.RowIter
	release func()
	once    sync.Once
}

func (i *releaseIter) Next() (sql.Row, error) {
	row, err := i.child.Next()
	if err != nil {
		_ = i.Close()
		return nil, err
	}
	return row, nil
}

func (i *releaseIter) Close() (err error) {
	i.once.Do(i.release)
	if i.child != nil {
		err = i.child.Close()
	}
	return err
}

func (i *releaseIter) RowCompareFunc(sch sql.Schema) (sql.RowCompareFunc, error) {
	ordIter, ok := i.child.(sql.OrderableRowIter)
	if !ok {
		return nil, sql.ErrIterUnorderable.New()
	}
	return ordIter.RowCompareFunc(sch)
}
