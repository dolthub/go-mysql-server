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

package analyzer

import (
	"reflect"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
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

func (r *Releaser) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	iter, err := r.Child.RowIter(ctx, row)
	if err != nil {
		r.Release()
		return nil, err
	}

	return &releaseIter{ctx: ctx, child: iter, release: r.Release}, nil
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
	ctx     *sql.Context
	child   sql.RowIter
	release func()
	once    sync.Once
}

func (i *releaseIter) Next() (sql.Row, error) {
	row, err := i.child.Next()
	if err != nil {
		_ = i.Close(i.ctx)
		return nil, err
	}
	return row, nil
}

func (i *releaseIter) Close(ctx *sql.Context) (err error) {
	i.once.Do(i.release)
	if i.child != nil {
		err = i.child.Close(ctx)
	}
	return err
}
