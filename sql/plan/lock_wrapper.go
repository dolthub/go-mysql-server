// Copyright 2022 Dolthub, Inc.
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

package plan

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer/locks"
)

type LockWrapper struct {
	UnaryNode
	LockManager locks.LockManager // probs an import loop
	TableName   string
}

func NewLockWrapper(child sql.Node) *LockWrapper {
	return &LockWrapper{UnaryNode: UnaryNode{Child: child}}
}

func (l *LockWrapper) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	tableName := getTableName(l.Child) // gets the name of the child. Typically is a row source
	if l.TableName != "" {
		tableName = l.TableName
	}

	ri, err := l.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return newLockedWrappedIter(ctx, tableName, ri, l.LockManager)
}

type wrappedIter struct {
	ri          sql.RowIter
	LockManager locks.LockManager
}

var _ sql.RowIter = (*wrappedIter)(nil)

func newLockedWrappedIter(ctx *sql.Context, tableName string, ri sql.RowIter, lm locks.LockManager) (*wrappedIter, error) {
	err := lm.LockTable(ctx, ctx.GetCurrentDatabase(), tableName)
	if err != nil {
		return nil, err
	}

	return &wrappedIter{
		ri:          ri,
		LockManager: lm,
	}, nil
}

func (w wrappedIter) Next(context *sql.Context) (sql.Row, error) {
	return w.ri.Next(context)
}

func (w wrappedIter) Close(context *sql.Context) error {
	return w.ri.Close(context)
}

func (l *LockWrapper) WithChildren(node ...sql.Node) (sql.Node, error) {
	if len(node) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(node), 1)
	}

	l.UnaryNode.Child = node[0]
	return l, nil
}

func (l *LockWrapper) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return l.Child.CheckPrivileges(ctx, opChecker)
}

func (l *LockWrapper) String() string {
	return fmt.Sprintf("LOCKWrapper(%s)", l.Child.String())
}

func (l *LockWrapper) WithLockManager(lm locks.LockManager) *LockWrapper {
	nc := *l
	nc.LockManager = lm
	return &nc
}

func (l *LockWrapper) WithTableName(tableName string) *LockWrapper {
	nc := *l
	nc.TableName = tableName
	return &nc
}

var _ sql.Node = (*LockWrapper)(nil)
