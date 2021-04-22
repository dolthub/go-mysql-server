// Copyright 2021 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// AlterDefaultSet represents the ALTER COLUMN SET DEFAULT statement.
type AlterDefaultSet struct {
	UnaryNode
	ColumnName string
	Default    *sql.ColumnDefaultValue
}

var _ sql.Node = (*AlterDefaultSet)(nil)
var _ sql.Expressioner = (*AlterDefaultSet)(nil)

// AlterDefaultDrop represents the ALTER COLUMN DROP DEFAULT statement.
type AlterDefaultDrop struct {
	UnaryNode
	ColumnName string
}

var _ sql.Node = (*AlterDefaultDrop)(nil)

func getAlterable(node sql.Node) (sql.AlterableTable, error) {
	switch node := node.(type) {
	case sql.AlterableTable:
		return node, nil
	case *IndexedTableAccess:
		return getAlterable(node.ResolvedTable)
	case *ResolvedTable:
		return getAlterableTableUnderlying(node.Table)
	case sql.TableWrapper:
		return getAlterableTableUnderlying(node.Underlying())
	}
	for _, child := range node.Children() {
		deleter, _ := getAlterable(child)
		if deleter != nil {
			return deleter, nil
		}
	}
	return nil, sql.ErrAlterTableNotSupported.New()
}

func getAlterableTableUnderlying(t sql.Table) (sql.AlterableTable, error) {
	switch t := t.(type) {
	case sql.AlterableTable:
		return t, nil
	case sql.TableWrapper:
		return getAlterableTableUnderlying(t.Underlying())
	default:
		return nil, sql.ErrAlterTableNotSupported.New()
	}
}

// NewAlterDefaultSet returns a *AlterDefaultSet node.
func NewAlterDefaultSet(table sql.Node, columnName string, defVal *sql.ColumnDefaultValue) *AlterDefaultSet {
	return &AlterDefaultSet{
		UnaryNode:  UnaryNode{table},
		ColumnName: columnName,
		Default:    defVal,
	}
}

// String implements the sql.Node interface.
func (d *AlterDefaultSet) String() string {
	return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s", d.UnaryNode.Child.String(), d.ColumnName, d.Default.String())
}

// RowIter implements the sql.Node interface.
func (d *AlterDefaultSet) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	alterable, err := getAlterable(d.Child)
	if err != nil {
		return nil, err
	}
	loweredColName := strings.ToLower(d.ColumnName)
	var col *sql.Column
	for _, schCol := range alterable.Schema() {
		if strings.ToLower(schCol.Name) == loweredColName {
			col = schCol
			break
		}
	}
	if col == nil {
		return nil, sql.ErrTableColumnNotFound.New(d.Child.String(), d.ColumnName)
	}
	newCol := &(*col)
	newCol.Default = d.Default
	return sql.RowsToRowIter(), alterable.ModifyColumn(ctx, d.ColumnName, newCol, nil)
}

// WithChildren implements the sql.Node interface.
func (d *AlterDefaultSet) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	return NewAlterDefaultSet(children[0], d.ColumnName, d.Default), nil
}

// Resolved implements the sql.Node interface.
func (d *AlterDefaultSet) Resolved() bool {
	return d.UnaryNode.Resolved() && d.Default.Resolved()
}

// Expressions implements the sql.Expressioner interface.
func (d *AlterDefaultSet) Expressions() []sql.Expression {
	return expression.WrapExpressions(d.Default)
}

// WithExpressions implements the sql.Expressioner interface.
func (d *AlterDefaultSet) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(exprs), 1)
	}
	nd := *d
	unwrappedColDefVal, ok := exprs[0].(*expression.Wrapper).Unwrap().(*sql.ColumnDefaultValue)
	if ok {
		nd.Default = unwrappedColDefVal
	} else { // nil fails type check
		nd.Default = nil
	}
	return &nd, nil
}

// NewAlterDefaultDrop returns a *AlterDefaultDrop node.
func NewAlterDefaultDrop(table sql.Node, columnName string) *AlterDefaultDrop {
	return &AlterDefaultDrop{
		UnaryNode:  UnaryNode{table},
		ColumnName: columnName,
	}
}

// String implements the sql.Node interface.
func (d *AlterDefaultDrop) String() string {
	return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT", d.UnaryNode.Child.String(), d.ColumnName)
}

// RowIter implements the sql.Node interface.
func (d *AlterDefaultDrop) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	alterable, err := getAlterable(d.Child)
	if err != nil {
		return nil, err
	}
	loweredColName := strings.ToLower(d.ColumnName)
	var col *sql.Column
	for _, schCol := range alterable.Schema() {
		if strings.ToLower(schCol.Name) == loweredColName {
			col = schCol
			break
		}
	}
	if col == nil {
		return nil, sql.ErrTableColumnNotFound.New(d.Child.String(), d.ColumnName)
	}
	newCol := &(*col)
	newCol.Default = nil
	return sql.RowsToRowIter(), alterable.ModifyColumn(ctx, d.ColumnName, newCol, nil)
}

// WithChildren implements the sql.Node interface.
func (d *AlterDefaultDrop) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	return NewAlterDefaultDrop(children[0], d.ColumnName), nil
}
