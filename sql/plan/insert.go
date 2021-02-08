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

package plan

import (
	"fmt"
	"io"
	"strings"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
)

// ErrInsertIntoNotSupported is thrown when a table doesn't support inserts
var ErrInsertIntoNotSupported = errors.NewKind("table doesn't support INSERT INTO")
var ErrReplaceIntoNotSupported = errors.NewKind("table doesn't support REPLACE INTO")
var ErrOnDuplicateKeyUpdateNotSupported = errors.NewKind("table doesn't support ON DUPLICATE KEY UPDATE")
var ErrAutoIncrementNotSupported = errors.NewKind("table doesn't support AUTO_INCREMENT")
var ErrInsertIntoMismatchValueCount = errors.NewKind("number of values does not match number of columns provided")
var ErrInsertIntoUnsupportedValues = errors.NewKind("%T is unsupported for inserts")
var ErrInsertIntoDuplicateColumn = errors.NewKind("duplicate column name %v")
var ErrInsertIntoNonexistentColumn = errors.NewKind("invalid column name %v")
var ErrInsertIntoNonNullableDefaultNullColumn = errors.NewKind("column name '%v' is non-nullable but attempted to set default value of null")
var ErrInsertIntoNonNullableProvidedNull = errors.NewKind("column name '%v' is non-nullable but attempted to set a value of null")
var ErrInsertIntoIncompatibleTypes = errors.NewKind("cannot convert type %s to %s")

// InsertInto is a node describing the insertion into some table.
type InsertInto struct {
	Destination sql.Node
	Source      sql.Node
	ColumnNames []string
	IsReplace   bool
	OnDupExprs  []sql.Expression
}

// NewInsertInto creates an InsertInto node.
func NewInsertInto(dst, src sql.Node, isReplace bool, cols []string, onDupExprs []sql.Expression) *InsertInto {
	return &InsertInto{
		Destination: dst,
		Source:      src,
		ColumnNames: cols,
		IsReplace:   isReplace,
		OnDupExprs:  onDupExprs,
	}
}

// Schema implements the sql.Node interface.
// Insert nodes return rows that are inserted. Replaces return a concatenation of the deleted row and the inserted row.
// If no row was deleted, the value of those columns is nil.
func (p *InsertInto) Schema() sql.Schema {
	if p.IsReplace {
		return append(p.Destination.Schema(), p.Destination.Schema()...)
	}
	return p.Destination.Schema()
}

func (p *InsertInto) Children() []sql.Node {
	return []sql.Node{p.Destination}
}

type insertIter struct {
	schema      sql.Schema
	inserter    sql.RowInserter
	replacer    sql.RowReplacer
	updater     sql.RowUpdater
	rowSource   sql.RowIter
	ctx         *sql.Context
	updateExprs []sql.Expression
	tableNode   sql.Node
	closed      bool
}

func GetInsertable(node sql.Node) (sql.InsertableTable, error) {
	switch node := node.(type) {
	case *Exchange:
		return GetInsertable(node.Child)
	case sql.InsertableTable:
		return node, nil
	case *ResolvedTable:
		return getInsertableTable(node.Table)
	case *prependNode:
		return GetInsertable(node.Child)
	default:
		return nil, ErrInsertIntoNotSupported.New()
	}
}

func getInsertableTable(t sql.Table) (sql.InsertableTable, error) {
	switch t := t.(type) {
	case sql.InsertableTable:
		return t, nil
	case sql.TableWrapper:
		return getInsertableTable(t.Underlying())
	default:
		return nil, ErrInsertIntoNotSupported.New()
	}
}

func newInsertIter(
	ctx *sql.Context,
	table sql.Node,
	values sql.Node,
	isReplace bool,
	onDupUpdateExpr []sql.Expression,
	row sql.Row,
) (*insertIter, error) {
	dstSchema := table.Schema()

	insertable, err := GetInsertable(table)
	if err != nil {
		return nil, err
	}

	var inserter sql.RowInserter

	var replacer sql.RowReplacer
	var updater sql.RowUpdater
	// These type casts have already been asserted in the analyzer
	if isReplace {
		replacer = insertable.(sql.ReplaceableTable).Replacer(ctx)
	} else {
		inserter = insertable.Inserter(ctx)
		if len(onDupUpdateExpr) > 0 {
			updater = insertable.(sql.UpdatableTable).Updater(ctx)
		}
	}

	rowIter, err := values.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return &insertIter{
		schema:      dstSchema,
		tableNode:   table,
		inserter:    inserter,
		replacer:    replacer,
		updater:     updater,
		rowSource:   rowIter,
		updateExprs: onDupUpdateExpr,
		ctx:         ctx,
	}, nil
}

func (i insertIter) Next() (returnRow sql.Row, returnErr error) {
	row, err := i.rowSource.Next()
	if err == io.EOF {
		return nil, err
	}

	if err != nil {
		_ = i.rowSource.Close()
		return nil, err
	}

	// Prune the row down to the size of the schema. It can be larger in the case of running with an outer scope, in which
	// case the additional scope variables are prepended to the row.
	if len(row) > len(i.schema) {
		row = row[len(row)-len(i.schema):]
	}

	err = validateNullability(i.schema, row)
	if err != nil {
		_ = i.rowSource.Close()
		return nil, err
	}

	// Do any necessary type conversions to the target schema
	for i, col := range i.schema {
		if row[i] != nil {
			row[i], err = col.Type.Convert(row[i])
			if err != nil {
				return nil, err
			}
		}
	}

	if i.replacer != nil {
		toReturn := row.Append(row)
		if err = i.replacer.Delete(i.ctx, row); err != nil {
			if !sql.ErrDeleteRowNotFound.Is(err) {
				_ = i.rowSource.Close()
				return nil, err
			}
			// if the row was not found during deletion, write nils into the toReturn row
			for i := range row {
				toReturn[i] = nil
			}
		}

		if err = i.replacer.Insert(i.ctx, row); err != nil {
			_ = i.rowSource.Close()
			return nil, err
		}
		return toReturn, nil
	} else {
		if err := i.inserter.Insert(i.ctx, row); err != nil {
			if (!sql.ErrPrimaryKeyViolation.Is(err) && !sql.ErrUniqueKeyViolation.Is(err)) || len(i.updateExprs) == 0 {
				_ = i.rowSource.Close()
				return nil, err
			}

			// Handle ON DUPLICATE KEY UPDATE clause
			var pkExpression sql.Expression
			for index, col := range i.schema {
				if col.PrimaryKey {
					value := row[index]
					exp := expression.NewEquals(expression.NewGetField(index, col.Type, col.Name, col.Nullable), expression.NewLiteral(value, col.Type))
					if pkExpression != nil {
						pkExpression = expression.NewAnd(pkExpression, exp)
					} else {
						pkExpression = exp
					}
				}
			}

			filter := NewFilter(pkExpression, i.tableNode)
			filterIter, err := filter.RowIter(i.ctx, row)
			if err != nil {
				return nil, err
			}

			defer func() {
				err := filterIter.Close()
				if returnErr == nil {
					returnErr = err
				}
			}()

			// By definition, there can only be a single row here. And only one row should ever be updated according to the
			// spec:
			// https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html
			rowToUpdate, err := filterIter.Next()
			if err != nil {
				return nil, err
			}

			err = i.resolveValues(i.ctx, row)
			if err != nil {
				return nil, err
			}

			newRow, err := applyUpdateExpressions(i.ctx, i.updateExprs, rowToUpdate)
			if err != nil {
				return nil, err
			}

			err = i.updater.Update(i.ctx, rowToUpdate, newRow)
			if err != nil {
				return nil, err
			}

			// In the case that we attempted an update, return a concatenated [old,new] row just like update.
			return rowToUpdate.Append(newRow), nil
		}
	}

	return row, nil
}

// resolveValues resolves all VALUES functions.
func (i insertIter) resolveValues(ctx *sql.Context, insertRow sql.Row) error {
	for _, updateExpr := range i.updateExprs {
		var err error
		sql.Inspect(updateExpr, func(expr sql.Expression) bool {
			valuesExpr, ok := expr.(*function.Values)
			if !ok {
				return true
			}
			getField, ok := valuesExpr.Child.(*expression.GetField)
			if !ok {
				err = fmt.Errorf("VALUES functions may only contain column names")
				return false
			}
			valuesExpr.Value = insertRow[getField.Index()]
			return false
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (i insertIter) Close() error {
	if !i.closed {
		i.closed = true
		if i.inserter != nil {
			if err := i.inserter.Close(i.ctx); err != nil {
				return err
			}
		}
		if i.replacer != nil {
			if err := i.replacer.Close(i.ctx); err != nil {
				return err
			}
		}
		if i.updater != nil {
			if err := i.updater.Close(i.ctx); err != nil {
				return err
			}
		}
		if i.rowSource != nil {
			if err := i.rowSource.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

// RowIter implements the Node interface.
func (p *InsertInto) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return newInsertIter(ctx, p.Destination, p.Source, p.IsReplace, p.OnDupExprs, row)
}

// WithChildren implements the Node interface.
func (p *InsertInto) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}

	np := *p
	np.Destination = children[0]
	return &np, nil
}

// WithSource sets the source node for this insert, which is analyzed separately
func (p *InsertInto) WithSource(src sql.Node) sql.Node {
	np := *p
	np.Source = src
	return &np
}

func (p InsertInto) String() string {
	pr := sql.NewTreePrinter()
	if p.IsReplace {
		_ = pr.WriteNode("Replace(%s)", strings.Join(p.ColumnNames, ", "))
	} else {
		_ = pr.WriteNode("Insert(%s)", strings.Join(p.ColumnNames, ", "))
	}
	_ = pr.WriteChildren(p.Destination.String(), p.Source.String())
	return pr.String()
}

func (p InsertInto) DebugString() string {
	pr := sql.NewTreePrinter()
	var columnNames []string
	if p.IsReplace {
		_ = pr.WriteNode("Replace(%s)", strings.Join(columnNames, ", "))
	} else {
		_ = pr.WriteNode("Insert(%s)", strings.Join(columnNames, ", "))
	}
	_ = pr.WriteChildren(sql.DebugString(p.Destination), sql.DebugString(p.Source))
	return pr.String()
}

func validateNullability(dstSchema sql.Schema, row sql.Row) error {
	for i, col := range dstSchema {
		if !col.Nullable && row[i] == nil {
			return ErrInsertIntoNonNullableProvidedNull.New(col.Name)
		}
	}
	return nil
}

func (p *InsertInto) Expressions() []sql.Expression {
	return p.OnDupExprs
}

func (p *InsertInto) WithExpressions(newExprs ...sql.Expression) (sql.Node, error) {
	if len(newExprs) != len(p.OnDupExprs) {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(p.OnDupExprs), 1)
	}

	return NewInsertInto(p.Destination, p.Source, p.IsReplace, p.ColumnNames, newExprs), nil
}

// Resolved implements the Resolvable interface.
func (p *InsertInto) Resolved() bool {
	if !p.Destination.Resolved() || !p.Source.Resolved() {
		return false
	}
	for _, updateExpr := range p.OnDupExprs {
		if !updateExpr.Resolved() {
			return false
		}
	}
	return true
}
