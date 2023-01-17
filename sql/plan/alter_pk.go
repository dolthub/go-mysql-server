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
	"io"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type PKAction byte

const (
	PrimaryKeyAction_Create PKAction = iota
	PrimaryKeyAction_Drop
)

// ErrNotPrimaryKeyAlterable is return when a table cannot be determined to be primary key alterable
var ErrNotPrimaryKeyAlterable = errors.NewKind("error: table is not primary key alterable")

type AlterPK struct {
	ddlNode

	Action       PKAction
	Table        sql.Node
	Columns      []sql.IndexColumn
	Catalog      sql.Catalog
	targetSchema sql.Schema
}

var _ sql.Databaser = (*AlterPK)(nil)
var _ sql.SchemaTarget = (*AlterPK)(nil)

func NewAlterCreatePk(db sql.Database, table sql.Node, columns []sql.IndexColumn) *AlterPK {
	return &AlterPK{
		Action:  PrimaryKeyAction_Create,
		ddlNode: ddlNode{db: db},
		Table:   table,
		Columns: columns,
	}
}

func NewAlterDropPk(db sql.Database, table sql.Node) *AlterPK {
	return &AlterPK{
		Action:  PrimaryKeyAction_Drop,
		Table:   table,
		ddlNode: ddlNode{db: db},
	}
}

func (a *AlterPK) Resolved() bool {
	for _, expr := range a.Expressions() {
		if expr.Resolved() == false {
			return false
		}
	}

	return a.Table.Resolved() && a.ddlNode.Resolved()
}

func (a *AlterPK) String() string {
	action := "add"
	if a.Action == PrimaryKeyAction_Drop {
		action = "drop"
	}

	return fmt.Sprintf("alter table %s %s primary key", a.Table.String(), action)
}

func (a *AlterPK) Schema() sql.Schema {
	return types.OkResultSchema
}

func (a AlterPK) WithTargetSchema(schema sql.Schema) (sql.Node, error) {
	a.targetSchema = schema
	return &a, nil
}

func (a *AlterPK) TargetSchema() sql.Schema {
	return a.targetSchema
}

func (a *AlterPK) Expressions() []sql.Expression {
	return transform.WrappedColumnDefaults(a.targetSchema)
}

func (a AlterPK) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(a.targetSchema) {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(exprs), len(a.targetSchema))
	}

	a.targetSchema = transform.SchemaWithDefaults(a.targetSchema, exprs[:len(a.targetSchema)])
	return &a, nil
}

type dropPkIter struct {
	targetSchema sql.Schema
	pkAlterable  sql.PrimaryKeyAlterableTable
	runOnce      bool
}

func (d *dropPkIter) Next(ctx *sql.Context) (sql.Row, error) {
	if d.runOnce {
		return nil, io.EOF
	}
	d.runOnce = true

	if rwt, ok := d.pkAlterable.(sql.RewritableTable); ok {
		err := d.rewriteTable(ctx, rwt)
		if err != nil {
			return nil, err
		}

		return sql.NewRow(types.NewOkResult(0)), nil
	}

	err := d.pkAlterable.DropPrimaryKey(ctx)
	if err != nil {
		return nil, err
	}

	return sql.NewRow(types.NewOkResult(0)), nil
}

func (d *dropPkIter) Close(context *sql.Context) error {
	return nil
}

func (d *dropPkIter) rewriteTable(ctx *sql.Context, rwt sql.RewritableTable) error {
	newSchema := dropKeyFromSchema(d.targetSchema)

	oldPkSchema, newPkSchema := sql.SchemaToPrimaryKeySchema(rwt, rwt.Schema()), newSchema

	inserter, err := rwt.RewriteInserter(ctx, oldPkSchema, newPkSchema, nil, nil, nil)
	if err != nil {
		return err
	}

	partitions, err := rwt.Partitions(ctx)
	if err != nil {
		return err
	}

	rowIter := sql.NewTableRowIter(ctx, rwt, partitions)

	for {
		r, err := rowIter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		err = inserter.Insert(ctx, r)
		if err != nil {
			return err
		}
	}

	// TODO: move this into iter.close, probably
	err = inserter.Close(ctx)
	if err != nil {
		return err
	}

	return nil
}

func dropKeyFromSchema(schema sql.Schema) sql.PrimaryKeySchema {
	newSch := schema.Copy()
	for i := range newSch {
		newSch[i].PrimaryKey = false
	}

	return sql.NewPrimaryKeySchema(newSch)
}

type createPkIter struct {
	targetSchema sql.Schema
	columns      []sql.IndexColumn
	pkAlterable  sql.PrimaryKeyAlterableTable
	runOnce      bool
}

func (c *createPkIter) Next(ctx *sql.Context) (sql.Row, error) {
	if c.runOnce {
		return nil, io.EOF
	}
	c.runOnce = true

	if rwt, ok := c.pkAlterable.(sql.RewritableTable); ok {
		err := c.rewriteTable(ctx, rwt)
		if err != nil {
			return nil, err
		}

		return sql.NewRow(types.NewOkResult(0)), nil
	}

	err := c.pkAlterable.CreatePrimaryKey(ctx, c.columns)
	if err != nil {
		return nil, err
	}

	return sql.NewRow(types.NewOkResult(0)), nil
}

func (c createPkIter) Close(context *sql.Context) error {
	return nil
}

func (c *createPkIter) rewriteTable(ctx *sql.Context, rwt sql.RewritableTable) error {
	newSchema := addKeyToSchema(rwt.Name(), c.targetSchema, c.columns)

	oldPkSchema, newPkSchema := sql.SchemaToPrimaryKeySchema(rwt, rwt.Schema()), newSchema

	inserter, err := rwt.RewriteInserter(ctx, oldPkSchema, newPkSchema, nil, nil, c.columns)
	if err != nil {
		return err
	}

	partitions, err := rwt.Partitions(ctx)
	if err != nil {
		return err
	}

	rowIter := sql.NewTableRowIter(ctx, rwt, partitions)

	for {
		r, err := rowIter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		// check for null values in the primary key insert
		for _, i := range newSchema.PkOrdinals {
			if r[i] == nil {
				return sql.ErrInsertIntoNonNullableProvidedNull.New(newSchema.Schema[i].Name)
			}
		}

		err = inserter.Insert(ctx, r)
		if err != nil {
			return err
		}
	}

	// TODO: move this into iter.close, probably
	err = inserter.Close(ctx)
	if err != nil {
		return err
	}

	return nil
}

func addKeyToSchema(tableName string, schema sql.Schema, columns []sql.IndexColumn) sql.PrimaryKeySchema {
	newSch := schema.Copy()
	ordinals := make([]int, len(columns))
	for i := range columns {
		idx := schema.IndexOf(columns[i].Name, tableName)
		ordinals[i] = idx
		newSch[idx].PrimaryKey = true
	}
	return sql.NewPrimaryKeySchema(newSch, ordinals...)
}

func (a *AlterPK) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// We grab the table from the database to ensure that state is properly refreshed, thereby preventing multiple keys
	// being defined.
	// Grab the table fresh from the database.
	table, err := getTableFromDatabase(ctx, a.Database(), a.Table)
	if err != nil {
		return nil, err
	}

	// TODO: these validation checks belong in the analysis phase, not here
	pkAlterable, ok := table.(sql.PrimaryKeyAlterableTable)
	if !ok {
		return nil, ErrNotPrimaryKeyAlterable.New(a.Table)
	}
	if err != nil {
		return nil, err
	}

	switch a.Action {
	case PrimaryKeyAction_Create:
		if hasPrimaryKeys(pkAlterable) {
			return sql.RowsToRowIter(), sql.ErrMultiplePrimaryKeysDefined.New()
		}

		for _, c := range a.Columns {
			if !pkAlterable.Schema().Contains(c.Name, pkAlterable.Name()) {
				return sql.RowsToRowIter(), sql.ErrKeyColumnDoesNotExist.New(c.Name)
			}
		}

		return &createPkIter{
			targetSchema: a.targetSchema,
			columns:      a.Columns,
			pkAlterable:  pkAlterable,
		}, nil
	case PrimaryKeyAction_Drop:
		return &dropPkIter{
			targetSchema: a.targetSchema,
			pkAlterable:  pkAlterable,
		}, nil
	default:
		panic("unreachable")
	}
}

func hasPrimaryKeys(table sql.Table) bool {
	for _, c := range table.Schema() {
		if c.PrimaryKey {
			return true
		}
	}

	return false
}

func (a AlterPK) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 1)
	}

	a.Table = children[0]
	return &a, nil
}

// Children implements the sql.Node interface.
func (a *AlterPK) Children() []sql.Node {
	return []sql.Node{a.Table}
}

// WithDatabase implements the sql.Databaser interface.
func (a AlterPK) WithDatabase(database sql.Database) (sql.Node, error) {
	a.db = database
	return &a, nil
}

// CheckPrivileges implements the interface sql.Node.
func (a *AlterPK) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(a.Database().Name(), getTableName(a.Table), "", sql.PrivilegeType_Alter))
}
