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
	"strings"

	"github.com/dolthub/go-mysql-server/sql/grant_tables"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type IfNotExistsOption bool

const (
	IfNotExists       IfNotExistsOption = true
	IfNotExistsAbsent IfNotExistsOption = false
)

type TempTableOption bool

const (
	IsTempTable       TempTableOption = true
	IsTempTableAbsent TempTableOption = false
)

// Ddl nodes have a reference to a database, but no children and a nil schema.
type ddlNode struct {
	db sql.Database
}

// Resolved implements the Resolvable interface.
func (c *ddlNode) Resolved() bool {
	_, ok := c.db.(sql.UnresolvedDatabase)
	return !ok
}

// Database implements the sql.Databaser interface.
func (c *ddlNode) Database() sql.Database {
	return c.db
}

// Schema implements the Node interface.
func (*ddlNode) Schema() sql.Schema { return nil }

// Children implements the Node interface.
func (c *ddlNode) Children() []sql.Node { return nil }

type IndexDefinition struct {
	IndexName  string
	Using      sql.IndexUsing
	Constraint sql.IndexConstraint
	Columns    []sql.IndexColumn
	Comment    string
}

func (i *IndexDefinition) String() string {
	return i.IndexName
}

// TableSpec is a node describing the schema of a table.
type TableSpec struct {
	Schema  sql.PrimaryKeySchema
	FkDefs  []*sql.ForeignKeyConstraint
	ChDefs  []*sql.CheckConstraint
	IdxDefs []*IndexDefinition
}

func (c *TableSpec) WithSchema(schema sql.PrimaryKeySchema) *TableSpec {
	nc := *c
	nc.Schema = schema
	return &nc
}

func (c *TableSpec) WithForeignKeys(fkDefs []*sql.ForeignKeyConstraint) *TableSpec {
	nc := *c
	nc.FkDefs = fkDefs
	return &nc
}

func (c *TableSpec) WithCheckConstraints(chDefs []*sql.CheckConstraint) *TableSpec {
	nc := *c
	nc.ChDefs = chDefs
	return &nc
}

func (c *TableSpec) WithIndices(idxDefs []*IndexDefinition) *TableSpec {
	nc := *c
	nc.IdxDefs = idxDefs
	return &nc
}

// CreateTable is a node describing the creation of some table.
type CreateTable struct {
	ddlNode
	name         string
	CreateSchema sql.PrimaryKeySchema
	ifNotExists  IfNotExistsOption
	fkDefs       []*sql.ForeignKeyConstraint
	chDefs       sql.CheckConstraints
	idxDefs      []*IndexDefinition
	like         sql.Node
	temporary    TempTableOption
	selectNode   sql.Node
}

var _ sql.Databaser = (*CreateTable)(nil)
var _ sql.Node = (*CreateTable)(nil)
var _ sql.Expressioner = (*CreateTable)(nil)

// NewCreateTable creates a new CreateTable node
func NewCreateTable(db sql.Database, name string, ifn IfNotExistsOption, temp TempTableOption, tableSpec *TableSpec) *CreateTable {
	for _, s := range tableSpec.Schema.Schema {
		s.Source = name
	}

	return &CreateTable{
		ddlNode:      ddlNode{db},
		name:         name,
		CreateSchema: tableSpec.Schema,
		fkDefs:       tableSpec.FkDefs,
		chDefs:       tableSpec.ChDefs,
		idxDefs:      tableSpec.IdxDefs,
		ifNotExists:  ifn,
		temporary:    temp,
	}
}

// NewCreateTableLike creates a new CreateTable node for CREATE TABLE LIKE statements
func NewCreateTableLike(db sql.Database, name string, likeTable sql.Node, ifn IfNotExistsOption, temp TempTableOption) *CreateTable {
	return &CreateTable{
		ddlNode:     ddlNode{db},
		name:        name,
		ifNotExists: ifn,
		like:        likeTable,
		temporary:   temp,
	}
}

// NewCreateTableSelect create a new CreateTable node for CREATE TABLE [AS] SELECT
func NewCreateTableSelect(db sql.Database, name string, selectNode sql.Node, tableSpec *TableSpec, ifn IfNotExistsOption, temp TempTableOption) *CreateTable {
	for _, s := range tableSpec.Schema.Schema {
		s.Source = name
	}

	return &CreateTable{
		ddlNode:      ddlNode{db: db},
		CreateSchema: tableSpec.Schema,
		fkDefs:       tableSpec.FkDefs,
		chDefs:       tableSpec.ChDefs,
		idxDefs:      tableSpec.IdxDefs,
		name:         name,
		selectNode:   selectNode,
		ifNotExists:  ifn,
		temporary:    temp,
	}
}

// WithDatabase implements the sql.Databaser interface.
func (c *CreateTable) WithDatabase(db sql.Database) (sql.Node, error) {
	nc := *c
	nc.db = db
	return &nc, nil
}

// Schema implements the sql.Node interface.
func (c *CreateTable) Schema() sql.Schema {
	return sql.Schema{}
}

func (c *CreateTable) PkSchema() sql.PrimaryKeySchema {
	return c.CreateSchema
}

// Resolved implements the Resolvable interface.
func (c *CreateTable) Resolved() bool {
	if !c.ddlNode.Resolved() {
		return false
	}

	for _, col := range c.CreateSchema.Schema {
		if !col.Default.Resolved() {
			return false
		}
	}

	for _, chDef := range c.chDefs {
		if !chDef.Expr.Resolved() {
			return false
		}
	}

	if c.like != nil {
		if !c.like.Resolved() {
			return false
		}
	}

	return true
}

// RowIter implements the Node interface.
func (c *CreateTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	var err error
	if c.temporary == IsTempTable {
		maybePrivDb := c.db
		if privDb, ok := maybePrivDb.(grant_tables.PrivilegedDatabase); ok {
			maybePrivDb = privDb.Unwrap()
		}
		creatable, ok := maybePrivDb.(sql.TemporaryTableCreator)
		if !ok {
			return sql.RowsToRowIter(), sql.ErrTemporaryTableNotSupported.New()
		}

		if err := c.validateDefaultPosition(); err != nil {
			return sql.RowsToRowIter(), err
		}

		err = creatable.CreateTemporaryTable(ctx, c.name, c.CreateSchema)
	} else {
		maybePrivDb := c.db
		if privDb, ok := maybePrivDb.(grant_tables.PrivilegedDatabase); ok {
			maybePrivDb = privDb.Unwrap()
		}
		creatable, ok := maybePrivDb.(sql.TableCreator)
		if !ok {
			return sql.RowsToRowIter(), sql.ErrCreateTableNotSupported.New(c.db.Name())
		}

		if err := c.validateDefaultPosition(); err != nil {
			return sql.RowsToRowIter(), err
		}

		err = creatable.CreateTable(ctx, c.name, c.CreateSchema)
	}

	if err != nil && !(sql.ErrTableAlreadyExists.Is(err) && (c.ifNotExists == IfNotExists)) {
		return sql.RowsToRowIter(), err
	}

	//TODO: in the event that foreign keys or indexes aren't supported, you'll be left with a created table and no foreign keys/indexes
	//this also means that if a foreign key or index fails, you'll only have what was declared up to the failure
	tableNode, ok, err := c.db.GetTableInsensitive(ctx, c.name)
	if err != nil {
		return sql.RowsToRowIter(), err
	}
	if !ok {
		return sql.RowsToRowIter(), sql.ErrTableCreatedNotFound.New()
	}

	var nonPrimaryIdxes []*IndexDefinition
	for _, def := range c.idxDefs {
		if def.Constraint != sql.IndexConstraint_Primary {
			nonPrimaryIdxes = append(nonPrimaryIdxes, def)
		}
	}

	if len(nonPrimaryIdxes) > 0 {
		err = c.createIndexes(ctx, tableNode, nonPrimaryIdxes)
		if err != nil {
			return sql.RowsToRowIter(), err
		}
	}

	if len(c.fkDefs) > 0 {
		err = c.createForeignKeys(ctx, tableNode)
		if err != nil {
			return sql.RowsToRowIter(), err
		}
	}

	if len(c.chDefs) > 0 {
		err = c.createChecks(ctx, tableNode)
		if err != nil {
			return sql.RowsToRowIter(), err
		}
	}

	return sql.RowsToRowIter(), nil
}

func (c *CreateTable) createIndexes(ctx *sql.Context, tableNode sql.Table, idxes []*IndexDefinition) error {
	idxAlterable, ok := tableNode.(sql.IndexAlterableTable)
	if !ok {
		return ErrNotIndexable.New()
	}

	for _, idxDef := range idxes {
		err := idxAlterable.CreateIndex(ctx, idxDef.IndexName, idxDef.Using, idxDef.Constraint, idxDef.Columns, idxDef.Comment)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *CreateTable) createForeignKeys(ctx *sql.Context, tableNode sql.Table) error {
	fkAlterable, ok := tableNode.(sql.ForeignKeyAlterableTable)
	if !ok {
		return ErrNoForeignKeySupport.New(c.name)
	}

	fkChecks, err := ctx.GetSessionVariable(ctx, "foreign_key_checks")
	if err != nil {
		return err
	}
	if fkChecks.(int8) == 1 {
		for _, fkDef := range c.fkDefs {
			refTbl, ok, err := c.db.GetTableInsensitive(ctx, fkDef.ReferencedTable)
			if err != nil {
				return err
			}
			if !ok {
				return sql.ErrTableNotFound.New(fkDef.ReferencedTable)
			}
			err = executeCreateForeignKey(ctx, fkAlterable, refTbl, fkDef)
			if err != nil {
				return err
			}
		}
	} else {
		for _, fkDef := range c.fkDefs {
			err = fkAlterable.CreateForeignKey(
				ctx,
				fkDef.Name,
				fkDef.Columns,
				fkDef.ReferencedTable,
				fkDef.ReferencedColumns,
				fkDef.OnUpdate,
				fkDef.OnDelete,
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *CreateTable) createChecks(ctx *sql.Context, tableNode sql.Table) error {
	chAlterable, ok := tableNode.(sql.CheckAlterableTable)
	if !ok {
		return ErrNoCheckConstraintSupport.New(c.name)
	}

	for _, ch := range c.chDefs {
		check, err := NewCheckDefinition(ctx, ch)
		if err != nil {
			return err
		}
		err = chAlterable.CreateCheck(ctx, check)
		if err != nil {
			return err
		}
	}

	return nil
}

// Children implements the Node interface.
func (c *CreateTable) Children() []sql.Node {
	if c.like != nil {
		return []sql.Node{c.like}
	} else if c.selectNode != nil {
		return []sql.Node{c.selectNode}
	}
	return nil
}

// WithChildren implements the Node interface.
func (c CreateTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) == 0 {
		return &c, nil
	} else if len(children) == 1 {
		child := children[0]

		switch child.(type) {
		case *Project, *Limit:
			c.selectNode = child
		default:
			c.like = child
		}

		return &c, nil
	} else {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
}

// CheckPrivileges implements the interface sql.Node.
func (c *CreateTable) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if c.temporary == IsTempTable {
		return opChecker.UserHasPrivileges(ctx,
			sql.NewPrivilegedOperation(c.db.Name(), "", "", sql.PrivilegeType_CreateTempTable))
	} else {
		return opChecker.UserHasPrivileges(ctx,
			sql.NewPrivilegedOperation(c.db.Name(), "", "", sql.PrivilegeType_Create))
	}
}

func (c *CreateTable) String() string {
	ifNotExists := ""
	if c.ifNotExists {
		ifNotExists = "if not exists "
	}
	return fmt.Sprintf("Create table %s%s", ifNotExists, c.name)
}

func (c *CreateTable) DebugString() string {
	ifNotExists := ""
	if c.ifNotExists {
		ifNotExists = "if not exists "
	}
	p := sql.NewTreePrinter()
	p.WriteNode("Create table %s%s", ifNotExists, c.name)

	var children []string
	children = append(children, c.schemaDebugString())

	if len(c.fkDefs) > 0 {
		children = append(children, c.foreignKeysDebugString())
	}
	if len(c.idxDefs) > 0 {
		children = append(children, c.indexesDebugString())
	}
	if len(c.chDefs) > 0 {
		children = append(children, c.checkConstraintsDebugString())
	}

	p.WriteChildren(children...)
	return p.String()
}

func (c *CreateTable) foreignKeysDebugString() string {
	p := sql.NewTreePrinter()
	p.WriteNode("ForeignKeys")
	var children []string
	for _, def := range c.fkDefs {
		children = append(children, sql.DebugString(def))
	}
	p.WriteChildren(children...)
	return p.String()
}

func (c *CreateTable) indexesDebugString() string {
	p := sql.NewTreePrinter()
	p.WriteNode("Indexes")
	var children []string
	for _, def := range c.idxDefs {
		children = append(children, sql.DebugString(def))
	}
	p.WriteChildren(children...)
	return p.String()
}

func (c *CreateTable) checkConstraintsDebugString() string {
	p := sql.NewTreePrinter()
	p.WriteNode("CheckConstraints")
	var children []string
	for _, def := range c.chDefs {
		children = append(children, sql.DebugString(def))
	}
	p.WriteChildren(children...)
	return p.String()
}

func (c *CreateTable) schemaDebugString() string {
	p := sql.NewTreePrinter()
	p.WriteNode("Columns")
	var children []string
	for _, col := range c.CreateSchema.Schema {
		children = append(children, sql.DebugString(col))
	}
	p.WriteChildren(children...)
	return p.String()
}

func (c *CreateTable) Expressions() []sql.Expression {
	exprs := make([]sql.Expression, len(c.CreateSchema.Schema)+len(c.chDefs))
	i := 0
	for _, col := range c.CreateSchema.Schema {
		exprs[i] = expression.WrapExpression(col.Default)
		i++
	}
	for _, ch := range c.chDefs {
		exprs[i] = ch.Expr
		i++
	}
	return exprs
}

func (c *CreateTable) Like() sql.Node {
	return c.like
}

func (c *CreateTable) Select() sql.Node {
	return c.selectNode
}

func (c *CreateTable) TableSpec() *TableSpec {
	tableSpec := TableSpec{}

	ret := tableSpec.WithSchema(c.CreateSchema)
	ret = ret.WithForeignKeys(c.fkDefs)
	ret = ret.WithIndices(c.idxDefs)
	ret = ret.WithCheckConstraints(c.chDefs)

	return ret
}

func (c *CreateTable) Name() string {
	return c.name
}

func (c *CreateTable) IfNotExists() IfNotExistsOption {
	return c.ifNotExists
}

func (c *CreateTable) Temporary() TempTableOption {
	return c.temporary
}

func (c CreateTable) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	length := len(c.CreateSchema.Schema) + len(c.chDefs)
	if len(exprs) != length {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(exprs), length)
	}

	nc := c

	// Make sure to make a deep copy of any slices here so we aren't modifying the original pointer
	ns := c.CreateSchema.Schema.Copy()
	i := 0
	for ; i < len(c.CreateSchema.Schema); i++ {
		unwrappedColDefVal, ok := exprs[i].(*expression.Wrapper).Unwrap().(*sql.ColumnDefaultValue)
		if ok {
			ns[i].Default = unwrappedColDefVal
		} else { // nil fails type check
			ns[i].Default = nil
		}
	}
	nc.CreateSchema = sql.NewPrimaryKeySchema(ns, c.CreateSchema.PkOrdinals...)

	ncd, err := c.chDefs.FromExpressions(exprs[i:])
	if err != nil {
		return nil, err
	}

	nc.chDefs = ncd
	return &nc, nil
}

func (c *CreateTable) validateDefaultPosition() error {
	colsAfterThis := make(map[string]*sql.Column)
	for i := len(c.CreateSchema.Schema) - 1; i >= 0; i-- {
		col := c.CreateSchema.Schema[i]
		colsAfterThis[col.Name] = col
		if err := inspectDefaultForInvalidColumns(col, colsAfterThis); err != nil {
			return err
		}
	}

	return nil
}

// DropTable is a node describing dropping one or more tables
type DropTable struct {
	ddlNode
	names        []string
	ifExists     bool
	triggerNames []string
}

var _ sql.Node = (*DropTable)(nil)
var _ sql.Databaser = (*DropTable)(nil)

// NewDropTable creates a new DropTable node
func NewDropTable(db sql.Database, ifExists bool, tableNames ...string) *DropTable {
	return &DropTable{
		ddlNode:  ddlNode{db},
		names:    tableNames,
		ifExists: ifExists,
	}
}

// WithDatabase implements the sql.Databaser interface.
func (d *DropTable) WithDatabase(db sql.Database) (sql.Node, error) {
	nc := *d
	nc.db = db
	return &nc, nil
}

// WithTriggers returns this node but with the given triggers.
func (d *DropTable) WithTriggers(triggers []string) sql.Node {
	nd := *d
	nd.triggerNames = triggers
	return &nd
}

// TableNames returns the names of the tables to drop.
func (d *DropTable) TableNames() []string {
	return d.names
}

// RowIter implements the Node interface.
func (d *DropTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	droppable, ok := d.db.(sql.TableDropper)
	if !ok {
		return nil, sql.ErrDropTableNotSupported.New(d.db.Name())
	}

	var err error
	for _, tableName := range d.names {
		tbl, ok, err := d.db.GetTableInsensitive(ctx, tableName)

		if err != nil {
			return nil, err
		}

		if !ok {
			if d.ifExists {
				continue
			}

			return nil, sql.ErrTableNotFound.New(tableName)
		}
		err = droppable.DropTable(ctx, tbl.Name())
		if err != nil {
			return nil, err
		}
	}

	if len(d.triggerNames) > 0 {
		//TODO: if dropping any triggers fail, then we'll be left in a state where triggers exist for a table that was dropped
		triggerDb, ok := d.db.(sql.TriggerDatabase)
		if !ok {
			return nil, fmt.Errorf(`tables %v are referenced in triggers %v, but database does not support triggers`, d.names, d.triggerNames)
		}
		for _, trigger := range d.triggerNames {
			err = triggerDb.DropTrigger(ctx, trigger)
			if err != nil {
				return nil, err
			}
		}
	}

	return sql.RowsToRowIter(), err
}

// WithChildren implements the Node interface.
func (d *DropTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(d, children...)
}

// CheckPrivileges implements the interface sql.Node.
func (d *DropTable) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(d.db.Name(), "", "", sql.PrivilegeType_Drop))
}

func (d *DropTable) String() string {
	ifExists := ""
	names := strings.Join(d.names, ", ")
	if d.ifExists {
		ifExists = "if exists "
	}
	return fmt.Sprintf("Drop table %s%s", ifExists, names)
}
