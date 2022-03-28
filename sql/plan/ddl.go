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

// ColumnNames returns each column's name without the length property.
func (i *IndexDefinition) ColumnNames() []string {
	colNames := make([]string, len(i.Columns))
	for i, col := range i.Columns {
		colNames[i] = col.Name
	}
	return colNames
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
	fkParentTbls []sql.ForeignKeyTable
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

// ForeignKeys returns any foreign keys that will be declared on this table.
func (c *CreateTable) ForeignKeys() []*sql.ForeignKeyConstraint {
	return c.fkDefs
}

// WithParentForeignKeyTables adds the tables that are referenced in each foreign key. The table indices is assumed
// to match the foreign key indices in their respective slices.
func (c *CreateTable) WithParentForeignKeyTables(refTbls []sql.ForeignKeyTable) (*CreateTable, error) {
	if len(c.fkDefs) != len(refTbls) {
		return nil, fmt.Errorf("table `%s` defines `%d` foreign keys but found `%d` referenced tables",
			c.name, len(c.fkDefs), len(refTbls))
	}
	nc := *c
	nc.fkParentTbls = refTbls
	return &nc, nil
}

func (c *CreateTable) createIndexes(ctx *sql.Context, tableNode sql.Table, idxes []*IndexDefinition) error {
	idxAlterable, ok := tableNode.(sql.IndexAlterableTable)
	if !ok {
		return ErrNotIndexable.New()
	}

	indexMap := make(map[string]struct{})
	for _, idxDef := range idxes {
		indexName := idxDef.IndexName
		// If the name is empty, we create a new name using the columns provided while appending an ascending integer
		// until we get a non-colliding name if the original name (or each preceding name) already exists.
		if indexName == "" {
			indexName = strings.Join(idxDef.ColumnNames(), "")
			if _, ok = indexMap[strings.ToLower(indexName)]; ok {
				for i := 0; true; i++ {
					newIndexName := fmt.Sprintf("%s_%d", indexName, i)
					if _, ok = indexMap[strings.ToLower(newIndexName)]; !ok {
						indexName = newIndexName
						break
					}
				}
			}
		} else if _, ok = indexMap[strings.ToLower(idxDef.IndexName)]; ok {
			return sql.ErrIndexIDAlreadyRegistered.New(idxDef.IndexName)
		}
		err := idxAlterable.CreateIndex(ctx, indexName, idxDef.Using, idxDef.Constraint, idxDef.Columns, idxDef.Comment)
		if err != nil {
			return err
		}
		indexMap[strings.ToLower(indexName)] = struct{}{}
	}

	return nil
}

func (c *CreateTable) createForeignKeys(ctx *sql.Context, tableNode sql.Table) error {
	fkTbl, ok := tableNode.(sql.ForeignKeyTable)
	if !ok {
		return sql.ErrNoForeignKeySupport.New(c.name)
	}

	fkChecks, err := ctx.GetSessionVariable(ctx, "foreign_key_checks")
	if err != nil {
		return err
	}
	for i, fkDef := range c.fkDefs {
		if fkDef.OnUpdate == sql.ForeignKeyReferentialAction_SetDefault || fkDef.OnDelete == sql.ForeignKeyReferentialAction_SetDefault {
			return sql.ErrForeignKeySetDefault.New()
		}
		if fkChecks.(int8) == 1 {
			fkParentTbl := c.fkParentTbls[i]
			// If a foreign key is self-referential then the analyzer uses a nil since the table does not yet exist
			if fkParentTbl == nil {
				fkParentTbl = fkTbl
			}
			// If foreign_key_checks are true, then the referenced tables will be populated
			err = ResolveForeignKey(ctx, fkTbl, fkParentTbl, *fkDef, true)
			if err != nil {
				return err
			}
		} else {
			err = fkTbl.AddForeignKey(ctx, *fkDef)
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
	Tables       []sql.Node
	ifExists     bool
	triggerNames []string
}

var _ sql.Node = (*DropTable)(nil)

// NewDropTable creates a new DropTable node
func NewDropTable(tbls []sql.Node, ifExists bool) *DropTable {
	return &DropTable{
		Tables:   tbls,
		ifExists: ifExists,
	}
}

// WithTriggers returns this node but with the given triggers.
func (d *DropTable) WithTriggers(triggers []string) sql.Node {
	nd := *d
	nd.triggerNames = triggers
	return &nd
}

// TableNames returns the names of the tables to drop.
func (d *DropTable) TableNames() ([]string, error) {
	tblNames := make([]string, len(d.Tables))
	for i, t := range d.Tables {
		// either *ResolvedTable OR *UnresolvedTable here
		if uTable, ok := t.(*UnresolvedTable); ok {
			tblNames[i] = uTable.Name()
		} else if rTable, ok := t.(*ResolvedTable); ok {
			tblNames[i] = rTable.Name()
		} else {
			return []string{}, sql.ErrInvalidType.New(t)
		}
	}
	return tblNames, nil
}

// IfExists returns ifExists variable.
func (d *DropTable) IfExists() bool {
	return d.ifExists
}

// RowIter implements the Node interface.
func (d *DropTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	var err error
	var curdb sql.Database

	for _, table := range d.Tables {
		tbl := table.(*ResolvedTable)
		curdb = tbl.Database

		droppable := tbl.Database.(sql.TableDropper)

		if fkTable, err := getForeignKeyTable(tbl); err == nil {
			fkChecks, err := ctx.GetSessionVariable(ctx, "foreign_key_checks")
			if err != nil {
				return nil, err
			}
			if fkChecks.(int8) == 1 {
				parentFks, err := fkTable.GetReferencedForeignKeys(ctx)
				if err != nil {
					return nil, err
				}
				if len(parentFks) > 0 {
					return nil, sql.ErrForeignKeyDropTable.New(fkTable.Name(), parentFks[0].Name)
				}
			}
			fks, err := fkTable.GetDeclaredForeignKeys(ctx)
			if err != nil {
				return nil, err
			}
			for _, fk := range fks {
				if err = fkTable.DropForeignKey(ctx, fk.Name); err != nil {
					return nil, err
				}
			}
		}

		err = droppable.DropTable(ctx, tbl.Name())
		if err != nil {
			return nil, err
		}
	}

	if len(d.triggerNames) > 0 {
		triggerDb, ok := curdb.(sql.TriggerDatabase)
		if !ok {
			tblNames, _ := d.TableNames()
			return nil, fmt.Errorf(`tables %v are referenced in triggers %v, but database does not support triggers`, tblNames, d.triggerNames)
		}
		//TODO: if dropping any triggers fail, then we'll be left in a state where triggers exist for a table that was dropped
		for _, trigger := range d.triggerNames {
			err = triggerDb.DropTrigger(ctx, trigger)
			if err != nil {
				return nil, err
			}
		}
	}

	return sql.RowsToRowIter(), err
}

// Children implements the Node interface.
func (d *DropTable) Children() []sql.Node {
	return d.Tables
}

// Resolved implements the sql.Expression interface.
func (d *DropTable) Resolved() bool {
	for _, table := range d.Tables {
		if !table.Resolved() {
			return false
		}
	}

	return true
}

// Schema implements the sql.Expression interface.
func (d *DropTable) Schema() sql.Schema {
	return sql.OkResultSchema
}

// WithChildren implements the Node interface.
func (d *DropTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	// Number of children can be smaller than original as the non-existent tables get filtered out in some cases
	var newChildren = make([]sql.Node, len(children))
	for i, child := range children {
		newChildren[i] = child
	}
	d.Tables = newChildren
	return d, nil
}

// CheckPrivileges implements the interface sql.Node.
func (d *DropTable) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	for _, tbl := range d.Tables {
		if !opChecker.UserHasPrivileges(ctx,
			sql.NewPrivilegedOperation(getDatabaseName(tbl), getTableName(tbl), "", sql.PrivilegeType_Drop)) {
			return false
		}
	}
	return true
}

// String implements the sql.Node interface.
func (d *DropTable) String() string {
	ifExists := ""
	tblNames, _ := d.TableNames()
	names := strings.Join(tblNames, ", ")
	if d.ifExists {
		ifExists = "if exists "
	}
	return fmt.Sprintf("Drop table %s%s", ifExists, names)
}
