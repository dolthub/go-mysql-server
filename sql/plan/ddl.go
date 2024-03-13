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

	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Ddl nodes have a reference to a database, but no children and a nil schema.
type ddlNode struct {
	Db sql.Database
}

// Resolved implements the Resolvable interface.
func (c *ddlNode) Resolved() bool {
	_, ok := c.Db.(sql.UnresolvedDatabase)
	return !ok
}

// Database implements the sql.Databaser interface.
func (c *ddlNode) Database() sql.Database {
	return c.Db
}

// Schema implements the Node interface.
func (*ddlNode) Schema() sql.Schema {
	return types.OkResultSchema
}

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

func (i *IndexDefinition) IsSpatial() bool {
	return i.Constraint == sql.IndexConstraint_Spatial
}

func (i *IndexDefinition) IsUnique() bool {
	return i.Constraint == sql.IndexConstraint_Unique
}

func (i *IndexDefinition) IsFullText() bool {
	return i.Constraint == sql.IndexConstraint_Fulltext
}

// ColumnNames returns each column's name without the length property.
func (i *IndexDefinition) ColumnNames() []string {
	colNames := make([]string, len(i.Columns))
	for i, col := range i.Columns {
		colNames[i] = col.Name
	}
	return colNames
}

// AsIndexDef returns the IndexDefinition as the other form.
func (i *IndexDefinition) AsIndexDef() sql.IndexDef {
	//TODO: We should get rid of this IndexDefinition and just use the SQL package one
	cols := make([]sql.IndexColumn, len(i.Columns))
	copy(cols, i.Columns)
	return sql.IndexDef{
		Name:       i.IndexName,
		Columns:    cols,
		Constraint: i.Constraint,
		Storage:    i.Using,
		Comment:    i.Comment,
	}
}

// TableSpec is a node describing the schema of a table.
type TableSpec struct {
	Schema    sql.PrimaryKeySchema
	FkDefs    []*sql.ForeignKeyConstraint
	ChDefs    []*sql.CheckConstraint
	IdxDefs   []*IndexDefinition
	Collation sql.CollationID
	Comment   string
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
	ifNotExists  bool
	FkDefs       []*sql.ForeignKeyConstraint
	fkParentTbls []sql.ForeignKeyTable
	checks       sql.CheckConstraints
	IdxDefs      []*IndexDefinition
	Collation    sql.CollationID
	Comment      string
	like         sql.Node
	temporary    bool
	selectNode   sql.Node
}

var _ sql.Databaser = (*CreateTable)(nil)
var _ sql.Node = (*CreateTable)(nil)
var _ sql.Expressioner = (*CreateTable)(nil)
var _ sql.SchemaTarget = (*CreateTable)(nil)
var _ sql.CheckConstraintNode = (*CreateTable)(nil)
var _ sql.CollationCoercible = (*CreateTable)(nil)

// NewCreateTable creates a new CreateTable node
func NewCreateTable(db sql.Database, name string, ifn, temp bool, tableSpec *TableSpec) *CreateTable {
	for _, s := range tableSpec.Schema.Schema {
		s.Source = name
	}

	return &CreateTable{
		ddlNode:      ddlNode{db},
		name:         name,
		CreateSchema: tableSpec.Schema,
		FkDefs:       tableSpec.FkDefs,
		checks:       tableSpec.ChDefs,
		IdxDefs:      tableSpec.IdxDefs,
		Collation:    tableSpec.Collation,
		Comment:      tableSpec.Comment,
		ifNotExists:  ifn,
		temporary:    temp,
	}
}

// NewCreateTableSelect create a new CreateTable node for CREATE TABLE [AS] SELECT
func NewCreateTableSelect(db sql.Database, name string, selectNode sql.Node, tableSpec *TableSpec, ifn, temp bool) *CreateTable {
	for _, s := range tableSpec.Schema.Schema {
		s.Source = name
	}

	return &CreateTable{
		ddlNode:      ddlNode{Db: db},
		CreateSchema: tableSpec.Schema,
		FkDefs:       tableSpec.FkDefs,
		checks:       tableSpec.ChDefs,
		IdxDefs:      tableSpec.IdxDefs,
		name:         name,
		selectNode:   selectNode,
		ifNotExists:  ifn,
		temporary:    temp,
	}
}

func (c *CreateTable) Checks() sql.CheckConstraints {
	return c.checks
}

func (c *CreateTable) WithChecks(checks sql.CheckConstraints) sql.Node {
	ret := *c
	ret.checks = checks
	return &ret
}

// WithTargetSchema  implements the sql.TargetSchema interface.
func (c *CreateTable) WithTargetSchema(schema sql.Schema) (sql.Node, error) {
	return nil, fmt.Errorf("unable to set target schema without primary key info")
}

// TargetSchema implements the sql.TargetSchema interface.
func (c *CreateTable) TargetSchema() sql.Schema {
	return c.CreateSchema.Schema
}

// WithDatabase implements the sql.Databaser interface.
func (c *CreateTable) WithDatabase(db sql.Database) (sql.Node, error) {
	nc := *c
	nc.Db = db
	return &nc, nil
}

// Schema implements the sql.Node interface.
func (c *CreateTable) Schema() sql.Schema {
	return types.OkResultSchema
}

func (c *CreateTable) PkSchema() sql.PrimaryKeySchema {
	return c.CreateSchema
}

// Resolved implements the Resolvable interface.
func (c *CreateTable) Resolved() bool {
	if !c.ddlNode.Resolved() || !c.CreateSchema.Schema.Resolved() {
		return false
	}

	for _, chDef := range c.checks {
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

func (c *CreateTable) IsReadOnly() bool {
	return false
}

// ForeignKeys returns any foreign keys that will be declared on this table.
func (c *CreateTable) ForeignKeys() []*sql.ForeignKeyConstraint {
	return c.FkDefs
}

// WithParentForeignKeyTables adds the tables that are referenced in each foreign key. The table indices is assumed
// to match the foreign key indices in their respective slices.
func (c *CreateTable) WithParentForeignKeyTables(refTbls []sql.ForeignKeyTable) (*CreateTable, error) {
	if len(c.FkDefs) != len(refTbls) {
		return nil, fmt.Errorf("table `%s` defines `%d` foreign keys but found `%d` referenced tables",
			c.name, len(c.FkDefs), len(refTbls))
	}
	nc := *c
	nc.fkParentTbls = refTbls
	return &nc, nil
}

func (c *CreateTable) CreateForeignKeys(ctx *sql.Context, tableNode sql.Table) error {
	fkTbl, ok := tableNode.(sql.ForeignKeyTable)
	if !ok {
		return sql.ErrNoForeignKeySupport.New(c.name)
	}

	fkChecks, err := ctx.GetSessionVariable(ctx, "foreign_key_checks")
	if err != nil {
		return err
	}

	for i, fkDef := range c.FkDefs {
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
			err = ResolveForeignKey(ctx, fkTbl, fkParentTbl, *fkDef, true, true, true)
			if err != nil {
				return err
			}
		} else {
			// If foreign_key_checks are true, then the referenced tables will be populated
			err = ResolveForeignKey(ctx, fkTbl, nil, *fkDef, true, false, false)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *CreateTable) CreateChecks(ctx *sql.Context, tableNode sql.Table) error {
	chAlterable, ok := tableNode.(sql.CheckAlterableTable)
	if !ok {
		return ErrNoCheckConstraintSupport.New(c.name)
	}

	for _, ch := range c.checks {
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
	}
	if c.selectNode != nil {
		return []sql.Node{c.selectNode}
	}
	return nil
}

// WithChildren implements the Node interface.
func (c *CreateTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	nc := *c
	if len(children) == 0 {
		return &nc, nil
	}
	if len(children) == 1 {
		if c.like != nil {
			nc.like = children[0]
		} else {
			nc.selectNode = children[0]
		}
		return &nc, nil
	}
	return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
}

// CheckPrivileges implements the interface sql.Node.
func (c *CreateTable) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	priv := sql.PrivilegeType_Create
	if c.temporary {
		priv = sql.PrivilegeType_CreateTempTable
	}
	subject := sql.PrivilegeCheckSubject{Database: CheckPrivilegeNameForDatabase(c.Db)}
	return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(subject, priv))
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*CreateTable) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
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

	if c.selectNode != nil {
		p.WriteNode("Create table %s%s as", ifNotExists, c.name)
		p.WriteChildren(sql.DebugString(c.selectNode))
		return p.String()
	}

	p.WriteNode("Create table %s%s", ifNotExists, c.name)

	var children []string
	children = append(children, c.schemaDebugString())

	if len(c.FkDefs) > 0 {
		children = append(children, c.foreignKeysDebugString())
	}
	if len(c.IdxDefs) > 0 {
		children = append(children, c.indexesDebugString())
	}
	if len(c.checks) > 0 {
		children = append(children, c.checkConstraintsDebugString())
	}

	p.WriteChildren(children...)
	return p.String()
}

func (c *CreateTable) foreignKeysDebugString() string {
	p := sql.NewTreePrinter()
	p.WriteNode("ForeignKeys")
	var children []string
	for _, def := range c.FkDefs {
		children = append(children, sql.DebugString(def))
	}
	p.WriteChildren(children...)
	return p.String()
}

func (c *CreateTable) indexesDebugString() string {
	p := sql.NewTreePrinter()
	p.WriteNode("Indexes")
	var children []string
	for _, def := range c.IdxDefs {
		children = append(children, sql.DebugString(def))
	}
	p.WriteChildren(children...)
	return p.String()
}

func (c *CreateTable) checkConstraintsDebugString() string {
	p := sql.NewTreePrinter()
	p.WriteNode("CheckConstraints")
	var children []string
	for _, def := range c.checks {
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
	exprs := transform.WrappedColumnDefaults(c.CreateSchema.Schema)

	for _, ch := range c.checks {
		exprs = append(exprs, ch.Expr)
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
	ret = ret.WithForeignKeys(c.FkDefs)
	ret = ret.WithIndices(c.IdxDefs)
	ret = ret.WithCheckConstraints(c.checks)
	ret.Collation = c.Collation
	ret.Comment = c.Comment

	return ret
}

func (c *CreateTable) Name() string {
	return c.name
}

func (c *CreateTable) IfNotExists() bool {
	return c.ifNotExists
}

func (c *CreateTable) Temporary() bool {
	return c.temporary
}

func (c *CreateTable) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	schemaLen := len(c.CreateSchema.Schema)
	length := schemaLen + len(c.checks)
	if len(exprs) != length {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(exprs), length)
	}

	nc := *c

	// Make sure to make a deep copy of any slices here so we aren't modifying the original pointer
	ns, err := transform.SchemaWithDefaults(c.CreateSchema.Schema, exprs[:schemaLen])
	if err != nil {
		return nil, err
	}

	nc.CreateSchema = sql.NewPrimaryKeySchema(ns, c.CreateSchema.PkOrdinals...)

	ncd, err := c.checks.FromExpressions(exprs[schemaLen:])
	if err != nil {
		return nil, err
	}

	nc.checks = ncd
	return &nc, nil
}

func (c *CreateTable) ValidateDefaultPosition() error {
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
	TriggerNames []string
}

var _ sql.Node = (*DropTable)(nil)
var _ sql.CollationCoercible = (*DropTable)(nil)

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
	nd.TriggerNames = triggers
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

func (d *DropTable) IsReadOnly() bool {
	return false
}

// Schema implements the sql.Expression interface.
func (d *DropTable) Schema() sql.Schema {
	return types.OkResultSchema
}

// WithChildren implements the Node interface.
func (d *DropTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	// Number of children can be smaller than original as the non-existent
	// tables get filtered out in some cases
	var newChildren = make([]sql.Node, len(children))
	copy(newChildren, children)
	nd := *d
	nd.Tables = newChildren
	return &nd, nil
}

// CheckPrivileges implements the interface sql.Node.
func (d *DropTable) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	for _, tbl := range d.Tables {
		subject := sql.PrivilegeCheckSubject{
			Database: CheckPrivilegeNameForDatabase(GetDatabase(tbl)),
			Table:    getTableName(tbl),
		}

		if !opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Drop)) {
			return false
		}
	}
	return true
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*DropTable) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
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
