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

	"github.com/dolthub/go-mysql-server/sql/mysql_db"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/fulltext"
	"github.com/dolthub/go-mysql-server/sql/types"
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
	FkDefs       []*sql.ForeignKeyConstraint
	fkParentTbls []sql.ForeignKeyTable
	ChDefs       sql.CheckConstraints
	IdxDefs      []*IndexDefinition
	Collation    sql.CollationID
	like         sql.Node
	temporary    TempTableOption
	selectNode   sql.Node
}

var _ sql.Databaser = (*CreateTable)(nil)
var _ sql.Node = (*CreateTable)(nil)
var _ sql.Expressioner = (*CreateTable)(nil)
var _ sql.SchemaTarget = (*CreateTable)(nil)
var _ sql.CollationCoercible = (*CreateTable)(nil)

// NewCreateTable creates a new CreateTable node
func NewCreateTable(db sql.Database, name string, ifn IfNotExistsOption, temp TempTableOption, tableSpec *TableSpec) *CreateTable {
	for _, s := range tableSpec.Schema.Schema {
		s.Source = name
	}

	return &CreateTable{
		ddlNode:      ddlNode{db},
		name:         name,
		CreateSchema: tableSpec.Schema,
		FkDefs:       tableSpec.FkDefs,
		ChDefs:       tableSpec.ChDefs,
		IdxDefs:      tableSpec.IdxDefs,
		Collation:    tableSpec.Collation,
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
		ddlNode:      ddlNode{Db: db},
		CreateSchema: tableSpec.Schema,
		FkDefs:       tableSpec.FkDefs,
		ChDefs:       tableSpec.ChDefs,
		IdxDefs:      tableSpec.IdxDefs,
		name:         name,
		selectNode:   selectNode,
		ifNotExists:  ifn,
		temporary:    temp,
	}
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

	for _, chDef := range c.ChDefs {
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

func (c *CreateTable) CreateIndexes(ctx *sql.Context, tableNode sql.Table, idxes []*IndexDefinition) (err error) {
	idxAlterable, ok := tableNode.(sql.IndexAlterableTable)
	if !ok {
		return ErrNotIndexable.New()
	}

	indexMap := make(map[string]struct{})
	fulltextIndexes := make([]sql.IndexDef, 0, len(idxes))
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
		// We'll create the Full-Text indexes after all others
		if idxDef.Constraint == sql.IndexConstraint_Fulltext {
			otherDef := idxDef.AsIndexDef()
			otherDef.Name = indexName
			fulltextIndexes = append(fulltextIndexes, otherDef)
			continue
		}
		err := idxAlterable.CreateIndex(ctx, sql.IndexDef{
			Name:       indexName,
			Columns:    idxDef.Columns,
			Constraint: idxDef.Constraint,
			Storage:    idxDef.Using,
			Comment:    idxDef.Comment,
		})
		if err != nil {
			return err
		}
		indexMap[strings.ToLower(indexName)] = struct{}{}
	}

	// Evaluate our Full-Text indexes now
	if len(fulltextIndexes) > 0 {
		database, ok := c.Db.(fulltext.Database)
		if !ok {
			if privDb, ok := c.Db.(mysql_db.PrivilegedDatabase); ok {
				if database, ok = privDb.Unwrap().(fulltext.Database); !ok {
					return sql.ErrCreateTableNotSupported.New(c.Db.Name())
				}
			} else {
				return sql.ErrCreateTableNotSupported.New(c.Db.Name())
			}
		}
		if err = fulltext.CreateFulltextIndexes(ctx, database, idxAlterable, nil, fulltextIndexes...); err != nil {
			return err
		}
	}

	return nil
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
			err = ResolveForeignKey(ctx, fkTbl, fkParentTbl, *fkDef, true, true)
			if err != nil {
				return err
			}
		} else {
			// If foreign_key_checks are true, then the referenced tables will be populated
			err = ResolveForeignKey(ctx, fkTbl, nil, *fkDef, true, false)
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

	for _, ch := range c.ChDefs {
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

		if c.like != nil {
			c.like = child
		} else {
			c.selectNode = child
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
			sql.NewPrivilegedOperation(CheckPrivilegeNameForDatabase(c.Db), "", "", sql.PrivilegeType_CreateTempTable))
	} else {
		return opChecker.UserHasPrivileges(ctx,
			sql.NewPrivilegedOperation(CheckPrivilegeNameForDatabase(c.Db), "", "", sql.PrivilegeType_Create))
	}
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
	if len(c.ChDefs) > 0 {
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
	for _, def := range c.ChDefs {
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
	exprs := make([]sql.Expression, len(c.CreateSchema.Schema)+len(c.ChDefs))
	i := 0
	for _, col := range c.CreateSchema.Schema {
		exprs[i] = expression.WrapExpression(col.Default)
		i++
	}
	for _, ch := range c.ChDefs {
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
	ret = ret.WithForeignKeys(c.FkDefs)
	ret = ret.WithIndices(c.IdxDefs)
	ret = ret.WithCheckConstraints(c.ChDefs)
	ret.Collation = c.Collation

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
	length := len(c.CreateSchema.Schema) + len(c.ChDefs)
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

	ncd, err := c.ChDefs.FromExpressions(exprs[i:])
	if err != nil {
		return nil, err
	}

	nc.ChDefs = ncd
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
		// either *TableNode OR *UnresolvedTable here
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
		db := getDatabase(tbl)
		if !opChecker.UserHasPrivileges(ctx,
			sql.NewPrivilegedOperation(CheckPrivilegeNameForDatabase(db), getTableName(tbl), "", sql.PrivilegeType_Drop)) {
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
