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
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var ErrNotView = errors.NewKind("'%' is not VIEW")

// ShowCreateTable is a node that shows the CREATE TABLE statement for a table.
type ShowCreateTable struct {
	*UnaryNode
	IsView           bool
	Indexes          []sql.Index
	Checks           sql.CheckConstraints
	targetSchema     sql.Schema
	primaryKeySchema sql.PrimaryKeySchema
	asOf             sql.Expression
}

var _ sql.Node = (*ShowCreateTable)(nil)
var _ sql.Expressioner = (*ShowCreateTable)(nil)
var _ sql.SchemaTarget = (*ShowCreateTable)(nil)
var _ Versionable = (*ShowCreateTable)(nil)

// NewShowCreateTable creates a new ShowCreateTable node.
func NewShowCreateTable(table sql.Node, isView bool) *ShowCreateTable {
	return NewShowCreateTableWithAsOf(table, isView, nil)
}

// NewShowCreateTableWithAsOf creates a new ShowCreateTable node for a specific version of a table.
func NewShowCreateTableWithAsOf(table sql.Node, isView bool, asOf sql.Expression) *ShowCreateTable {
	return &ShowCreateTable{
		UnaryNode: &UnaryNode{table},
		IsView:    isView,
		asOf:      asOf,
	}
}

// Resolved implements the Resolvable interface.
func (sc *ShowCreateTable) Resolved() bool {
	if !sc.Child.Resolved() {
		return false
	}

	for _, col := range sc.targetSchema {
		if !col.Default.Resolved() {
			return false
		}
	}

	return true
}

func (sc ShowCreateTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(1, len(children))
	}
	child := children[0]

	switch child.(type) {
	case *SubqueryAlias, *ResolvedTable, sql.UnresolvedTable:
	default:
		return nil, sql.ErrInvalidChildType.New(sc, child, (*SubqueryAlias)(nil))
	}

	sc.Child = child
	return &sc, nil
}

// CheckPrivileges implements the interface sql.Node.
func (sc *ShowCreateTable) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	// The table won't be visible during the resolution step if the user doesn't have the correct privileges
	return true
}

func (sc ShowCreateTable) WithTargetSchema(schema sql.Schema) (sql.Node, error) {
	sc.targetSchema = schema
	return &sc, nil
}

func (sc *ShowCreateTable) TargetSchema() sql.Schema {
	return sc.targetSchema
}

func (sc ShowCreateTable) WithPrimaryKeySchema(schema sql.PrimaryKeySchema) (sql.Node, error) {
	sc.primaryKeySchema = schema
	return &sc, nil
}

func (sc *ShowCreateTable) Expressions() []sql.Expression {
	return transform.WrappedColumnDefaults(sc.targetSchema)
}

func (sc ShowCreateTable) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(sc.targetSchema) {
		return nil, sql.ErrInvalidChildrenNumber.New(sc, len(exprs), len(sc.targetSchema))
	}

	sc.targetSchema = transform.SchemaWithDefaults(sc.targetSchema, exprs)
	return &sc, nil
}

// Schema implements the Node interface.
func (sc *ShowCreateTable) Schema() sql.Schema {
	switch sc.Child.(type) {
	case *SubqueryAlias:
		return sql.Schema{
			&sql.Column{Name: "View", Type: types.LongText, Nullable: false},
			&sql.Column{Name: "Create View", Type: types.LongText, Nullable: false},
			&sql.Column{Name: "character_set_client", Type: types.LongText, Nullable: false},
			&sql.Column{Name: "collation_connection", Type: types.LongText, Nullable: false},
		}
	case *ResolvedTable, sql.UnresolvedTable:
		return sql.Schema{
			&sql.Column{Name: "Table", Type: types.LongText, Nullable: false},
			&sql.Column{Name: "Create Table", Type: types.LongText, Nullable: false},
		}
	default:
		panic(fmt.Sprintf("unexpected type %T", sc.Child))
	}
}

// GetTargetSchema returns the final resolved target schema of show create table.
func (sc *ShowCreateTable) GetTargetSchema() sql.Schema {
	return sc.targetSchema
}

// WithAsOf implements the Versionable interface.
func (sc *ShowCreateTable) WithAsOf(asOf sql.Expression) (sql.Node, error) {
	nsc := *sc
	nsc.asOf = asOf
	return &nsc, nil
}

// AsOf implements the Versionable interface.
func (sc *ShowCreateTable) AsOf() sql.Expression {
	return sc.asOf
}

// RowIter implements the Node interface
func (sc *ShowCreateTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return &showCreateTablesIter{
		table:    sc.Child,
		isView:   sc.IsView,
		indexes:  sc.Indexes,
		checks:   sc.Checks,
		schema:   sc.targetSchema,
		pkSchema: sc.primaryKeySchema,
	}, nil
}

// String implements the fmt.Stringer interface.
func (sc *ShowCreateTable) String() string {
	t := "TABLE"
	if sc.IsView {
		t = "VIEW"
	}

	name := ""
	if nameable, ok := sc.Child.(sql.Nameable); ok {
		name = nameable.Name()
	}

	asOfClause := ""
	if sc.asOf != nil {
		asOfClause = fmt.Sprintf("as of %v", sc.asOf)
	}

	return fmt.Sprintf("SHOW CREATE %s %s %s", t, name, asOfClause)
}

type showCreateTablesIter struct {
	table        sql.Node
	schema       sql.Schema
	didIteration bool
	isView       bool
	indexes      []sql.Index
	checks       sql.CheckConstraints
	pkSchema     sql.PrimaryKeySchema
}

func (i *showCreateTablesIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.didIteration {
		return nil, io.EOF
	}

	i.didIteration = true

	var row sql.Row
	switch table := i.table.(type) {
	case *ResolvedTable:
		// MySQL behavior is to allow show create table for views, but not show create view for tables.
		if i.isView {
			return nil, ErrNotView.New(table.Name())
		}

		composedCreateTableStatement, err := i.produceCreateTableStatement(ctx, table.Table, i.schema, i.pkSchema)
		if err != nil {
			return nil, err
		}
		row = sql.NewRow(
			table.Name(),                 // "Table" string
			composedCreateTableStatement, // "Create Table" string
		)
	case *SubqueryAlias:
		characterSetClient, err := ctx.GetSessionVariable(ctx, "character_set_client")
		if err != nil {
			return nil, err
		}
		collationConnection, err := ctx.GetSessionVariable(ctx, "collation_connection")
		if err != nil {
			return nil, err
		}
		row = sql.NewRow(
			table.Name(),                      // "View" string
			produceCreateViewStatement(table), // "Create View" string
			characterSetClient,
			collationConnection,
		)
	default:
		panic(fmt.Sprintf("unexpected type %T", i.table))
	}

	return row, nil
}

type NameAndSchema interface {
	sql.Nameable
	Schema() sql.Schema
}

func (i *showCreateTablesIter) produceCreateTableStatement(ctx *sql.Context, table sql.Table, schema sql.Schema, pkSchema sql.PrimaryKeySchema) (string, error) {
	colStmts := make([]string, len(schema))
	var primaryKeyCols []string

	var pkOrdinals []int
	if len(pkSchema.Schema) > 0 {
		pkOrdinals = pkSchema.PkOrdinals
	}

	// Statement creation parts for each column
	for i, col := range schema {

		var colDefault string
		// TODO: The columns that are rendered in defaults should be backticked
		if col.Default != nil {
			// TODO : string literals should have character set introducer
			colDefault = col.Default.String()
			if colDefault != "NULL" && col.Default.IsLiteral() && !types.IsTime(col.Default.Type()) && !types.IsText(col.Default.Type()) {
				v, err := col.Default.Eval(ctx, nil)
				if err != nil {
					return "", err
				}
				colDefault = fmt.Sprintf("'%v'", v)
			}
		}

		if col.PrimaryKey && len(pkSchema.Schema) == 0 {
			pkOrdinals = append(pkOrdinals, i)
		}

		colStmts[i] = FmtCreateTableColumn(col.Name, col.Type, col.Nullable, col.AutoIncrement, col.Default != nil, colDefault, col.Comment)
	}

	for _, i := range pkOrdinals {
		primaryKeyCols = append(primaryKeyCols, schema[i].Name)
	}

	if len(primaryKeyCols) > 0 {
		colStmts = append(colStmts, FmtCreateTablePrimaryKey(primaryKeyCols))
	}

	for _, index := range i.indexes {
		// The primary key may or may not be declared as an index by the table. Don't print it twice if it's here.
		if isPrimaryKeyIndex(index, table) {
			continue
		}

		prefixLengths := index.PrefixLengths()
		var indexCols []string
		for i, expr := range index.Expressions() {
			col := GetColumnFromIndexExpr(expr, table)
			if col != nil {
				indexDef := quoteIdentifier(col.Name)
				if len(prefixLengths) > i && prefixLengths[i] != 0 {
					indexDef += fmt.Sprintf("(%v)", prefixLengths[i])
				}
				indexCols = append(indexCols, indexDef)
			}
		}

		colStmts = append(colStmts, FmtCreateTableIndex(index.IsUnique(), index.IsSpatial(), index.ID(), indexCols, index.Comment()))
	}

	fkt, err := getForeignKeyTable(table)
	if err == nil && fkt != nil {
		fks, err := fkt.GetDeclaredForeignKeys(ctx)
		if err != nil {
			return "", err
		}
		for _, fk := range fks {
			onDelete := ""
			if len(fk.OnDelete) > 0 && fk.OnDelete != sql.ForeignKeyReferentialAction_DefaultAction {
				onDelete = string(fk.OnDelete)
			}
			onUpdate := ""
			if len(fk.OnUpdate) > 0 && fk.OnUpdate != sql.ForeignKeyReferentialAction_DefaultAction {
				onUpdate = string(fk.OnUpdate)
			}
			colStmts = append(colStmts, FmtCreateTableForiegnKey(fk.Name, fk.Columns, fk.ParentTable, fk.ParentColumns, onDelete, onUpdate))
		}
	}

	if i.checks != nil {
		for _, check := range i.checks {
			colStmts = append(colStmts, FmtCreateTableCheckConstraint(check.Name, check.Expr.String(), check.Enforced))
		}
	}

	return CreateTableFmt(table.Name(), colStmts, table.Collation().CharacterSet().Name(), table.Collation().Name()), nil
}

// quoteIdentifier wraps the specified identifier in backticks and escapes all occurrences of backticks in the
// identifier by replacing them with double backticks.
func quoteIdentifier(id string) string {
	id = strings.ReplaceAll(id, "`", "``")
	return fmt.Sprintf("`%s`", id)
}

// quoteIdentifiers wraps each of the specified identifiers in backticks, escapes all occurrences of backticks in
// the identifier, and returns a slice of the quoted identifiers.
func quoteIdentifiers(ids []string) []string {
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = quoteIdentifier(id)
	}
	return quoted
}

// isPrimaryKeyIndex returns whether the index given matches the table's primary key columns. Order is not considered.
func isPrimaryKeyIndex(index sql.Index, table sql.Table) bool {
	var pks []*sql.Column

	for _, col := range table.Schema() {
		if col.PrimaryKey {
			pks = append(pks, col)
		}
	}

	if len(index.Expressions()) != len(pks) {
		return false
	}

	for _, expr := range index.Expressions() {
		if col := GetColumnFromIndexExpr(expr, table); col != nil {
			found := false
			for _, pk := range pks {
				if col == pk {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		} else {
			return false
		}
	}

	return true
}

func produceCreateViewStatement(view *SubqueryAlias) string {
	return fmt.Sprintf(
		"CREATE VIEW `%s` AS %s",
		view.Name(),
		view.TextDefinition,
	)
}

func (i *showCreateTablesIter) Close(*sql.Context) error {
	return nil
}

func CreateTableFmt(tblName string, colStmts []string, tblCharsetName, tblCollName string) string {
	return fmt.Sprintf(
		"CREATE TABLE %s (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
		quoteIdentifier(tblName),
		strings.Join(colStmts, ",\n"),
		tblCharsetName,
		tblCollName,
	)
}

func FmtCreateTableColumn(colName string, colType sql.Type, nullable bool, autoInc bool, hasDefault bool, colDefault string, comment string) string {
	stmt := fmt.Sprintf("  %s %s", quoteIdentifier(colName), colType.String())
	if !nullable {
		stmt = fmt.Sprintf("%s NOT NULL", stmt)
	}
	if autoInc {
		stmt = fmt.Sprintf("%s AUTO_INCREMENT", stmt)
	}
	if c, ok := colType.(sql.SpatialColumnType); ok {
		if s, d := c.GetSpatialTypeSRID(); d {
			stmt = fmt.Sprintf("%s SRID %v", stmt, s)
		}
	}
	if hasDefault {
		stmt = fmt.Sprintf("%s DEFAULT %s", stmt, colDefault)
	}
	if comment != "" {
		stmt = fmt.Sprintf("%s COMMENT '%s'", stmt, comment)
	}
	return stmt
}

func FmtCreateTablePrimaryKey(pkCols []string) string {
	return fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(quoteIdentifiers(pkCols), ","))
}

func FmtCreateTableIndex(isUnique, isSpatial bool, indexID string, indexCols []string, comment string) string {
	unique := ""
	if isUnique {
		unique = "UNIQUE "
	}

	spatial := ""
	if isSpatial {
		unique = "SPATIAL "
	}
	key := fmt.Sprintf("  %s%sKEY %s (%s)", unique, spatial, quoteIdentifier(indexID), strings.Join(indexCols, ","))
	if comment != "" {
		key = fmt.Sprintf("%s COMMENT '%s'", key, comment)
	}
	return key
}

func FmtCreateTableForiegnKey(fkName string, fkCols []string, parentTbl string, parentCols []string, onDelete, onUpdate string) string {
	keyCols := strings.Join(quoteIdentifiers(fkCols), ",")
	refCols := strings.Join(quoteIdentifiers(parentCols), ",")
	fkey := fmt.Sprintf("  CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)", quoteIdentifier(fkName), keyCols, quoteIdentifier(parentTbl), refCols)
	if onDelete != "" {
		fkey = fmt.Sprintf("%s ON DELETE %s", fkey, onDelete)
	}
	if onUpdate != "" {
		fkey = fmt.Sprintf("%s ON UPDATE %s", fkey, onUpdate)
	}
	return fkey
}

func FmtCreateTableCheckConstraint(checkName, checkExpr string, enforced bool) string {
	cc := fmt.Sprintf("  CONSTRAINT %s CHECK (%s)", quoteIdentifier(checkName), checkExpr)
	if !enforced {
		cc = fmt.Sprintf("%s /*!80016 NOT ENFORCED */", cc)
	}
	return cc
}
