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
)

var ErrNotView = errors.NewKind("'%' is not VIEW")

// ShowCreateTable is a node that shows the CREATE TABLE statement for a table.
type ShowCreateTable struct {
	*UnaryNode
	IsView  bool
	Indexes []sql.Index
	Checks  sql.CheckConstraints
}

// NewShowCreateTable creates a new ShowCreateTable node.
func NewShowCreateTable(table sql.Node, isView bool) sql.Node {
	return &ShowCreateTable{
		UnaryNode: &UnaryNode{table},
		IsView:    isView,
	}
}

// Resolved implements the Resolvable interface.
func (n *ShowCreateTable) Resolved() bool {
	return true
}

func (n *ShowCreateTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(1, len(children))
	}
	child := children[0]

	switch child.(type) {
	case *SubqueryAlias, *ResolvedTable, *UnresolvedTable:
	default:
		return nil, sql.ErrInvalidChildType.New(n, child, (*SubqueryAlias)(nil))
	}

	nc := *n
	nc.Child = child
	return &nc, nil
}

// Schema implements the Node interface.
func (n *ShowCreateTable) Schema() sql.Schema {
	switch n.Child.(type) {
	case *SubqueryAlias:
		return sql.Schema{
			&sql.Column{Name: "View", Type: sql.LongText, Nullable: false},
			&sql.Column{Name: "Create View", Type: sql.LongText, Nullable: false},
		}
	case *ResolvedTable, *UnresolvedTable:
		return sql.Schema{
			&sql.Column{Name: "Table", Type: sql.LongText, Nullable: false},
			&sql.Column{Name: "Create Table", Type: sql.LongText, Nullable: false},
		}
	default:
		panic(fmt.Sprintf("unexpected type %T", n.Child))
	}
}

// RowIter implements the Node interface
func (n *ShowCreateTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return &showCreateTablesIter{
		ctx:     ctx,
		table:   n.Child,
		isView:  n.IsView,
		indexes: n.Indexes,
		checks:  n.Checks,
	}, nil
}

// String implements the fmt.Stringer interface.
func (n *ShowCreateTable) String() string {
	t := "TABLE"
	if n.IsView {
		t = "VIEW"
	}

	name := ""
	if nameable, ok := n.Child.(sql.Nameable); ok {
		name = nameable.Name()
	}

	return fmt.Sprintf("SHOW CREATE %s %s", t, name)
}

type showCreateTablesIter struct {
	table        sql.Node
	didIteration bool
	isView       bool
	ctx          *sql.Context
	indexes      []sql.Index
	checks       sql.CheckConstraints
}

func (i *showCreateTablesIter) Next() (sql.Row, error) {
	if i.didIteration {
		return nil, io.EOF
	}

	i.didIteration = true

	var composedCreateTableStatement string
	var tableName string

	switch table := i.table.(type) {
	case *ResolvedTable:
		// MySQL behavior is to allow show create table for views, but not show create view for tables.
		if i.isView {
			return nil, ErrNotView.New(table.Name())
		}

		tableName = table.Name()
		var err error
		composedCreateTableStatement, err = i.produceCreateTableStatement(table.Table)
		if err != nil {
			return nil, err
		}
	case *SubqueryAlias:
		tableName = table.Name()
		composedCreateTableStatement = produceCreateViewStatement(table)
	default:
		panic(fmt.Sprintf("unexpected type %T", i.table))
	}

	return sql.NewRow(
		tableName,                    // "Table" string
		composedCreateTableStatement, // "Create Table" string
	), nil
}

type NameAndSchema interface {
	sql.Nameable
	Schema() sql.Schema
}

func (i *showCreateTablesIter) produceCreateTableStatement(table sql.Table) (string, error) {
	schema := table.Schema()
	colStmts := make([]string, len(schema))
	var primaryKeyCols []string

	// Statement creation parts for each column
	// TODO: rather than lower-casing here, we should do it in the String() method of types
	for i, col := range schema {
		stmt := fmt.Sprintf("  `%s` %s", col.Name, strings.ToLower(col.Type.String()))

		if !col.Nullable {
			stmt = fmt.Sprintf("%s NOT NULL", stmt)
		}

		if col.AutoIncrement {
			stmt = fmt.Sprintf("%s AUTO_INCREMENT", stmt)
		}

		if col.Default != nil {
			stmt = fmt.Sprintf("%s DEFAULT %s", stmt, col.Default.String())
		}

		if col.Comment != "" {
			stmt = fmt.Sprintf("%s COMMENT '%s'", stmt, col.Comment)
		}

		if col.PrimaryKey {
			primaryKeyCols = append(primaryKeyCols, col.Name)
		}

		colStmts[i] = stmt
	}

	// TODO: the order of the primary key columns might not match their order in the schema. The current interface can't
	//  represent this. We will need a new sql.Table extension to support this cleanly.
	if len(primaryKeyCols) > 0 {
		primaryKey := fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(quoteIdentifiers(primaryKeyCols), ","))
		colStmts = append(colStmts, primaryKey)
	}

	for _, index := range i.indexes {
		// The primary key may or may not be declared as an index by the table. Don't print it twice if it's here.
		if isPrimaryKeyIndex(index, table) {
			continue
		}

		var indexCols []string
		for _, expr := range index.Expressions() {
			col := GetColumnFromIndexExpr(expr, table)
			if col != nil {
				indexCols = append(indexCols, fmt.Sprintf("`%s`", col.Name))
			}
		}

		unique := ""
		if index.IsUnique() {
			unique = "UNIQUE "
		}

		key := fmt.Sprintf("  %sKEY `%s` (%s)", unique, index.ID(), strings.Join(indexCols, ","))
		if index.Comment() != "" {
			key = fmt.Sprintf("%s COMMENT '%s'", key, index.Comment())
		}

		colStmts = append(colStmts, key)
	}

	fkt := getForeignKeyTable(table)
	if fkt != nil {
		fks, err := fkt.GetForeignKeys(i.ctx)
		if err != nil {
			return "", err
		}
		for _, fk := range fks {
			keyCols := strings.Join(quoteIdentifiers(fk.Columns), ",")
			refCols := strings.Join(quoteIdentifiers(fk.ReferencedColumns), ",")
			onDelete := ""
			if len(fk.OnDelete) > 0 && fk.OnDelete != sql.ForeignKeyReferenceOption_DefaultAction {
				onDelete = " ON DELETE " + string(fk.OnDelete)
			}
			onUpdate := ""
			if len(fk.OnUpdate) > 0 && fk.OnUpdate != sql.ForeignKeyReferenceOption_DefaultAction {
				onUpdate = " ON UPDATE " + string(fk.OnUpdate)
			}
			colStmts = append(colStmts, fmt.Sprintf("  CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s` (%s)%s%s", fk.Name, keyCols, fk.ReferencedTable, refCols, onDelete, onUpdate))
		}
	}

	if i.checks != nil {
		for _, check := range i.checks {
			st, err := formatCheckExpression(check.Expr)
			if err != nil {
				return "", err
			}

			fmted := fmt.Sprintf("  CONSTRAINT `%s` CHECK (%s)", check.Name, st)

			if !check.Enforced {
				fmted += " /*!80016 NOT ENFORCED */"
			}

			colStmts = append(colStmts, fmted)
		}
	}

	return fmt.Sprintf(
		"CREATE TABLE `%s` (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		table.Name(),
		strings.Join(colStmts, ",\n"),
	), nil
}

// getForeignKeyTable returns the underlying ForeignKeyTable for the table given, or nil if it isn't a ForeignKeyTable
func getForeignKeyTable(t sql.Table) sql.ForeignKeyTable {
	switch t := t.(type) {
	case sql.ForeignKeyTable:
		return t
	case sql.TableWrapper:
		return getForeignKeyTable(t.Underlying())
	default:
		return nil
	}
}

func getChecksTable(t sql.Table) sql.CheckTable {
	switch t := t.(type) {
	case sql.CheckTable:
		return t
	case sql.TableWrapper:
		return getChecksTable(t.Underlying())
	default:
		return nil
	}
}

func quoteIdentifiers(ids []string) []string {
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = fmt.Sprintf("`%s`", id)
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

func formatCheckExpression(expr sql.Expression) (string, error) {
	switch t := expr.(type) {
	case *expression.And:
		leftChild := t.Left
		rightChild := t.Right

		lok := isSoloOperand(leftChild)
		rok := isSoloOperand(rightChild)

		if lok && rok {
			return fmt.Sprintf("%s %s %s", getCorrectStringFromSoloOperand(leftChild), "AND", getCorrectStringFromSoloOperand(rightChild)), nil
		}

		var left string
		var err error
		if !lok {
			left, err = formatCheckExpression(leftChild)
			if err != nil {
				return "", err
			}
		} else {
			left = getCorrectStringFromSoloOperand(leftChild)
		}

		var right string
		if !rok {
			right, err = formatCheckExpression(rightChild)
			if err != nil {
				return "", err
			}
		} else {
			right = getCorrectStringFromSoloOperand(rightChild)
		}

		_, sameOp := leftChild.(*expression.And)

		if sameOp {
			return fmt.Sprintf("(%s %s %s)", left, "AND", right), nil
		} else {
			return fmt.Sprintf("(%s %s (%s))", left, "AND", right), nil
		}
	case *expression.Or:
		leftChild := t.Left
		rightChild := t.Right

		lok := isSoloOperand(leftChild)
		rok := isSoloOperand(rightChild)

		if lok && rok {
			return fmt.Sprintf("%s %s %s", getCorrectStringFromSoloOperand(leftChild), "OR", getCorrectStringFromSoloOperand(rightChild)), nil
		}

		var left string
		var err error
		if !lok {
			left, err = formatCheckExpression(leftChild)
			if err != nil {
				return "", err
			}
		} else {
			left = getCorrectStringFromSoloOperand(leftChild)
		}

		var right string
		if !rok {
			right, err = formatCheckExpression(rightChild)
			if err != nil {
				return "", err
			}
		} else {
			right = getCorrectStringFromSoloOperand(rightChild)
		}

		_, sameOp := leftChild.(*expression.Or)

		if sameOp {
			return fmt.Sprintf("(%s %s %s)", left, "OR", right), nil
		} else {
			return fmt.Sprintf("(%s %s (%s))", left, "OR", right), nil
		}
	case expression.Comparer:
		if isSoloOperand(t) {
			return getCorrectStringFromSoloOperand(t), nil
		}

		return fmt.Sprintf("(%s %s (%s))", t.Left(), t.Expression(), t.Right()), nil
	default:
		return expr.String(), nil
	}
}

func isSoloOperand(expr sql.Expression) bool {
	switch t := expr.(type) {
	case *expression.UnresolvedColumn, *expression.Literal:
		return true
	case expression.Comparer:
		if _, ok := t.Left().(expression.Comparer); ok {
			return false
		}

		if _, ok := t.Right().(expression.Comparer); ok {
			return false
		}

		return true
	default:
		return false
	}
}

func getCorrectStringFromSoloOperand(expr sql.Expression) string {
	switch t := expr.(type) {
	case *expression.UnresolvedColumn:
		name := t.Name()
		strings.Replace(name, "`", "", -1)

		return fmt.Sprintf("`%s`", name)
	case expression.Comparer:
		return fmt.Sprintf("(%s %s %s)", getCorrectStringFromSoloOperand(t.Left()), t.Expression(), getCorrectStringFromSoloOperand(t.Right()))
	case *expression.Arithmetic:
		return fmt.Sprintf("(%s %s %s)", getCorrectStringFromSoloOperand(t.Left), t.Op, getCorrectStringFromSoloOperand(t.Right))
	default:
		return t.String()
	}
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
