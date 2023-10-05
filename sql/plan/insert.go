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
	"strings"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// ErrInsertIntoNotSupported is thrown when a table doesn't support inserts
var ErrInsertIntoNotSupported = errors.NewKind("table doesn't support INSERT INTO")
var ErrReplaceIntoNotSupported = errors.NewKind("table doesn't support REPLACE INTO")
var ErrOnDuplicateKeyUpdateNotSupported = errors.NewKind("table doesn't support ON DUPLICATE KEY UPDATE")
var ErrAutoIncrementNotSupported = errors.NewKind("table doesn't support AUTO_INCREMENT")
var ErrInsertIntoUnsupportedValues = errors.NewKind("%T is unsupported for inserts")
var ErrInsertIntoDuplicateColumn = errors.NewKind("duplicate column name %v")
var ErrInsertIntoNonexistentColumn = errors.NewKind("invalid column name %v")
var ErrInsertIntoIncompatibleTypes = errors.NewKind("cannot convert type %s to %s")

// cc: https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sql-mode-strict
// The INSERT IGNORE syntax applies to these ignorable errors
// ER_BAD_NULL_ERROR - yes
// ER_DUP_ENTRY - yes
// ER_DUP_ENTRY_WITH_KEY_NAME - Yes
// ER_DUP_KEY - kinda
// ER_NO_PARTITION_FOR_GIVEN_VALUE - yes
// ER_NO_PARTITION_FOR_GIVEN_VALUE_SILENT - No
// ER_NO_REFERENCED_ROW_2 - Yes
// ER_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET - No
// ER_ROW_IS_REFERENCED_2 - Yes
// ER_SUBQUERY_NO_1_ROW - yes
// ER_VIEW_CHECK_FAILED - No
var IgnorableErrors = []*errors.Kind{sql.ErrInsertIntoNonNullableProvidedNull,
	sql.ErrPrimaryKeyViolation,
	sql.ErrPartitionNotFound,
	sql.ErrExpectedSingleRow,
	sql.ErrForeignKeyChildViolation,
	sql.ErrForeignKeyParentViolation,
	sql.ErrDuplicateEntry,
	sql.ErrUniqueKeyViolation,
	sql.ErrCheckConstraintViolated,
}

// InsertInto is the top level node for INSERT INTO statements. It has a source for rows and a destination to insert
// them into.
type InsertInto struct {
	db                    sql.Database
	Destination           sql.Node
	Source                sql.Node
	ColumnNames           []string
	IsReplace             bool
	HasUnspecifiedAutoInc bool
	OnDupExprs            []sql.Expression
	checks                sql.CheckConstraints
	Ignore                bool
}

var _ sql.Databaser = (*InsertInto)(nil)
var _ sql.Node = (*InsertInto)(nil)
var _ sql.Expressioner = (*InsertInto)(nil)
var _ sql.CollationCoercible = (*InsertInto)(nil)
var _ DisjointedChildrenNode = (*InsertInto)(nil)

// NewInsertInto creates an InsertInto node.
func NewInsertInto(db sql.Database, dst, src sql.Node, isReplace bool, cols []string, onDupExprs []sql.Expression, ignore bool) *InsertInto {
	return &InsertInto{
		db:          db,
		Destination: dst,
		Source:      src,
		ColumnNames: cols,
		IsReplace:   isReplace,
		OnDupExprs:  onDupExprs,
		Ignore:      ignore,
	}
}

var _ sql.CheckConstraintNode = (*RenameColumn)(nil)

func (ii *InsertInto) Checks() sql.CheckConstraints {
	return ii.checks
}

func (ii *InsertInto) WithChecks(checks sql.CheckConstraints) sql.Node {
	ret := *ii
	ret.checks = checks
	return &ret
}

func (ii *InsertInto) Dispose() {
	disposeNode(ii.Source)
}

// Schema implements the sql.Node interface.
// Insert nodes return rows that are inserted. Replaces return a concatenation of the deleted row and the inserted row.
// If no row was deleted, the value of those columns is nil.
func (ii *InsertInto) Schema() sql.Schema {
	if ii.IsReplace {
		return append(ii.Destination.Schema(), ii.Destination.Schema()...)
	}
	return ii.Destination.Schema()
}

func (ii *InsertInto) Children() []sql.Node {
	// The source node is analyzed completely independently, so we don't include it in children
	return []sql.Node{ii.Destination}
}

func (ii *InsertInto) Database() sql.Database {
	return ii.db
}

func (ii *InsertInto) IsReadOnly() bool {
	return false
}

func (ii *InsertInto) WithDatabase(database sql.Database) (sql.Node, error) {
	nc := *ii
	nc.db = database
	return &nc, nil
}

func (ii InsertInto) WithColumnNames(cols []string) *InsertInto {
	ii.ColumnNames = cols
	return &ii
}

// InsertDestination is a wrapper for a table to be used with InsertInto.Destination that allows the schema to be
// overridden. This is useful when the table in question has late-resolving column defaults.
type InsertDestination struct {
	UnaryNode
	DestinationName string
	Sch             sql.Schema
}

var _ sql.Node = (*InsertDestination)(nil)
var _ sql.Nameable = (*InsertDestination)(nil)
var _ sql.Expressioner = (*InsertDestination)(nil)
var _ sql.CollationCoercible = (*InsertDestination)(nil)

func NewInsertDestination(schema sql.Schema, node sql.Node) *InsertDestination {
	nameable := node.(sql.Nameable)
	return &InsertDestination{
		UnaryNode:       UnaryNode{Child: node},
		Sch:             schema,
		DestinationName: nameable.Name(),
	}
}

func (id *InsertDestination) Expressions() []sql.Expression {
	return transform.WrappedColumnDefaults(id.Sch)
}

func (id InsertDestination) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != len(id.Sch) {
		return nil, sql.ErrInvalidChildrenNumber.New(id, len(exprs), len(id.Sch))
	}

	sch, err := transform.SchemaWithDefaults(id.Sch, exprs)
	if err != nil {
		return nil, err
	}

	id.Sch = sch
	return &id, nil
}

func (id *InsertDestination) Name() string {
	return id.DestinationName
}

func (id *InsertDestination) IsReadOnly() bool {
	return true
}

func (id *InsertDestination) String() string {
	return id.UnaryNode.Child.String()
}

func (id *InsertDestination) DebugString() string {
	pr := sql.NewTreePrinter()
	pr.WriteNode("InsertDestination")
	var children []string
	for _, col := range id.Sch {
		children = append(children, sql.DebugString(col.Default))
	}
	children = append(children, sql.DebugString(id.Child))

	pr.WriteChildren(children...)

	return pr.String()
}

func (id *InsertDestination) Schema() sql.Schema {
	return id.Sch
}

func (id *InsertDestination) Resolved() bool {
	if !id.UnaryNode.Resolved() {
		return false
	}

	for _, col := range id.Sch {
		if !col.Default.Resolved() {
			return false
		}
	}

	return true
}

func (id InsertDestination) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(id, len(children), 1)
	}

	id.UnaryNode.Child = children[0]
	return &id, nil
}

// CheckPrivileges implements the interface sql.Node.
func (id *InsertDestination) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return id.Child.CheckPrivileges(ctx, opChecker)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (id *InsertDestination) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.GetCoercibility(ctx, id.Child)
}

// WithChildren implements the Node interface.
func (ii *InsertInto) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(ii, len(children), 1)
	}

	np := *ii
	np.Destination = children[0]
	return &np, nil
}

// CheckPrivileges implements the interface sql.Node.
func (ii *InsertInto) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	checkDbName := CheckPrivilegeNameForDatabase(ii.db)

	if ii.IsReplace {
		return opChecker.UserHasPrivileges(ctx,
			sql.NewPrivilegedOperation(checkDbName, getTableName(ii.Destination), "", sql.PrivilegeType_Insert, sql.PrivilegeType_Delete))
	} else {
		return opChecker.UserHasPrivileges(ctx,
			sql.NewPrivilegedOperation(checkDbName, getTableName(ii.Destination), "", sql.PrivilegeType_Insert))
	}
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*InsertInto) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// DisjointedChildren implements the interface DisjointedChildrenNode.
func (ii *InsertInto) DisjointedChildren() [][]sql.Node {
	return [][]sql.Node{
		{ii.Destination},
		{ii.Source},
	}
}

// WithDisjointedChildren implements the interface DisjointedChildrenNode.
func (ii *InsertInto) WithDisjointedChildren(children [][]sql.Node) (sql.Node, error) {
	if len(children) != 2 || len(children[0]) != 1 || len(children[1]) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(ii, len(children), 2)
	}
	np := *ii
	np.Destination = children[0][0]
	np.Source = children[1][0]
	return &np, nil
}

// WithSource sets the source node for this insert, which is analyzed separately
func (ii *InsertInto) WithSource(src sql.Node) *InsertInto {
	np := *ii
	np.Source = src
	return &np
}

// WithUnspecifiedAutoIncrement sets the unspecified auto increment flag for this insert operation. Inserts with this
// property set the LAST_INSERT_ID session variable, whereas inserts that manually specify values for an auto-insert
// column do not.
func (ii *InsertInto) WithUnspecifiedAutoIncrement(unspecifiedAutoIncrement bool) *InsertInto {
	np := *ii
	np.HasUnspecifiedAutoInc = unspecifiedAutoIncrement
	return &np
}

func (ii InsertInto) String() string {
	pr := sql.NewTreePrinter()
	if ii.IsReplace {
		_ = pr.WriteNode("Replace(%s)", strings.Join(ii.ColumnNames, ", "))
	} else {
		_ = pr.WriteNode("Insert(%s)", strings.Join(ii.ColumnNames, ", "))
	}
	_ = pr.WriteChildren(ii.Destination.String(), ii.Source.String())
	return pr.String()
}

func (ii InsertInto) DebugString() string {
	pr := sql.NewTreePrinter()
	if ii.IsReplace {
		_ = pr.WriteNode("Replace(%s)", strings.Join(ii.ColumnNames, ", "))
	} else {
		_ = pr.WriteNode("Insert(%s)", strings.Join(ii.ColumnNames, ", "))
	}
	_ = pr.WriteChildren(sql.DebugString(ii.Destination), sql.DebugString(ii.Source))
	return pr.String()
}

func (ii *InsertInto) Expressions() []sql.Expression {
	return append(ii.OnDupExprs, ii.checks.ToExpressions()...)
}

func (ii InsertInto) WithExpressions(newExprs ...sql.Expression) (sql.Node, error) {
	if len(newExprs) != len(ii.OnDupExprs)+len(ii.checks) {
		return nil, sql.ErrInvalidChildrenNumber.New(ii, len(newExprs), len(ii.OnDupExprs)+len(ii.checks))
	}

	ii.OnDupExprs = newExprs[:len(ii.OnDupExprs)]

	var err error
	ii.checks, err = ii.checks.FromExpressions(newExprs[len(ii.OnDupExprs):])
	if err != nil {
		return nil, err
	}

	return &ii, nil
}

// Resolved implements the Resolvable interface.
func (ii *InsertInto) Resolved() bool {
	if !ii.Destination.Resolved() || !ii.Source.Resolved() {
		return false
	}
	for _, updateExpr := range ii.OnDupExprs {
		if !updateExpr.Resolved() {
			return false
		}
	}
	for _, checkExpr := range ii.checks {
		if !checkExpr.Expr.Resolved() {
			return false
		}
	}
	return true
}

func GetInsertable(node sql.Node) (sql.InsertableTable, error) {
	switch node := node.(type) {
	case *Exchange:
		return GetInsertable(node.Child)
	case sql.InsertableTable:
		return node, nil
	case *ResolvedTable:
		return getInsertableTable(node.Table)
	case sql.TableWrapper:
		return getInsertableTable(node.Underlying())
	case *InsertDestination:
		return GetInsertable(node.Child)
	case *PrependNode:
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
