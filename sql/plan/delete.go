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

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

var ErrDeleteFromNotSupported = errors.NewKind("table doesn't support DELETE FROM")

// DeleteFrom is a node describing a deletion from some table.
type DeleteFrom struct {
	UnaryNode
	// targets are the explicitly specified table nodes from which rows should be deleted. For simple DELETES against a
	// single source table, targets do NOT need to be explicitly specified and will not be set here. For DELETE FROM JOIN
	// statements, targets MUST be explicitly specified by the user and will be populated here.
	explicitTargets []sql.Node
}

var _ sql.Databaseable = (*DeleteFrom)(nil)
var _ sql.Node = (*DeleteFrom)(nil)
var _ sql.CollationCoercible = (*DeleteFrom)(nil)

// NewDeleteFrom creates a DeleteFrom node.
func NewDeleteFrom(n sql.Node, targets []sql.Node) *DeleteFrom {
	return &DeleteFrom{
		UnaryNode:       UnaryNode{n},
		explicitTargets: targets,
	}
}

// HasExplicitTargets returns true if the target delete tables were explicitly specified. This can only happen with
// DELETE FROM JOIN statements – for DELETE FROM statements using a single source table, the target is NOT explicitly
// specified and is assumed to be the single source table.
func (p *DeleteFrom) HasExplicitTargets() bool {
	return len(p.explicitTargets) > 0
}

// WithExplicitTargets returns a new DeleteFrom node instance with the specified |targets| set as the explicitly
// specified targets of the delete operation.
func (p *DeleteFrom) WithExplicitTargets(targets []sql.Node) *DeleteFrom {
	copy := *p
	copy.explicitTargets = targets
	return &copy
}

// GetDeleteTargets returns the sql.Nodes representing the tables from which rows should be deleted. For a DELETE FROM
// JOIN statement, this will return the tables explicitly specified by the caller. For a DELETE FROM statement this will
// return the single table in the DELETE FROM source that is implicitly assumed to be the target of the delete operation.
func (p *DeleteFrom) GetDeleteTargets() []sql.Node {
	if len(p.explicitTargets) == 0 {
		return []sql.Node{p.Child}
	} else {
		return p.explicitTargets
	}
}

// Resolved implements the sql.Resolvable interface.
func (p *DeleteFrom) Resolved() bool {
	if p.Child.Resolved() == false {
		return false
	}

	for _, target := range p.explicitTargets {
		if target.Resolved() == false {
			return false
		}
	}

	return true
}

func (p *DeleteFrom) IsReadOnly() bool {
	return false
}

// DB returns the database being deleted from. |Database| is used by another interface we implement.
func (p *DeleteFrom) DB() sql.Database {
	return GetDatabase(p.Child)
}

func (p *DeleteFrom) Database() string {
	database := GetDatabase(p.Child)
	if database == nil {
		return ""
	}
	return database.Name()
}

// WithChildren implements the Node interface.
func (p *DeleteFrom) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewDeleteFrom(children[0], p.explicitTargets), nil
}

// CheckPrivileges implements the interface sql.Node.
func (p *DeleteFrom) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	// TODO: If column values are retrieved then the SELECT privilege is required
	//       For example: "DELETE FROM table WHERE z > 0"
	//       We would need SELECT privileges on the "z" column as it's retrieving values

	for _, target := range p.GetDeleteTargets() {
		deletable, err := GetDeletable(target)
		if err != nil {
			ctx.GetLogger().Warnf("unable to determine deletable table from delete target: %v", target)
			return false
		}

		db := GetDatabase(target)
		checkName := CheckPrivilegeNameForDatabase(db)
		op := sql.NewPrivilegedOperation(checkName, deletable.Name(), "", sql.PrivilegeType_Delete)
		if opChecker.UserHasPrivileges(ctx, op) == false {
			return false
		}
	}

	return true
}

func GetDeletable(node sql.Node) (sql.DeletableTable, error) {
	switch node := node.(type) {
	case sql.DeletableTable:
		return node, nil
	case *IndexedTableAccess:
		return GetDeletable(node.TableNode)
	case *ResolvedTable:
		return getDeletableTable(node.Table)
	case *TableAlias:
		return GetDeletable(node.Child)
	case *SubqueryAlias:
		return nil, ErrDeleteFromNotSupported.New()
	case *TriggerExecutor:
		return GetDeletable(node.Left())
	case sql.TableWrapper:
		return getDeletableTable(node.Underlying())
	case *JSONTable:
		return nil, fmt.Errorf("target table %s of the DELETE is not updatable", node.Name())
	}
	if len(node.Children()) > 1 {
		return nil, ErrDeleteFromNotSupported.New()
	}
	for _, child := range node.Children() {
		deleter, _ := GetDeletable(child)
		if deleter != nil {
			return deleter, nil
		}
	}
	return nil, ErrDeleteFromNotSupported.New()
}

func getDeletableTable(t sql.Table) (sql.DeletableTable, error) {
	switch t := t.(type) {
	case sql.DeletableTable:
		return t, nil
	case sql.TableWrapper:
		return getDeletableTable(t.Underlying())
	default:
		return nil, ErrDeleteFromNotSupported.New()
	}
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*DeleteFrom) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

func (p *DeleteFrom) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Delete")
	_ = pr.WriteChildren(p.Child.String())
	return pr.String()
}

func (p *DeleteFrom) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Delete")
	_ = pr.WriteChildren(sql.DebugString(p.Child))
	return pr.String()
}
