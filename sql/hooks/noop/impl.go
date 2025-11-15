// Copyright 2025 Dolthub, Inc.
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

package noop

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/hooks"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// init sets the global hooks to be no-ops by default. It is intended that the variable will be replaced by the
// integrator.
func init() {
	hooks.Global = NoOp{}
}

// NoOp implements hooks.Hooks while having all operations be no-ops. Integrators may supply their own implementation.
type NoOp struct{}

var _ hooks.Hooks = NoOp{}

// Table implements the interface hooks.Hooks.
func (NoOp) Table() hooks.Table {
	return Table{}
}

// TableColumn implements the interface hooks.Hooks.
func (NoOp) TableColumn() hooks.TableColumn {
	return TableColumn{}
}

// Table implements no-ops for the hooks.Table interface.
type Table struct{}

var _ hooks.Table = Table{}

// Create implements the interface hooks.Table.
func (Table) Create() hooks.CreateTable {
	return CreateTable{}
}

// Rename implements the interface hooks.Table.
func (Table) Rename() hooks.RenameTable {
	return RenameTable{}
}

// Drop implements the interface hooks.Table.
func (Table) Drop() hooks.DropTable {
	return DropTable{}
}

// TableColumn implements no-ops for the hooks.TableColumn interface.
type TableColumn struct{}

var _ hooks.TableColumn = TableColumn{}

// Add implements the interface hooks.TableColumn.
func (TableColumn) Add() hooks.TableAddColumn {
	return TableAddColumn{}
}

// Rename implements the interface hooks.TableColumn.
func (TableColumn) Rename() hooks.TableRenameColumn {
	return TableRenameColumn{}
}

// Modify implements the interface hooks.TableColumn.
func (TableColumn) Modify() hooks.TableModifyColumn {
	return TableModifyColumn{}
}

// Drop implements the interface hooks.TableColumn.
func (TableColumn) Drop() hooks.TableDropColumn {
	return TableDropColumn{}
}

// CreateTable implements no-ops for the hooks.CreateTable interface.
type CreateTable struct{}

var _ hooks.CreateTable = CreateTable{}

// PreSQLExecution implements the interface hooks.CreateTable.
func (CreateTable) PreSQLExecution(_ *sql.Context, n *plan.CreateTable) (*plan.CreateTable, error) {
	return n, nil
}

// PostSQLExecution implements the interface hooks.CreateTable.
func (CreateTable) PostSQLExecution(_ *sql.Context, n *plan.CreateTable) error {
	return nil
}

// RenameTable implements no-ops for the hooks.RenameTable interface.
type RenameTable struct{}

var _ hooks.RenameTable = RenameTable{}

// PreSQLExecution implements the interface hooks.RenameTable.
func (RenameTable) PreSQLExecution(_ *sql.Context, n *plan.RenameTable) (*plan.RenameTable, error) {
	return n, nil
}

// PostSQLExecution implements the interface hooks.RenameTable.
func (RenameTable) PostSQLExecution(_ *sql.Context, n *plan.RenameTable) error {
	return nil
}

// DropTable implements no-ops for the hooks.DropTable interface.
type DropTable struct{}

var _ hooks.DropTable = DropTable{}

// PreSQLExecution implements the interface hooks.DropTable.
func (DropTable) PreSQLExecution(_ *sql.Context, n *plan.DropTable) (*plan.DropTable, error) {
	return n, nil
}

// PostSQLExecution implements the interface hooks.DropTable.
func (DropTable) PostSQLExecution(_ *sql.Context, n *plan.DropTable) error {
	return nil
}

// TableAddColumn implements no-ops for the hooks.TableAddColumn interface.
type TableAddColumn struct{}

var _ hooks.TableAddColumn = TableAddColumn{}

// PreSQLExecution implements the interface hooks.TableAddColumn.
func (TableAddColumn) PreSQLExecution(_ *sql.Context, n *plan.AddColumn) (*plan.AddColumn, error) {
	return n, nil
}

// PostSQLExecution implements the interface hooks.TableAddColumn.
func (TableAddColumn) PostSQLExecution(_ *sql.Context, n *plan.AddColumn) error {
	return nil
}

// TableRenameColumn implements no-ops for the hooks.TableRenameColumn interface.
type TableRenameColumn struct{}

var _ hooks.TableRenameColumn = TableRenameColumn{}

// PreSQLExecution implements the interface hooks.TableRenameColumn.
func (TableRenameColumn) PreSQLExecution(_ *sql.Context, n *plan.RenameColumn) (*plan.RenameColumn, error) {
	return n, nil
}

// PostSQLExecution implements the interface hooks.TableRenameColumn.
func (TableRenameColumn) PostSQLExecution(_ *sql.Context, n *plan.RenameColumn) error {
	return nil
}

// TableModifyColumn implements no-ops for the hooks.TableModifyColumn interface.
type TableModifyColumn struct{}

var _ hooks.TableModifyColumn = TableModifyColumn{}

// PreSQLExecution implements the interface hooks.TableModifyColumn.
func (TableModifyColumn) PreSQLExecution(_ *sql.Context, n *plan.ModifyColumn) (*plan.ModifyColumn, error) {
	return n, nil
}

// PostSQLExecution implements the interface hooks.TableModifyColumn.
func (TableModifyColumn) PostSQLExecution(_ *sql.Context, n *plan.ModifyColumn) error {
	return nil
}

// TableDropColumn implements no-ops for the hooks.TableDropColumn interface.
type TableDropColumn struct{}

var _ hooks.TableDropColumn = TableDropColumn{}

// PreSQLExecution implements the interface hooks.TableDropColumn.
func (TableDropColumn) PreSQLExecution(_ *sql.Context, n *plan.DropColumn) (*plan.DropColumn, error) {
	return n, nil
}

// PostSQLExecution implements the interface hooks.TableDropColumn.
func (TableDropColumn) PostSQLExecution(_ *sql.Context, n *plan.DropColumn) error {
	return nil
}
