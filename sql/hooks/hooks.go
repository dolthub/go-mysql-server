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

package hooks

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// Global is a variable that contains the interface for calling hooks. By default, this contains no-op hooks as
// integrators may implement their own hooks. This variable should be overwritten by the integrator.
var Global Hooks

// Hooks is an interface that represents various hooks that are called within a statement's lifecycle.
type Hooks interface {
	// Table returns hooks related to direct table statements.
	Table() Table
	// TableColumn returns hooks related to table column statements.
	TableColumn() TableColumn
}

// Table contains hooks related to direct table statements.
type Table interface {
	// Create returns hooks related to CREATE TABLE statements.
	Create() CreateTable
	// Rename returns hooks related to RENAME TABLE statements.
	Rename() RenameTable
	// Drop returns hooks related to DROP TABLE statements.
	Drop() DropTable
}

// TableColumn contains hooks related to table column statements.
type TableColumn interface {
	// Add returns hooks related to ALTER TABLE ... ADD COLUMN statements.
	Add() TableAddColumn
	// Rename returns hooks related to ALTER TABLE ... RENAME COLUMN statements.
	Rename() TableRenameColumn
	// Modify returns hooks related to ALTER TABLE ... MODIFY COLUMN statements.
	Modify() TableModifyColumn
	// Drop returns hooks related to ALTER TABLE ... DROP COLUMN statements.
	Drop() TableDropColumn
}

// CreateTable contains hooks related to CREATE TABLE statements.
type CreateTable interface {
	// PreSQLExecution is called before the final step of statement execution, after analysis.
	PreSQLExecution(*sql.Context, *plan.CreateTable) (*plan.CreateTable, error)
	// PostSQLExecution is called after the final step of statement execution, after analysis.
	PostSQLExecution(*sql.Context, *plan.CreateTable) error
}

// RenameTable contains hooks related to RENAME TABLE statements.
type RenameTable interface {
	// PreSQLExecution is called before the final step of statement execution, after analysis.
	PreSQLExecution(*sql.Context, *plan.RenameTable) (*plan.RenameTable, error)
	// PostSQLExecution is called after the final step of statement execution, after analysis.
	PostSQLExecution(*sql.Context, *plan.RenameTable) error
}

// DropTable contains hooks related to DROP TABLE statements.
type DropTable interface {
	// PreSQLExecution is called before the final step of statement execution, after analysis.
	PreSQLExecution(*sql.Context, *plan.DropTable) (*plan.DropTable, error)
	// PostSQLExecution is called after the final step of statement execution, after analysis.
	PostSQLExecution(*sql.Context, *plan.DropTable) error
}

// TableAddColumn contains hooks related to ALTER TABLE ... ADD COLUMN statements.
type TableAddColumn interface {
	// PreSQLExecution is called before the final step of statement execution, after analysis.
	PreSQLExecution(*sql.Context, *plan.AddColumn) (*plan.AddColumn, error)
	// PostSQLExecution is called after the final step of statement execution, after analysis.
	PostSQLExecution(*sql.Context, *plan.AddColumn) error
}

// TableRenameColumn contains hooks related to ALTER TABLE ... RENAME COLUMN statements.
type TableRenameColumn interface {
	// PreSQLExecution is called before the final step of statement execution, after analysis.
	PreSQLExecution(*sql.Context, *plan.RenameColumn) (*plan.RenameColumn, error)
	// PostSQLExecution is called after the final step of statement execution, after analysis.
	PostSQLExecution(*sql.Context, *plan.RenameColumn) error
}

// TableModifyColumn contains hooks related to ALTER TABLE ... MODIFY COLUMN statements.
type TableModifyColumn interface {
	// PreSQLExecution is called before the final step of statement execution, after analysis.
	PreSQLExecution(*sql.Context, *plan.ModifyColumn) (*plan.ModifyColumn, error)
	// PostSQLExecution is called after the final step of statement execution, after analysis.
	PostSQLExecution(*sql.Context, *plan.ModifyColumn) error
}

// TableDropColumn contains hooks related to ALTER TABLE ... DROP COLUMN statements.
type TableDropColumn interface {
	// PreSQLExecution is called before the final step of statement execution, after analysis.
	PreSQLExecution(*sql.Context, *plan.DropColumn) (*plan.DropColumn, error)
	// PostSQLExecution is called after the final step of statement execution, after analysis.
	PostSQLExecution(*sql.Context, *plan.DropColumn) error
}
