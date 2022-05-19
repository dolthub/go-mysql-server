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

	"github.com/dolthub/go-mysql-server/sql"
)

type ShowCreateProcedure struct {
	db            sql.Database
	ProcedureName string
}

var _ sql.Databaser = (*ShowCreateProcedure)(nil)
var _ sql.Node = (*ShowCreateProcedure)(nil)

var showCreateProcedureSchema = sql.Schema{
	&sql.Column{Name: "Procedure", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "sql_mode", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "Create Procedure", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "character_set_client", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "collation_connection", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "Database Collation", Type: sql.LongText, Nullable: false},
}

// NewShowCreateProcedure creates a new ShowCreateProcedure node for SHOW CREATE PROCEDURE statements.
func NewShowCreateProcedure(db sql.Database, procedure string) *ShowCreateProcedure {
	return &ShowCreateProcedure{
		db:            db,
		ProcedureName: strings.ToLower(procedure),
	}
}

// String implements the sql.Node interface.
func (s *ShowCreateProcedure) String() string {
	return fmt.Sprintf("SHOW CREATE PROCEDURE %s", s.ProcedureName)
}

// Resolved implements the sql.Node interface.
func (s *ShowCreateProcedure) Resolved() bool {
	_, ok := s.db.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the sql.Node interface.
func (s *ShowCreateProcedure) Children() []sql.Node {
	return nil
}

// Schema implements the sql.Node interface.
func (s *ShowCreateProcedure) Schema() sql.Schema {
	return showCreateProcedureSchema
}

// RowIter implements the sql.Node interface.
func (s *ShowCreateProcedure) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	if externalProcedureDb, ok := s.db.(sql.ExternalStoredProcedureDatabase); ok {
		externalProcedures, err := externalProcedureDb.GetExternalStoredProcedures(ctx)
		if err != nil {
			return nil, err
		}
		for _, procedure := range externalProcedures {
			if strings.ToLower(procedure.Name) == s.ProcedureName {
				characterSetClient, err := ctx.GetSessionVariable(ctx, "character_set_client")
				if err != nil {
					return nil, err
				}
				collationConnection, err := ctx.GetSessionVariable(ctx, "collation_connection")
				if err != nil {
					return nil, err
				}
				collationServer, err := ctx.GetSessionVariable(ctx, "collation_server")
				if err != nil {
					return nil, err
				}

				return sql.RowsToRowIter(sql.Row{
					procedure.Name, // Procedure
					"",             // sql_mode
					procedure.FakeCreateProcedureStmt(externalProcedureDb.Name()), // Create Procedure
					characterSetClient,  // character_set_client
					collationConnection, // collation_connection
					collationServer,     // Database Collation
				}), nil
			}
		}
	}
	procedureDb, ok := s.db.(sql.StoredProcedureDatabase)
	if !ok {
		return nil, sql.ErrStoredProceduresNotSupported.New(s.db.Name())
	}
	procedures, err := procedureDb.GetStoredProcedures(ctx)
	if err != nil {
		return nil, err
	}
	for _, procedure := range procedures {
		if strings.ToLower(procedure.Name) == s.ProcedureName {
			characterSetClient, err := ctx.GetSessionVariable(ctx, "character_set_client")
			if err != nil {
				return nil, err
			}
			collationConnection, err := ctx.GetSessionVariable(ctx, "collation_connection")
			if err != nil {
				return nil, err
			}
			collationServer, err := ctx.GetSessionVariable(ctx, "collation_server")
			if err != nil {
				return nil, err
			}
			return sql.RowsToRowIter(sql.Row{
				procedure.Name,            // Procedure
				"",                        // sql_mode
				procedure.CreateStatement, // Create Procedure
				characterSetClient,        // character_set_client
				collationConnection,       // collation_connection
				collationServer,           // Database Collation
			}), nil
		}
	}
	return nil, sql.ErrStoredProcedureDoesNotExist.New(s.ProcedureName)
}

// WithChildren implements the sql.Node interface.
func (s *ShowCreateProcedure) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(s, children...)
}

// CheckPrivileges implements the interface sql.Node.
func (s *ShowCreateProcedure) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	// TODO: set definer
	// TODO: dynamic privilege SHOW ROUTINE
	// According to: https://dev.mysql.com/doc/refman/8.0/en/show-create-procedure.html
	// Must have SELECT, SHOW_ROUTINE, CREATE_ROUTINE, ALTER_ROUTINE, or EXECUTE privileges.
	return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation("", "", "", sql.PrivilegeType_Select)) ||
		opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(s.db.Name(), "", "", sql.PrivilegeType_CreateRoutine)) ||
		opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(s.db.Name(), "", "", sql.PrivilegeType_AlterRoutine)) ||
		opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(s.db.Name(), "", "", sql.PrivilegeType_Execute))
}

// Database implements the sql.Databaser interface.
func (s *ShowCreateProcedure) Database() sql.Database {
	return s.db
}

// WithDatabase implements the sql.Databaser interface.
func (s *ShowCreateProcedure) WithDatabase(db sql.Database) (sql.Node, error) {
	ns := *s
	ns.db = db
	return &ns, nil
}
