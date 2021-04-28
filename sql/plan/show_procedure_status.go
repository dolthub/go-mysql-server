// Copyright 2021 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql"
)

type ShowProcedureStatus struct {
	db         sql.Database
	Procedures []*Procedure
}

var _ sql.Databaser = (*ShowProcedureStatus)(nil)
var _ sql.Node = (*ShowProcedureStatus)(nil)

var showProcedureStatusSchema = sql.Schema{
	&sql.Column{Name: "Db", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "Name", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "Type", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "Definer", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "Modified", Type: sql.Datetime, Nullable: false},
	&sql.Column{Name: "Created", Type: sql.Datetime, Nullable: false},
	&sql.Column{Name: "Security_type", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "Comment", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "character_set_client", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "collation_connection", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "Database Collation", Type: sql.LongText, Nullable: false},
}

// NewShowProcedureStatus creates a new *ShowProcedureStatus node.
func NewShowProcedureStatus(db sql.Database) *ShowProcedureStatus {
	return &ShowProcedureStatus{
		db: db,
	}
}

// String implements the sql.Node interface.
func (s *ShowProcedureStatus) String() string {
	return "SHOW PROCEDURE STATUS"
}

// Resolved implements the sql.Node interface.
func (s *ShowProcedureStatus) Resolved() bool {
	_, ok := s.db.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the sql.Node interface.
func (s *ShowProcedureStatus) Children() []sql.Node {
	return nil
}

// Schema implements the sql.Node interface.
func (s *ShowProcedureStatus) Schema() sql.Schema {
	return showProcedureStatusSchema
}

// RowIter implements the sql.Node interface.
func (s *ShowProcedureStatus) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	var rows []sql.Row
	for _, procedure := range s.Procedures {
		securityType := "DEFINER"
		if procedure.SecurityContext == ProcedureSecurityContext_Invoker {
			securityType = "INVOKER"
		}
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
		rows = append(rows, sql.Row{
			s.db.Name(),                // Db
			procedure.Name,             // Name
			"PROCEDURE",                // Type
			procedure.Definer,          // Definer
			procedure.ModifiedAt.UTC(), // Modified
			procedure.CreatedAt.UTC(),  // Created
			securityType,               // Security_type
			procedure.Comment,          // Comment
			characterSetClient,         // character_set_client
			collationConnection,        // collation_connection
			collationServer,            // Database Collation
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

// WithChildren implements the sql.Node interface.
func (s *ShowProcedureStatus) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(s, children...)
}

// Database implements the sql.Databaser interface.
func (s *ShowProcedureStatus) Database() sql.Database {
	return s.db
}

// WithDatabase implements the sql.Databaser interface.
func (s *ShowProcedureStatus) WithDatabase(db sql.Database) (sql.Node, error) {
	ns := *s
	ns.db = db
	return &ns, nil
}
