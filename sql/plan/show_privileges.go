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
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ShowPrivileges represents the statement SHOW PRIVILEGES.
type ShowPrivileges struct{}

var _ sql.Node = (*ShowPrivileges)(nil)
var _ sql.CollationCoercible = (*ShowPrivileges)(nil)

// NewShowPrivileges returns a new ShowPrivileges node.
func NewShowPrivileges() *ShowPrivileges {
	return &ShowPrivileges{}
}

// Schema implements the interface sql.Node.
func (n *ShowPrivileges) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "Privilege", Type: types.LongText},
		&sql.Column{Name: "Context", Type: types.LongText},
		&sql.Column{Name: "Comment", Type: types.LongText},
	}
}

// String implements the interface sql.Node.
func (n *ShowPrivileges) String() string {
	return "SHOW PRIVILEGES"
}

// Resolved implements the interface sql.Node.
func (n *ShowPrivileges) Resolved() bool {
	return true
}

func (n *ShowPrivileges) IsReadOnly() bool {
	return true
}

// Children implements the interface sql.Node.
func (n *ShowPrivileges) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *ShowPrivileges) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*ShowPrivileges) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// RowIter implements the interface sql.Node.
func (n *ShowPrivileges) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(
		sql.UntypedSqlRow{"Alter", "Tables", "To alter the table"},
		sql.UntypedSqlRow{"Alter routine", "Functions,Procedures", "To alter or drop stored functions/procedures"},
		sql.UntypedSqlRow{"Create", "Databases,Tables,Indexes", "To create new databases and tables"},
		sql.UntypedSqlRow{"Create routine", "Databases", "To use CREATE FUNCTION/PROCEDURE"},
		sql.UntypedSqlRow{"Create role", "Server Admin", "To create new roles"},
		sql.UntypedSqlRow{"Create temporary tables", "Databases", "To use CREATE TEMPORARY TABLE"},
		sql.UntypedSqlRow{"Create view", "Tables", "To create new views"},
		sql.UntypedSqlRow{"Create user", "Server Admin", "To create new users"},
		sql.UntypedSqlRow{"Delete", "Tables", "To delete existing rows"},
		sql.UntypedSqlRow{"Drop", "Databases,Tables", "To drop databases, tables, and views"},
		sql.UntypedSqlRow{"Drop role", "Server Admin", "To drop roles"},
		sql.UntypedSqlRow{"Event", "Server Admin", "To create, alter, drop and execute events"},
		sql.UntypedSqlRow{"Execute", "Functions,Procedures", "To execute stored routines"},
		sql.UntypedSqlRow{"File", "File access on server", "To read and write files on the server"},
		sql.UntypedSqlRow{"Grant option", "Databases,Tables,Functions,Procedures", "To give to other users those privileges you possess"},
		sql.UntypedSqlRow{"Index", "Tables", "To create or drop indexes"},
		sql.UntypedSqlRow{"Insert", "Tables", "To insert data into tables"},
		sql.UntypedSqlRow{"Lock tables", "Databases", "To use LOCK TABLES (together with SELECT privilege)"},
		sql.UntypedSqlRow{"Process", "Server Admin", "To view the plain text of currently executing queries"},
		sql.UntypedSqlRow{"Proxy", "Server Admin", "To make proxy user possible"},
		sql.UntypedSqlRow{"References", "Databases,Tables", "To have references on tables"},
		sql.UntypedSqlRow{"Reload", "Server Admin", "To reload or refresh tables, logs and privileges"},
		sql.UntypedSqlRow{"Replication client", "Server Admin", "To ask where the slave or master servers are"},
		sql.UntypedSqlRow{"Replication slave", "Server Admin", "To read binary log events from the master"},
		sql.UntypedSqlRow{"Select", "Tables", "To retrieve rows from table"},
		sql.UntypedSqlRow{"Show databases", "Server Admin", "To see all databases with SHOW DATABASES"},
		sql.UntypedSqlRow{"Show view", "Tables", "To see views with SHOW CREATE VIEW"},
		sql.UntypedSqlRow{"Shutdown", "Server Admin", "To shut down the server"},
		sql.UntypedSqlRow{"Super", "Server Admin", "To use KILL thread, SET GLOBAL, CHANGE MASTER, etc."},
		sql.UntypedSqlRow{"Trigger", "Tables", "To use triggers"},
		sql.UntypedSqlRow{"Create tablespace", "Server Admin", "To create/alter/drop tablespaces"},
		sql.UntypedSqlRow{"Update", "Tables", "To update existing rows"},
		sql.UntypedSqlRow{"Usage", "Server Admin", "No privileges - allow connect only"},
		sql.UntypedSqlRow{"ENCRYPTION_KEY_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"INNODB_REDO_LOG_ARCHIVE", "Server Admin", ""},
		sql.UntypedSqlRow{"REPLICATION_APPLIER", "Server Admin", ""},
		sql.UntypedSqlRow{"INNODB_REDO_LOG_ENABLE", "Server Admin", ""},
		sql.UntypedSqlRow{"SET_USER_ID", "Server Admin", ""},
		sql.UntypedSqlRow{"SERVICE_CONNECTION_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"GROUP_REPLICATION_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"AUDIT_ABORT_EXEMPT", "Server Admin", ""},
		sql.UntypedSqlRow{"GROUP_REPLICATION_STREAM", "Server Admin", ""},
		sql.UntypedSqlRow{"CLONE_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"SYSTEM_USER", "Server Admin", ""},
		sql.UntypedSqlRow{"AUTHENTICATION_POLICY_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"SHOW_ROUTINE", "Server Admin", ""},
		sql.UntypedSqlRow{"BACKUP_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"CONNECTION_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"PERSIST_RO_VARIABLES_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"RESOURCE_GROUP_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"SESSION_VARIABLES_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"SYSTEM_VARIABLES_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"APPLICATION_PASSWORD_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"FLUSH_OPTIMIZER_COSTS", "Server Admin", ""},
		sql.UntypedSqlRow{"AUDIT_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"BINLOG_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"BINLOG_ENCRYPTION_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"FLUSH_STATUS", "Server Admin", ""},
		sql.UntypedSqlRow{"FLUSH_TABLES", "Server Admin", ""},
		sql.UntypedSqlRow{"FLUSH_USER_RESOURCES", "Server Admin", ""},
		sql.UntypedSqlRow{"XA_RECOVER_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"PASSWORDLESS_USER_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"TABLE_ENCRYPTION_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"ROLE_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"REPLICATION_SLAVE_ADMIN", "Server Admin", ""},
		sql.UntypedSqlRow{"RESOURCE_GROUP_USER", "Server Admin", ""},
	), nil
}
