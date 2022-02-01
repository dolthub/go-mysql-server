// Copyright 2022 Dolthub, Inc.
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

package enginetest

import (
	"testing"
	"time"

	"gopkg.in/src-d/go-errors.v1"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
)

// UserPrivilegeTest is used to define a test on the user and privilege systems. These tests always have the root
// account available, and the root account is used with any queries in the SetUpScript.
type UserPrivilegeTest struct {
	Name        string
	SetUpScript []string
	Assertions  []UserPrivilegeTestAssertion
}

// UserPrivilegeTestAssertion is within a UserPrivilegeTest to assert functionality.
type UserPrivilegeTestAssertion struct {
	User        string
	Host        string
	Query       string
	Expected    []sql.Row
	ExpectedErr *errors.Kind
}

// ServerAuthenticationTest is used to define a test on the server authentication system. These tests always have the
// root account available, and the root account is used with any queries in the SetUpScript. The SetUpFunc is run before
// the SetUpScript.
type ServerAuthenticationTest struct {
	Name        string
	SetUpFunc   func(ctx *sql.Context, t *testing.T, engine *sqle.Engine)
	SetUpScript []string
	Assertions  []ServerAuthenticationTestAssertion
}

// ServerAuthenticationTestAssertion is within a ServerAuthenticationTest to assert functionality.
type ServerAuthenticationTestAssertion struct {
	Username    string
	Password    string
	Query       string
	ExpectedErr bool
}

// UserPrivTests test the user and privilege systems. These tests always have the root account available, and the root
// account is used with any queries in the SetUpScript.
var UserPrivTests = []UserPrivilegeTest{
	{
		Name: "Basic user creation",
		SetUpScript: []string{
			"CREATE USER testuser@`127.0.0.1`;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				Query:       "CREATE USER testuser@`127.0.0.1`;",
				ExpectedErr: sql.ErrUserCreationFailure,
			},
			{
				Query:    "CREATE USER IF NOT EXISTS testuser@`127.0.0.1`;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO mysql.user (Host, User) VALUES ('localhost', 'testuser2');",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query: "SELECT * FROM mysql.user WHERE User = 'root';",
				Expected: []sql.Row{
					{
						"localhost",             // Host
						"root",                  // User
						"Y",                     // Select_priv
						"Y",                     // Insert_priv
						"Y",                     // Update_priv
						"Y",                     // Delete_priv
						"Y",                     // Create_priv
						"Y",                     // Drop_priv
						"Y",                     // Reload_priv
						"Y",                     // Shutdown_priv
						"Y",                     // Process_priv
						"Y",                     // File_priv
						"Y",                     // Grant_priv
						"Y",                     // References_priv
						"Y",                     // Index_priv
						"Y",                     // Alter_priv
						"Y",                     // Show_db_priv
						"Y",                     // Super_priv
						"Y",                     // Create_tmp_table_priv
						"Y",                     // Lock_tables_priv
						"Y",                     // Execute_priv
						"Y",                     // Repl_slave_priv
						"Y",                     // Repl_client_priv
						"Y",                     // Create_view_priv
						"Y",                     // Show_view_priv
						"Y",                     // Create_routine_priv
						"Y",                     // Alter_routine_priv
						"Y",                     // Create_user_priv
						"Y",                     // Event_priv
						"Y",                     // Trigger_priv
						"Y",                     // Create_tablespace_priv
						"",                      // ssl_type
						"",                      // ssl_cipher
						"",                      // x509_issuer
						"",                      // x509_subject
						uint32(0),               // max_questions
						uint32(0),               // max_updates
						uint32(0),               // max_connections
						uint32(0),               // max_user_connections
						"mysql_native_password", // plugin
						"",                      // authentication_string
						"N",                     // password_expired
						time.Unix(1, 0).UTC(),   // password_last_changed
						nil,                     // password_lifetime
						"N",                     // account_locked
						"Y",                     // Create_role_priv
						"Y",                     // Drop_role_priv
						nil,                     // Password_reuse_history
						nil,                     // Password_reuse_time
						nil,                     // Password_require_current
						nil,                     // User_attributes
					},
				},
			},
			{
				Query: "SELECT Host, User FROM mysql.user;",
				Expected: []sql.Row{
					{"localhost", "root"},
					{"localhost", "testuser2"},
					{"127.0.0.1", "testuser"},
				},
			},
		},
	},
	{
		Name: "Basic SELECT and INSERT privilege checking",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"INSERT INTO test VALUES (1), (2), (3);",
			"CREATE USER tester@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "INSERT INTO test VALUES (4);",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT INSERT ON *.* TO tester@localhost;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "INSERT INTO test VALUES (4);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test;",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON *.* TO tester@localhost;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
		},
	},
}

// ServerAuthTests test the server authentication system. These tests always have the root account available, and the
// root account is used with any queries in the SetUpScript, along as being set to the context passed to SetUpFunc.
var ServerAuthTests = []ServerAuthenticationTest{
	{
		Name: "Basic root authentication",
		Assertions: []ServerAuthenticationTestAssertion{
			{
				Username:    "root",
				Password:    "",
				Query:       "SELECT * FROM mysql.user;",
				ExpectedErr: false,
			},
			{
				Username:    "root",
				Password:    "pass",
				Query:       "SELECT * FROM mysql.user;",
				ExpectedErr: true,
			},
		},
	},
	{
		Name: "Create User without plugin specification",
		SetUpScript: []string{
			"CREATE USER rand_user@localhost IDENTIFIED BY 'rand_pass';",
			"GRANT ALL ON *.* TO rand_user@localhost WITH GRANT OPTION;",
		},
		Assertions: []ServerAuthenticationTestAssertion{
			{
				Username:    "rand_user",
				Password:    "rand_pass",
				Query:       "SELECT * FROM mysql.user;",
				ExpectedErr: false,
			},
			{
				Username:    "rand_user",
				Password:    "rand_pass1",
				Query:       "SELECT * FROM mysql.user;",
				ExpectedErr: true,
			},
			{
				Username:    "rand_user",
				Password:    "",
				Query:       "SELECT * FROM mysql.user;",
				ExpectedErr: true,
			},
			{
				Username:    "rand_use",
				Password:    "rand_pass",
				Query:       "SELECT * FROM mysql.user;",
				ExpectedErr: true,
			},
		},
	},
	{
		Name: "Create User with plugin specification",
		SetUpScript: []string{
			"CREATE USER ranuse@localhost IDENTIFIED WITH mysql_native_password BY 'ranpas';",
			"GRANT ALL ON *.* TO ranuse@localhost WITH GRANT OPTION;",
		},
		Assertions: []ServerAuthenticationTestAssertion{
			{
				Username:    "ranuse",
				Password:    "ranpas",
				Query:       "SELECT * FROM mysql.user;",
				ExpectedErr: false,
			},
			{
				Username:    "ranuse",
				Password:    "what",
				Query:       "SELECT * FROM mysql.user;",
				ExpectedErr: true,
			},
			{
				Username:    "ranuse",
				Password:    "",
				Query:       "SELECT * FROM mysql.user;",
				ExpectedErr: true,
			},
		},
	},
	{
		Name: "Adding a Super User directly",
		SetUpFunc: func(ctx *sql.Context, t *testing.T, engine *sqle.Engine) {
			engine.Analyzer.Catalog.GrantTables.AddSuperUser("bestuser", "the_pass")
		},
		Assertions: []ServerAuthenticationTestAssertion{
			{
				Username:    "bestuser",
				Password:    "the_past",
				Query:       "SELECT * FROM mysql.user;",
				ExpectedErr: true,
			},
			{
				Username:    "bestuser",
				Password:    "the_pass",
				Query:       "SELECT * FROM mysql.user;",
				ExpectedErr: false,
			},
		},
	},
}
