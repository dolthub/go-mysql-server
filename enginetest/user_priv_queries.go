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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

// UserPrivTests test the user, authentication, and privilege systems.
var UserPrivTests = []ScriptTest{
	{
		Name: "Basic user creation",
		SetUpScript: []string{
			"CREATE USER testuser@`127.0.0.1`;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE USER testuser@`127.0.0.1`;",
				ExpectedErr: sql.ErrUserCreationFailure,
			},
			{
				Query:    "CREATE USER IF NOT EXISTS testuser@`127.0.0.1`;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO mysql.user (Host, User, ssl_cipher, x509_issuer, x509_subject) VALUES ('localhost', 'testuser2', '', '', '');",
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
						0,                       // max_questions
						0,                       // max_updates
						0,                       // max_connections
						0,                       // max_user_connections
						"caching_sha2_password", // plugin
						nil,                     // authentication_string
						"N",                     // password_expired
						time.Unix(0, 0).UTC(),   // password_last_changed
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
}
