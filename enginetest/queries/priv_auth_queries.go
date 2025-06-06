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

package queries

import (
	"testing"
	"time"

	"gopkg.in/src-d/go-errors.v1"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
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
	User           string
	Host           string
	Query          string
	Expected       []sql.Row
	ExpectedErr    *errors.Kind
	ExpectedErrStr string
}

// QuickPrivilegeTest specifically tests privileges on a predefined user (tester@localhost) using predefined tables and
// databases. Every test here can easily be represented by a UserPrivilegeTest, however this is intended to test
// specific privileges at a large scale, meaning there may be thousands of these tests, and hence the test data should
// be as small as possible.
//
// All queries will be run as a root user with full privileges (intended for setup), with the last query running as the
// testing user (tester@localhost). For example, the first query may grant a SELECT privilege, while the second query
// is the SELECT query. Of note, the current database as set by the context is retained when switching from the root
// user to the test user. This does not mean that the test user automatically gains access to the database, but this is
// used for any queries that (incorrectly) only work with the current database.
//
// ExpectingErr should be set when an error is expected, and it does not matter what the error is so long that it is one
// of the errors related to privilege checking (meaning a failed INSERT due to a missing column is NOT caught). If
// ExpectingErr is set and an error is given to ExpectedErr, then it is enforced that the error matches. However, if
// ExpectingErr is set and ExpectedErr is nil, then any privilege checking error will match.
//
// Expected makes a distinction between the nil value and the empty value. A nil value means that we do not care about
// the result, only that it did not error (unless one of the error-asserting fields are set). A non-nil value asserts
// that the returned value matches our Expected value. If the returned value is nil, then we make a special case to
// match the non-nil empty row with it, due to the aforementioned distinction.
//
// Statements that are run before every test (the state that all tests start with):
// CREATE USER tester@localhost;
// CREATE TABLE mydb.test (pk BIGINT PRIMARY KEY, v1 BIGINT);
// CREATE TABLE mydb.test2 (pk BIGINT PRIMARY KEY, v1 BIGINT);
// CREATE TABLE otherdb.test (pk BIGINT PRIMARY KEY, v1 BIGINT);
// CREATE TABLE otherdb.test2 (pk BIGINT PRIMARY KEY, v1 BIGINT);
// INSERT INTO mydb.test VALUES (0, 0), (1, 1);
// INSERT INTO mydb.test2 VALUES (0, 1), (1, 2);
// INSERT INTO otherdb.test VALUES (1, 1), (2, 2);
// INSERT INTO otherdb.test2 VALUES (1, 1), (2, 2);
type QuickPrivilegeTest struct {
	Queries      []string
	Expected     []sql.Row
	ExpectedErr  *errors.Kind
	ExpectingErr bool
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
	Username           string
	Password           string
	Query              string
	ExpectedErr        bool
	ExpectedErrKind    *errors.Kind
	ExpectedErrStr     string
	ExpectedAuthPlugin string
}

// UserPrivTests test the user and privilege systems. These tests always have the root account available, and the root
// account is used with any queries in the SetUpScript.
var UserPrivTests = []UserPrivilegeTest{
	{
		Name: "Create user limits",
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:        "root",
				Host:        "localhost",
				Query:       "create user abcdefghijklmnopqrstuvwxyz0123456789@'localhost' identified by 'abc123';",
				ExpectedErr: sql.ErrUserNameTooLong,
			},
			{
				User: "root",
				Host: "localhost",
				Query: "create user j@'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz" +
					"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz" +
					"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz'" +
					" identified by 'abc123';",
				ExpectedErr: sql.ErrUserHostTooLong,
			},
		},
	},
	{
		Name: "Binlog replication privileges",
		SetUpScript: []string{
			"CREATE USER user@localhost;",
			"CREATE USER 'replica-admin'@localhost;",
			"CREATE USER 'replica-client'@localhost;",
			"CREATE USER 'replica-reload'@localhost;",
			// REPLICATION_SLAVE_ADMIN allows: start replica, stop replica, change replication source, change replication filter
			"GRANT REPLICATION_SLAVE_ADMIN ON *.* TO 'replica-admin'@localhost;",
			// REPLICATION CLIENT allows: show replica status, show binary logs, show binary log status
			"GRANT REPLICATION CLIENT ON *.* to 'replica-client'@localhost;",
			// RELOAD allows: reset replica
			"GRANT RELOAD ON *.* TO 'replica-reload'@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			// START REPLICA
			{
				User:        "user",
				Host:        "localhost",
				Query:       "START REPLICA",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				// ErrNoReplicationController means the priv check passed
				User:        "replica-admin",
				Host:        "localhost",
				Query:       "START REPLICA",
				ExpectedErr: plan.ErrNoReplicationController,
			},
			{
				User:        "replica-client",
				Host:        "localhost",
				Query:       "START REPLICA",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:        "replica-reload",
				Host:        "localhost",
				Query:       "START REPLICA",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:        "root",
				Host:        "localhost",
				Query:       "START REPLICA",
				ExpectedErr: plan.ErrNoReplicationController,
			},

			// STOP REPLICA
			{
				User:        "user",
				Host:        "localhost",
				Query:       "STOP REPLICA",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				// ErrNoReplicationController means the priv check passed
				User:        "replica-admin",
				Host:        "localhost",
				Query:       "STOP REPLICA",
				ExpectedErr: plan.ErrNoReplicationController,
			},
			{
				User:        "replica-client",
				Host:        "localhost",
				Query:       "STOP REPLICA",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:        "replica-reload",
				Host:        "localhost",
				Query:       "STOP REPLICA",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:        "root",
				Host:        "localhost",
				Query:       "STOP REPLICA",
				ExpectedErr: plan.ErrNoReplicationController,
			},

			// RESET REPLICA
			{
				User:        "user",
				Host:        "localhost",
				Query:       "RESET REPLICA",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:        "replica-admin",
				Host:        "localhost",
				Query:       "RESET REPLICA",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:        "replica-client",
				Host:        "localhost",
				Query:       "RESET REPLICA",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				// ErrNoReplicationController means the priv check passed
				User:        "replica-reload",
				Host:        "localhost",
				Query:       "RESET REPLICA",
				ExpectedErr: plan.ErrNoReplicationController,
			},
			{
				User:        "root",
				Host:        "localhost",
				Query:       "RESET REPLICA",
				ExpectedErr: plan.ErrNoReplicationController,
			},

			// SHOW REPLICA STATUS
			{
				User:        "user",
				Host:        "localhost",
				Query:       "SHOW REPLICA STATUS;",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:        "replica-admin",
				Host:        "localhost",
				Query:       "SHOW REPLICA STATUS;",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "replica-client",
				Host:     "localhost",
				Query:    "SHOW REPLICA STATUS;",
				Expected: []sql.Row{},
			},
			{
				User:        "replica-reload",
				Host:        "localhost",
				Query:       "SHOW REPLICA STATUS;",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SHOW REPLICA STATUS;",
				Expected: []sql.Row{},
			},

			// SHOW BINARY LOGS
			{
				User:        "user",
				Host:        "localhost",
				Query:       "SHOW BINARY LOGS;",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:        "replica-admin",
				Host:        "localhost",
				Query:       "SHOW BINARY LOGS;",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "replica-client",
				Host:     "localhost",
				Query:    "SHOW BINARY LOGS;",
				Expected: []sql.Row{},
			},
			{
				User:        "replica-reload",
				Host:        "localhost",
				Query:       "SHOW BINARY LOGS;",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SHOW BINARY LOGS;",
				Expected: []sql.Row{},
			},

			// SHOW BINARY LOG STATUS
			{
				User:        "user",
				Host:        "localhost",
				Query:       "SHOW BINARY LOG STATUS;",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:        "replica-admin",
				Host:        "localhost",
				Query:       "SHOW BINARY LOG STATUS;",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "replica-client",
				Host:     "localhost",
				Query:    "SHOW BINARY LOG STATUS;",
				Expected: []sql.Row{},
			},
			{
				User:        "replica-reload",
				Host:        "localhost",
				Query:       "SHOW BINARY LOG STATUS;",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SHOW BINARY LOG STATUS;",
				Expected: []sql.Row{},
			},

			// CHANGE REPLICATION SOURCE
			{
				User:        "user",
				Host:        "localhost",
				Query:       "CHANGE REPLICATION SOURCE TO SOURCE_HOST='localhost';",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				// ErrNoReplicationController means the priv check passed
				User:        "replica-admin",
				Host:        "localhost",
				Query:       "CHANGE REPLICATION SOURCE TO SOURCE_HOST='localhost';",
				ExpectedErr: plan.ErrNoReplicationController,
			},
			{
				User:        "replica-client",
				Host:        "localhost",
				Query:       "CHANGE REPLICATION SOURCE TO SOURCE_HOST='localhost';",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:        "replica-reload",
				Host:        "localhost",
				Query:       "CHANGE REPLICATION SOURCE TO SOURCE_HOST='localhost';",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:        "root",
				Host:        "localhost",
				Query:       "CHANGE REPLICATION SOURCE TO SOURCE_HOST='localhost';",
				ExpectedErr: plan.ErrNoReplicationController,
			},

			// CHANGE REPLICATION FILTER
			{
				User:        "user",
				Host:        "localhost",
				Query:       "CHANGE REPLICATION FILTER REPLICATE_IGNORE_TABLE=(db01.t1);",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				// ErrNoReplicationController means the priv check passed
				User:        "replica-admin",
				Host:        "localhost",
				Query:       "CHANGE REPLICATION FILTER REPLICATE_IGNORE_TABLE=(db01.t1);",
				ExpectedErr: plan.ErrNoReplicationController,
			},
			{
				User:        "replica-client",
				Host:        "localhost",
				Query:       "CHANGE REPLICATION FILTER REPLICATE_IGNORE_TABLE=(db01.t1);",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:        "replica-reload",
				Host:        "localhost",
				Query:       "CHANGE REPLICATION FILTER REPLICATE_IGNORE_TABLE=(db01.t1);",
				ExpectedErr: sql.ErrPrivilegeCheckFailed,
			},
			{
				User:        "root",
				Host:        "localhost",
				Query:       "CHANGE REPLICATION FILTER REPLICATE_IGNORE_TABLE=(db01.t1);",
				ExpectedErr: plan.ErrNoReplicationController,
			},
		},
	},
	{
		Name: "Basic database and table name visibility",
		SetUpScript: []string{
			"CREATE TABLE mydb.test (pk BIGINT PRIMARY KEY);",
			"CREATE TABLE mydb.invis (pk BIGINT PRIMARY KEY);",
			"INSERT INTO mydb.test VALUES (1);",
			"CREATE USER tester@localhost;",
			"CREATE ROLE test_role;",
			"GRANT SELECT ON mydb.* TO test_role;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM mydb.test;/*1*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "WITH cte AS (SELECT * FROM mydb.test) SELECT * FROM cte;/*1*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM mydb.test2;/*1*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON *.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM mydb.test;/*2*/",
				Expected: []sql.Row{{1}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM mydb.test2;/*2*/",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE SELECT ON *.* FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{ // Ensure we've reverted to initial state (all SELECTs after REVOKEs are doing this)
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM mydb.test;/*3*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM mydb.test2;/*3*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM mydb.test;/*4*/",
				Expected: []sql.Row{{1}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM mydb.test2;/*4*/",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE SELECT ON mydb.* FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM mydb.test;/*5*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM mydb.test2;/*5*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.test TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM mydb.test;/*6*/",
				Expected: []sql.Row{{1}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "WITH cte AS (SELECT * FROM mydb.test) SELECT * FROM cte;/*6*/",
				Expected: []sql.Row{{1}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "WITH cte AS (SELECT * FROM mydb.test) SELECT * FROM cte JOIN mydb.invis t2 WHERE cte.pk = t2.pk;",
				ExpectedErr: sql.ErrTableAccessDeniedForUser,
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "WITH cte AS (SELECT * FROM mydb.test) SELECT * FROM cte JOIN mydb.test t2 WHERE cte.pk = t2.pk;",
				Expected: []sql.Row{{1, 1}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM mydb.test2;/*6*/",
				ExpectedErr: sql.ErrTableAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE SELECT ON mydb.test FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM mydb.test;/*7*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM mydb.test2;/*7*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.test2 TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM mydb.test;/*8*/",
				ExpectedErr: sql.ErrTableAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM mydb.test2;/*8*/",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE SELECT ON mydb.test2 FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM mydb.test;/*9*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM mydb.test2;/*9*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT test_role TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM mydb.test;/*10*/",
				Expected: []sql.Row{{1}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM mydb.test2;/*10*/",
				ExpectedErr: sql.ErrTableNotFound,
			},
		},
	},
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
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO mysql.user (Host, User) VALUES ('localhost', 'testuser2');",
				Expected: []sql.Row{{types.NewOkResult(1)}},
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
						[]byte(""),              // ssl_cipher
						[]byte(""),              // x509_issuer
						[]byte(""),              // x509_subject
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
						"",                      // identity
					},
				},
			},
			{
				Query: "SELECT Host, User, Plugin, length(authentication_string) > 0 FROM mysql.user order by User;",
				Expected: []sql.Row{
					{"localhost", "root", "mysql_native_password", false},
					{"127.0.0.1", "testuser", "mysql_native_password", false},
					// testuser2 was inserted directly into the table, so it uses the column default
					// from the plugin field – caching_sha2_password
					{"localhost", "testuser2", "caching_sha2_password", false},
				},
			},
		},
	},
	{
		Name: "User creation with auth plugin specified: mysql_native_password",
		SetUpScript: []string{
			"CREATE USER testuser1@`127.0.0.1` identified with mysql_native_password by 'pass1';",
			"CREATE USER testuser2@`127.0.0.1` identified with 'mysql_native_password';",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				Query:    "select user, host, plugin, authentication_string from mysql.user where user='testuser1';",
				Expected: []sql.Row{{"testuser1", "127.0.0.1", "mysql_native_password", "*22A99BA288DB55E8E230679259740873101CD636"}},
			},
			{
				Query:    "select user, host, plugin, authentication_string from mysql.user where user='testuser2';",
				Expected: []sql.Row{{"testuser2", "127.0.0.1", "mysql_native_password", ""}},
			},
		},
	},
	{
		Name: "User creation with auth plugin specified: caching_sha2_password",
		SetUpScript: []string{
			"CREATE USER testuser1@`127.0.0.1` identified with caching_sha2_password by 'pass1';",
			"CREATE USER testuser2@`127.0.0.1` identified with 'caching_sha2_password';",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				// caching_sha2_password auth uses a random salt to create the authentication
				// string. Since it's not a consistent value during each test run, we just sanity
				// check the first bytes of metadata (digest type, iterations) in the auth string.
				Query:    "select user, host, plugin, authentication_string like '$A$005$%' from mysql.user where user='testuser1';",
				Expected: []sql.Row{{"testuser1", "127.0.0.1", "caching_sha2_password", true}},
			},
			{
				Query:    "select user, host, plugin, authentication_string from mysql.user where user='testuser2';",
				Expected: []sql.Row{{"testuser2", "127.0.0.1", "caching_sha2_password", ""}},
			},
		},
	},
	{
		Name: "Migrate a user from mysql_native_password to caching_sha2_password",
		SetUpScript: []string{
			"CREATE USER testuser1@`127.0.0.1` identified with mysql_native_password by 'pass1';",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				Query:    "select user, host, plugin, authentication_string from mysql.user where user='testuser1';",
				Expected: []sql.Row{{"testuser1", "127.0.0.1", "mysql_native_password", "*22A99BA288DB55E8E230679259740873101CD636"}},
			},
			{
				Query:    "ALTER USER testuser1@`127.0.0.1` IDENTIFIED WITH caching_sha2_password BY 'pass1';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// caching_sha2_password auth uses a random salt to create the authentication
				// string. Since it's not a consistent value during each test run, we just sanity
				// check the first bytes of metadata (digest type, iterations) in the auth string.
				Query:    "select user, host, plugin, authentication_string like '$A$005$%' from mysql.user where user='testuser1';",
				Expected: []sql.Row{{"testuser1", "127.0.0.1", "caching_sha2_password", true}},
			},
			{
				Query:    "ALTER USER testuser1@`127.0.0.1` IDENTIFIED WITH caching_sha2_password;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select user, host, plugin, authentication_string from mysql.user where user='testuser1';",
				Expected: []sql.Row{{"testuser1", "127.0.0.1", "caching_sha2_password", ""}},
			},
		},
	},
	{
		Name: "Dynamic privilege support",
		SetUpScript: []string{
			"CREATE USER testuser@localhost;",
			"GRANT REPLICATION_SLAVE_ADMIN ON *.* TO testuser@localhost;",
			"GRANT CLONE_ADMIN ON *.* TO testuser@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				Query: "SELECT user, host from mysql.user",
				Expected: []sql.Row{
					{"root", "localhost"},
					{"testuser", "localhost"},
				},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "SHOW GRANTS FOR testuser@localhost;",
				Expected: []sql.Row{
					{"GRANT USAGE ON *.* TO `testuser`@`localhost`"},
					{"GRANT CLONE_ADMIN, REPLICATION_SLAVE_ADMIN ON *.* TO `testuser`@`localhost`"},
				},
			},
			{
				// Dynamic privileges may only be applied globally
				User:        "root",
				Host:        "localhost",
				Query:       "GRANT REPLICATION_SLAVE_ADMIN ON mydb.* TO 'testuser'@'localhost';",
				ExpectedErr: sql.ErrGrantRevokeIllegalPrivilegeWithMessage,
			},
			{
				// Dynamic privileges may only be applied globally
				User:        "root",
				Host:        "localhost",
				Query:       "GRANT REPLICATION_SLAVE_ADMIN ON mydb.mytable TO 'testuser'@'localhost';",
				ExpectedErr: sql.ErrGrantRevokeIllegalPrivilegeWithMessage,
			},
			{
				// Dynamic privileges may only be applied globally
				User:        "root",
				Host:        "localhost",
				Query:       "REVOKE REPLICATION_SLAVE_ADMIN ON mydb.* FROM 'testuser'@'localhost';",
				ExpectedErr: sql.ErrGrantRevokeIllegalPrivilegeWithMessage,
			},
			{
				// Dynamic privileges may only be applied globally
				User:        "root",
				Host:        "localhost",
				Query:       "REVOKE REPLICATION_SLAVE_ADMIN ON mydb.mytable FROM 'testuser'@'localhost';",
				ExpectedErr: sql.ErrGrantRevokeIllegalPrivilegeWithMessage,
			},
		},
	},
	{
		Name: "user creation no host",
		SetUpScript: []string{
			"CREATE USER testuser;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				Query: "SELECT user, host from mysql.user",
				Expected: []sql.Row{
					{"root", "localhost"},
					{"testuser", "%"},
				},
			},
		},
	},
	{
		Name: "grants at various scopes no host",
		SetUpScript: []string{
			"CREATE USER tester;",
			"GRANT SELECT ON *.* to tester",
			"GRANT SELECT ON db.* to tester",
			"GRANT SELECT ON db.tbl to tester",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:  "root",
				Host:  "localhost",
				Query: "SHOW GRANTS FOR tester@localhost;",
				Expected: []sql.Row{
					{"GRANT SELECT ON *.* TO `tester`@`%`"},
					{"GRANT SELECT ON `db`.* TO `tester`@`%`"},
					{"GRANT SELECT ON `db`.`tbl` TO `tester`@`%`"},
				},
			},
		},
	},
	{
		Name: "procedure grants and restrictions",
		SetUpScript: []string{
			"CREATE USER granted@localhost",
			"GRANT EXECUTE ON mydb.* TO granted@localhost",
			"GRANT EXECUTE ON PROCEDURE mydb.memory_admin_only TO granted@localhost", // Explicit grant on admin only proc
			"CREATE USER denied@localhost",
			"GRANT EXECUTE ON mydb.* TO denied@localhost", // Access to DB, but not to admin proc.
			"CREATE USER targeted@localhost",
			"GRANT EXECUTE ON PROCEDURE mydb.memory_admin_only TO targeted@localhost", // Explicit grant on admin only proc, even though no access to DB.
			"CREATE USER noaccess@localhost",                                          // Ensure this user can't run any procedure
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "granted",
				Host:     "localhost",
				Query:    "CALL mydb.memory_admin_only(1,2)",
				Expected: []sql.Row{{3}},
			},
			{
				User:     "denied",
				Host:     "localhost",
				Query:    "CALL mydb.memory_variadic_add(3,2)", // Verify this user _can_ access non-admin proc
				Expected: []sql.Row{{5}},
			},
			{
				User:           "denied",
				Host:           "localhost",
				Query:          "CALL mydb.memory_admin_only(1,2)",
				ExpectedErrStr: "command denied to user 'denied'@'localhost'",
			},
			{
				User:           "targeted",
				Host:           "localhost",
				Query:          "CALL mydb.memory_variadic_add(3,2)", // Verify this user _can't_ access non-admin proc
				ExpectedErrStr: "command denied to user 'targeted'@'localhost'",
			},
			{
				User:     "targeted",
				Host:     "localhost",
				Query:    "CALL mydb.memory_admin_only(7,2)",
				Expected: []sql.Row{{9}},
			},
			{
				User:           "noaccess",
				Host:           "localhost",
				Query:          "CALL mydb.memory_variadic_add(3,2)", // Verify this user can't access non-admin proc
				ExpectedErrStr: "Access denied for user 'noaccess'@'localhost' to database 'mydb'",
			},
			{
				User:           "noaccess",
				Host:           "localhost",
				Query:          "CALL mydb.memory_admin_only(1,2)",
				ExpectedErrStr: "Access denied for user 'noaccess'@'localhost' to database 'mydb'",
			},
		},
	},
	{
		Name: "Valid users without privileges may use the dual table",
		SetUpScript: []string{
			"CREATE USER tester@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT 1+2;",
				Expected: []sql.Row{{3}},
			},
			{
				User:           "noexist",
				Host:           "localhost",
				Query:          "SELECT 1+2;",
				ExpectedErrStr: "Access denied for user 'noexist' (errno 1045) (sqlstate 28000)",
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
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT INSERT ON *.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "INSERT INTO test VALUES (4);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
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
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
		},
	},
	{
		Name: "Database-level privileges exist",
		SetUpScript: []string{
			"CREATE USER tester@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT, UPDATE, EXECUTE ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT * FROM mysql.db;",
				Expected: []sql.Row{{"localhost", "mydb", "tester", "Y", "N", "Y", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "Y", "N", "N"}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE UPDATE ON mydb.* FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT * FROM mysql.db;",
				Expected: []sql.Row{{"localhost", "mydb", "tester", "Y", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "Y", "N", "N"}},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "UPDATE mysql.db SET Insert_priv = 'Y' WHERE User = 'tester';",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: 1,
					InsertID:     0,
					Info: plan.UpdateInfo{
						Matched:  1,
						Updated:  1,
						Warnings: 0,
					},
				}}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT * FROM mysql.db;",
				Expected: []sql.Row{{"localhost", "mydb", "tester", "Y", "Y", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "Y", "N", "N"}},
			},
		},
	},
	{
		Name: "Table-level privileges exist",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"CREATE USER tester@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT, DELETE, DROP ON mydb.test TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT * FROM mysql.tables_priv;",
				Expected: []sql.Row{{"localhost", "mydb", "tester", "test", "", time.Unix(1, 0).UTC(), "Select,Delete,Drop", ""}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE DELETE ON mydb.test FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT * FROM mysql.tables_priv;",
				Expected: []sql.Row{{"localhost", "mydb", "tester", "test", "", time.Unix(1, 0).UTC(), "Select,Drop", ""}},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "UPDATE mysql.tables_priv SET table_priv = 'References,Index' WHERE User = 'tester';",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: 1,
					InsertID:     0,
					Info: plan.UpdateInfo{
						Matched:  1,
						Updated:  1,
						Warnings: 0,
					},
				}}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT * FROM mysql.tables_priv;",
				Expected: []sql.Row{{"localhost", "mydb", "tester", "test", "", time.Unix(1, 0).UTC(), "References,Index", ""}},
			},
		},
	},
	{
		Name: "GRANT Procedure and function privileges reflect in mysql.procs_priv",
		SetUpScript: []string{
			"CREATE USER tester1@localhost;",
			"CREATE USER tester2@localhost;",
			"GRANT EXECUTE ON PROCEDURE mydb.proc1 TO tester1@localhost;",
			"GRANT GRANT OPTION ON PROCEDURE mydb.proc1 TO tester1@localhost;",
			"GRANT ALTER ROUTINE ON PROCEDURE mydb.proc2 TO tester1@localhost;",
			"GRANT GRANT OPTION ON PROCEDURE mydb.proc1 TO tester2@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:  "root",
				Host:  "localhost",
				Query: "SELECT Routine_name,Routine_type,proc_priv from mysql.procs_priv WHERE User = 'tester1'",
				Expected: []sql.Row{
					{"proc1", "PROCEDURE", "Grant,Execute"},
					{"proc2", "PROCEDURE", "Alter Routine"},
				},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT Routine_name,Routine_type,proc_priv from mysql.procs_priv WHERE User = 'tester2'",
				Expected: []sql.Row{{"proc1", "PROCEDURE", "Grant"}},
			},
			{
				User:     "tester1",
				Host:     "localhost",
				Query:    "GRANT Execute ON PROCEDURE mydb.proc1 TO tester2@localhost",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:           "tester2",
				Host:           "localhost",
				Query:          "GRANT Execute ON PROCEDURE mydb.proc2 TO tester1@localhost",
				ExpectedErrStr: "command denied to user 'tester2'@'localhost'",
			},
		},
	},
	{
		Name: "GRANT Procedure and function privileges reflect in mysql.procs_priv",
		SetUpScript: []string{
			"CREATE USER tester1@localhost;",
			"CREATE USER tester2@localhost;",
			"GRANT EXECUTE ON PROCEDURE mydb.proc1 TO tester1@localhost;",
			"GRANT GRANT OPTION ON PROCEDURE mydb.proc1 TO tester1@localhost;",
			"GRANT ALTER ROUTINE ON PROCEDURE mydb.proc2 TO tester1@localhost;",
			"GRANT GRANT OPTION ON PROCEDURE mydb.proc2 TO tester2@localhost;",
			"GRANT EXECUTE ON PROCEDURE mydb.proc2 TO tester2@localhost;",
			"REVOKE EXECUTE ON PROCEDURE mydb.proc1 FROM tester1@localhost;",
			"REVOKE ALTER ROUTINE ON PROCEDURE mydb.proc2 FROM tester1@localhost;",
			"REVOKE ALTER ROUTINE ON PROCEDURE mydb.proc2 FROM tester2@localhost;", // Should be no-op.
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT Routine_name,Routine_type,proc_priv from mysql.procs_priv WHERE User = 'tester1'",
				Expected: []sql.Row{{"proc1", "PROCEDURE", "Grant"}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT Routine_name,Routine_type,proc_priv from mysql.procs_priv WHERE User = 'tester2'",
				Expected: []sql.Row{sql.Row{"proc2", "PROCEDURE", "Grant,Execute"}},
			},
		},
	},
	{
		Name: "GRANT function privileges errors",
		SetUpScript: []string{
			"CREATE USER tester1@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:           "root",
				Host:           "localhost",
				Query:          "GRANT GRANT OPTION ON FUNCTION mydb.func1 TO tester1@localhost;",
				ExpectedErrStr: "fine grain function permissions currently unsupported",
			},
			{
				User:           "root",
				Host:           "localhost",
				Query:          "GRANT EXECUTE ON FUNCTION mydb.func1 TO tester1@localhost;",
				ExpectedErrStr: "fine grain function permissions currently unsupported",
			},
			{
				User:           "root",
				Host:           "localhost",
				Query:          "GRANT ALTER ROUTINE ON FUNCTION mydb.func1 TO tester1@localhost;",
				ExpectedErrStr: "fine grain function permissions currently unsupported",
			},
		},
	},
	{
		Name: "Basic revoke SELECT privilege",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"INSERT INTO test VALUES (1), (2), (3);",
			"CREATE USER tester@localhost;",
			"GRANT SELECT ON *.* TO tester@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT User, Host, Select_priv FROM mysql.user WHERE User = 'tester';",
				Expected: []sql.Row{{"tester", "localhost", "Y"}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE SELECT ON *.* FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test;",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT User, Host, Select_priv FROM mysql.user WHERE User = 'tester';",
				Expected: []sql.Row{{"tester", "localhost", "N"}},
			},

			{
				// Re-revoking does nothing
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE SELECT ON *.* FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// IF EXISTS option does nothing
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE IF EXISTS SELECT ON *.* FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT User, Host, Select_priv FROM mysql.user WHERE User = 'tester';",
				Expected: []sql.Row{{"tester", "localhost", "N"}},
			},
		},
	},
	{
		Name: "Basic revoke all global static privileges",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"INSERT INTO test VALUES (1), (2), (3);",
			"CREATE USER tester@localhost;",
			"CREATE USER tester1@localhost;",
			"CREATE USER tester2@localhost;",
			"GRANT ALL ON *.* TO tester@localhost;",
			"GRANT ALL ON *.* TO tester1@localhost WITH GRANT OPTION;",
			"GRANT ALL ON *.* TO tester2@localhost WITH GRANT OPTION;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "INSERT INTO test VALUES (4);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT User, Host, Select_priv, Insert_priv FROM mysql.user WHERE User = 'tester';",
				Expected: []sql.Row{{"tester", "localhost", "Y", "Y"}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE ALL ON *.* FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test;",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "INSERT INTO test VALUES (5);",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT User, Host, Select_priv, Insert_priv, Grant_priv FROM mysql.user WHERE User = 'tester';",
				Expected: []sql.Row{{"tester", "localhost", "N", "N", "N"}},
			},

			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT User, Host, Select_priv, Insert_priv, Grant_priv FROM mysql.user WHERE User = 'tester1';",
				Expected: []sql.Row{{"tester1", "localhost", "Y", "Y", "Y"}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE ALL, GRANT OPTION FROM tester1@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT User, Host, Select_priv, Insert_priv, Grant_priv FROM mysql.user WHERE User = 'tester1';",
				Expected: []sql.Row{{"tester1", "localhost", "N", "N", "N"}},
			},

			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT User, Host, Select_priv, Insert_priv, Grant_priv FROM mysql.user WHERE User = 'tester2';",
				Expected: []sql.Row{{"tester2", "localhost", "Y", "Y", "Y"}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE ALL PRIVILEGES, GRANT OPTION FROM tester2@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT User, Host, Select_priv, Insert_priv, Grant_priv FROM mysql.user WHERE User = 'tester2';",
				Expected: []sql.Row{{"tester2", "localhost", "N", "N", "N"}},
			},
			{
				// Re-revoking does nothing
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE ALL PRIVILEGES, GRANT OPTION FROM tester2@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// IF EXISTS does nothing
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE IF EXISTS ALL PRIVILEGES, GRANT OPTION FROM tester2@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT User, Host, Select_priv, Insert_priv, Grant_priv FROM mysql.user WHERE User = 'tester2';",
				Expected: []sql.Row{{"tester2", "localhost", "N", "N", "N"}},
			},
			{
				User:        "root",
				Host:        "localhost",
				Query:       "REVOKE IF EXISTS ALL PRIVILEGES, GRANT OPTION FROM fake1@localhost, fake2@localhost, fake3@localhost;",
				ExpectedErr: sql.ErrRevokeUserDoesNotExist,
			},
			{
				// TODO: check warnings
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE IF EXISTS ALL PRIVILEGES, GRANT OPTION FROM fake1@localhost, fake2@localhost, fake3@localhost IGNORE UNKNOWN USER;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Basic role creation",
		SetUpScript: []string{
			"CREATE ROLE test_role;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT User, Host, account_locked FROM mysql.user WHERE User = 'test_role';",
				Expected: []sql.Row{{"test_role", "%", "Y"}},
			},
		},
	},
	{
		Name: "Grant Role with SELECT Privilege",
		SetUpScript: []string{
			"SET @@GLOBAL.activate_all_roles_on_login = true;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"INSERT INTO test VALUES (1), (2), (3);",
			"CREATE USER tester@localhost;",
			"CREATE ROLE test_role;",
			"GRANT SELECT ON *.* TO test_role;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test;",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM mysql.role_edges;",
				Expected: []sql.Row{{0}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT test_role TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT * FROM mysql.role_edges;",
				Expected: []sql.Row{{"%", "test_role", "localhost", "tester", "N"}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT User, Host, Select_priv FROM mysql.user WHERE User = 'tester';",
				Expected: []sql.Row{{"tester", "localhost", "N"}},
			},
		},
	},
	{
		Name: "Revoke role currently granted to a user",
		SetUpScript: []string{
			"SET @@GLOBAL.activate_all_roles_on_login = true;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"INSERT INTO test VALUES (1), (2), (3);",
			"CREATE USER tester@localhost;",
			"CREATE ROLE test_role;",
			"GRANT SELECT ON *.* TO test_role;",
			"GRANT test_role TO tester@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT * FROM mysql.role_edges;",
				Expected: []sql.Row{{"%", "test_role", "localhost", "tester", "N"}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE test_role FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test;",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM mysql.role_edges;",
				Expected: []sql.Row{{0}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM mysql.user WHERE User = 'test_role';",
				Expected: []sql.Row{{1}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM mysql.user WHERE User = 'tester';",
				Expected: []sql.Row{{1}},
			},
			{
				User:        "root",
				Host:        "localhost",
				Query:       "REVOKE fake_role FROM tester@localhost;",
				ExpectedErr: sql.ErrGrantRevokeRoleDoesNotExist,
			},
			{
				// TODO: check for warning
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE IF EXISTS fake_role FROM tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "root",
				Host:        "localhost",
				Query:       "REVOKE test_role FROM fake_user@localhost;",
				ExpectedErr: sql.ErrGrantRevokeRoleDoesNotExist,
			},
			{
				// TODO: check for warning
				User:     "root",
				Host:     "localhost",
				Query:    "REVOKE test_role FROM fake_user@localhost IGNORE UNKNOWN USER;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Drop role currently granted to a user",
		SetUpScript: []string{
			"SET @@GLOBAL.activate_all_roles_on_login = true;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"INSERT INTO test VALUES (1), (2), (3);",
			"CREATE USER tester@localhost;",
			"CREATE ROLE test_role;",
			"GRANT SELECT ON *.* TO test_role;",
			"GRANT test_role TO tester@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT * FROM mysql.role_edges;",
				Expected: []sql.Row{{"%", "test_role", "localhost", "tester", "N"}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "DROP ROLE test_role;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:        "tester",
				Host:        "localhost",
				Query:       "SELECT * FROM test;",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM mysql.role_edges;",
				Expected: []sql.Row{{0}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM mysql.user WHERE User = 'test_role';",
				Expected: []sql.Row{{0}},
			},
			{ // Ensure nothing wonky happened like the user was deleted as well
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM mysql.user WHERE User = 'tester';",
				Expected: []sql.Row{{1}},
			},
			{
				User:        "root",
				Host:        "localhost",
				Query:       "DROP ROLE test_role;",
				ExpectedErr: sql.ErrRoleDeletionFailure,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "DROP ROLE IF EXISTS test_role;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Drop user with role currently granted",
		SetUpScript: []string{
			"SET @@GLOBAL.activate_all_roles_on_login = true;",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"INSERT INTO test VALUES (1), (2), (3);",
			"CREATE USER tester@localhost;",
			"CREATE ROLE test_role;",
			"GRANT SELECT ON *.* TO test_role;",
			"GRANT test_role TO tester@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT * FROM mysql.role_edges;",
				Expected: []sql.Row{{"%", "test_role", "localhost", "tester", "N"}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "DROP USER tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM mysql.role_edges;",
				Expected: []sql.Row{{0}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM mysql.user WHERE User = 'tester';",
				Expected: []sql.Row{{0}},
			},
			{ // Ensure nothing wonky happened like the role was deleted as well
				User:     "root",
				Host:     "localhost",
				Query:    "SELECT COUNT(*) FROM mysql.user WHERE User = 'test_role';",
				Expected: []sql.Row{{1}},
			},
			{
				User:        "root",
				Host:        "localhost",
				Query:       "DROP USER tester@localhost;",
				ExpectedErr: sql.ErrUserDeletionFailure,
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "DROP USER IF EXISTS tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Show grants on root account",
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:  "root",
				Host:  "localhost",
				Query: "SHOW GRANTS;",
				Expected: []sql.Row{{"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, SHUTDOWN, PROCESS, " +
					"FILE, REFERENCES, INDEX, ALTER, SHOW DATABASES, SUPER, CREATE TEMPORARY TABLES, LOCK TABLES, " +
					"EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, " +
					"ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, CREATE TABLESPACE, CREATE ROLE, DROP ROLE ON *.* TO " +
					"`root`@`localhost` WITH GRANT OPTION"}},
			},
		},
	},
	{
		Name: "Show grants on a user from the root account",
		SetUpScript: []string{
			"CREATE USER tester@localhost;",
			"GRANT SELECT ON *.* TO tester@localhost;",
			"CREATE ROLE test_role1;",
			"CREATE ROLE test_role2;",
			"GRANT INSERT ON *.* TO test_role1;",
			"GRANT REFERENCES ON *.* TO test_role2;",
			"GRANT test_role1 TO tester@localhost;",
			"GRANT test_role2 TO tester@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:  "root",
				Host:  "localhost",
				Query: "SHOW GRANTS FOR tester@localhost;",
				Expected: []sql.Row{
					{"GRANT SELECT ON *.* TO `tester`@`localhost`"},
					{"GRANT `test_role1`@`%`, `test_role2`@`%` TO `tester`@`localhost`"},
				},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT UPDATE ON *.* TO tester@localhost WITH GRANT OPTION;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "SHOW GRANTS FOR tester@localhost;",
				Expected: []sql.Row{
					{"GRANT SELECT, UPDATE ON *.* TO `tester`@`localhost` WITH GRANT OPTION"},
					{"GRANT `test_role1`@`%`, `test_role2`@`%` TO `tester`@`localhost`"},
				},
			},
			{
				User:  "tester",
				Host:  "localhost",
				Query: "SHOW GRANTS;",
				Expected: []sql.Row{
					{"GRANT SELECT, UPDATE ON *.* TO `tester`@`localhost` WITH GRANT OPTION"},
					{"GRANT `test_role1`@`%`, `test_role2`@`%` TO `tester`@`localhost`"},
				},
			},
		},
	},
	{
		Name: "show user with no grants",
		SetUpScript: []string{
			"CREATE USER tester@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:  "root",
				Host:  "localhost",
				Query: "SHOW GRANTS FOR tester@localhost;",
				Expected: []sql.Row{
					{"GRANT USAGE ON *.* TO `tester`@`localhost`"},
				},
			},
		},
	},
	{
		Name: "show grants with multiple global grants",
		SetUpScript: []string{
			"CREATE USER tester@localhost;",
			"GRANT SELECT ON *.* to tester@localhost",
			"GRANT INSERT ON *.* to tester@localhost",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:  "root",
				Host:  "localhost",
				Query: "SHOW GRANTS FOR tester@localhost;",
				Expected: []sql.Row{
					{"GRANT SELECT, INSERT ON *.* TO `tester`@`localhost`"},
				},
			},
		},
	},
	{
		Name: "show grants at various scopes",
		SetUpScript: []string{
			"CREATE USER tester@localhost;",
			"GRANT SELECT ON *.* to tester@localhost",
			"GRANT SELECT ON db.* to tester@localhost",
			"GRANT SELECT ON db.tbl to tester@localhost",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:  "root",
				Host:  "localhost",
				Query: "SHOW GRANTS FOR tester@localhost;",
				Expected: []sql.Row{
					{"GRANT SELECT ON *.* TO `tester`@`localhost`"},
					{"GRANT SELECT ON `db`.* TO `tester`@`localhost`"},
					{"GRANT SELECT ON `db`.`tbl` TO `tester`@`localhost`"},
				},
			},
		},
	},
	{
		Name: "show grants at only some scopes",
		SetUpScript: []string{
			"CREATE USER tester@localhost;",
			"GRANT SELECT ON *.* to tester@localhost",
			"GRANT SELECT ON db.tbl to tester@localhost",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:  "root",
				Host:  "localhost",
				Query: "SHOW GRANTS FOR tester@localhost;",
				Expected: []sql.Row{
					{"GRANT SELECT ON *.* TO `tester`@`localhost`"},
					{"GRANT SELECT ON `db`.`tbl` TO `tester`@`localhost`"},
				},
			},
		},
	},
	{
		Name: "show always shows global USAGE priv regardless of other privs",
		SetUpScript: []string{
			"CREATE USER tester@localhost;",
			"GRANT SELECT ON db.* to tester@localhost",
			"GRANT INSERT ON db1.* to tester@localhost",
			"GRANT DELETE ON db2.* to tester@localhost",
			"GRANT SELECT ON db.tbl to tester@localhost",
			"GRANT INSERT ON db.tbl to tester@localhost",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:  "root",
				Host:  "localhost",
				Query: "SHOW GRANTS FOR tester@localhost;",
				Expected: []sql.Row{
					{"GRANT USAGE ON *.* TO `tester`@`localhost`"},
					{"GRANT SELECT ON `db`.* TO `tester`@`localhost`"},
					{"GRANT INSERT ON `db1`.* TO `tester`@`localhost`"},
					{"GRANT DELETE ON `db2`.* TO `tester`@`localhost`"},
					{"GRANT SELECT, INSERT ON `db`.`tbl` TO `tester`@`localhost`"},
				},
			},
		},
	},
	{
		Name: "with grant option works at every scope",
		SetUpScript: []string{
			"CREATE USER tester@localhost;",
			"GRANT SELECT ON *.* to tester@localhost WITH GRANT OPTION",
			"GRANT SELECT ON db.* to tester@localhost WITH GRANT OPTION",
			"GRANT SELECT ON db.tbl to tester@localhost WITH GRANT OPTION",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:  "root",
				Host:  "localhost",
				Query: "SHOW GRANTS FOR tester@localhost;",
				Expected: []sql.Row{
					{"GRANT SELECT ON *.* TO `tester`@`localhost` WITH GRANT OPTION"},
					{"GRANT SELECT ON `db`.* TO `tester`@`localhost` WITH GRANT OPTION"},
					{"GRANT SELECT ON `db`.`tbl` TO `tester`@`localhost` WITH GRANT OPTION"},
				},
			},
		},
	},
	{
		Name: "adding with grant option applies to existing privileges",
		SetUpScript: []string{
			"CREATE USER tester@localhost;",
			"GRANT SELECT ON *.* to tester@localhost",
			"GRANT INSERT ON *.* to tester@localhost WITH GRANT OPTION",
			"GRANT SELECT ON db.* to tester@localhost",
			"GRANT INSERT ON db.* to tester@localhost WITH GRANT OPTION",
			"GRANT SELECT ON db.tbl to tester@localhost",
			"GRANT INSERT ON db.tbl to tester@localhost WITH GRANT OPTION",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:  "root",
				Host:  "localhost",
				Query: "SHOW GRANTS FOR tester@localhost;",
				Expected: []sql.Row{
					{"GRANT SELECT, INSERT ON *.* TO `tester`@`localhost` WITH GRANT OPTION"},
					{"GRANT SELECT, INSERT ON `db`.* TO `tester`@`localhost` WITH GRANT OPTION"},
					{"GRANT SELECT, INSERT ON `db`.`tbl` TO `tester`@`localhost` WITH GRANT OPTION"},
				},
			},
		},
	},
	{
		Name: "SHOW DATABASES shows `mysql` database",
		SetUpScript: []string{
			"CREATE USER testuser;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:  "root",
				Host:  "localhost",
				Query: "SELECT user FROM mysql.user;",
				Expected: []sql.Row{
					{"root"},
					{"testuser"},
				},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "SELECT USER();",
				Expected: []sql.Row{
					{"root@localhost"},
				},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "SHOW DATABASES",
				Expected: []sql.Row{
					{"information_schema"},
					{"mydb"},
					{"mysql"},
				},
			},
		},
	},
	{
		Name: "Anonymous User",
		SetUpScript: []string{
			"CREATE TABLE mydb.test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"CREATE TABLE mydb.test2 (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO mydb.test VALUES (0, 0), (1, 1);",
			"INSERT INTO mydb.test2 VALUES (0, 1), (1, 2);",
			"CREATE USER 'rand_user'@'localhost';",
			"CREATE USER ''@'%';",
			"GRANT SELECT ON mydb.test TO 'rand_user'@'localhost';",
			"GRANT SELECT ON mydb.test2 TO ''@'%';",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:  "rand_user",
				Host:  "localhost",
				Query: "SELECT * FROM mydb.test;",
				Expected: []sql.Row{
					{0, 0},
					{1, 1},
				},
			},
			{
				User:        "rand_user",
				Host:        "localhost",
				Query:       "SELECT * FROM mydb.test2;",
				ExpectedErr: sql.ErrTableAccessDeniedForUser,
			},
			{
				User:        "rand_user",
				Host:        "non_existent_host",
				Query:       "SELECT * FROM mydb.test;",
				ExpectedErr: sql.ErrTableAccessDeniedForUser,
			},
			{
				User:  "rand_user",
				Host:  "non_existent_host",
				Query: "SELECT * FROM mydb.test2;",
				Expected: []sql.Row{
					{0, 1},
					{1, 2},
				},
			},
			{
				User:        "non_existent_user",
				Host:        "non_existent_host",
				Query:       "SELECT * FROM mydb.test;",
				ExpectedErr: sql.ErrTableAccessDeniedForUser,
			},
			{
				User:  "non_existent_user",
				Host:  "non_existent_host",
				Query: "SELECT * FROM mydb.test2;",
				Expected: []sql.Row{
					{0, 1},
					{1, 2},
				},
			},
			{
				User:        "",
				Host:        "%",
				Query:       "SELECT * FROM mydb.test;",
				ExpectedErr: sql.ErrTableAccessDeniedForUser,
			},
			{
				User:  "",
				Host:  "%",
				Query: "SELECT * FROM mydb.test2;",
				Expected: []sql.Row{
					{0, 1},
					{1, 2},
				},
			},
		},
	},
	{
		Name: "IPv4 Loopback == localhost",
		SetUpScript: []string{
			"CREATE TABLE mydb.test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"CREATE TABLE mydb.test2 (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"INSERT INTO mydb.test VALUES (0, 0), (1, 1);",
			"INSERT INTO mydb.test2 VALUES (0, 1), (1, 2);",
			"CREATE USER 'rand_user1'@'localhost';",
			"CREATE USER 'rand_user2'@'127.0.0.1';",
			"GRANT SELECT ON mydb.test TO 'rand_user1'@'localhost';",
			"GRANT SELECT ON mydb.test2 TO 'rand_user2'@'127.0.0.1';",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:  "rand_user1",
				Host:  "localhost",
				Query: "SELECT * FROM mydb.test;",
				Expected: []sql.Row{
					{0, 0},
					{1, 1},
				},
			},
			{
				User:  "rand_user1",
				Host:  "127.0.0.1",
				Query: "SELECT * FROM mydb.test;",
				Expected: []sql.Row{
					{0, 0},
					{1, 1},
				},
			},
			{
				User:           "rand_user1",
				Host:           "54.244.85.252",
				Query:          "SELECT * FROM mydb.test;",
				ExpectedErrStr: "Access denied for user 'rand_user1' (errno 1045) (sqlstate 28000)",
			},
			{
				User:  "rand_user2",
				Host:  "localhost",
				Query: "SELECT * FROM mydb.test2;",
				Expected: []sql.Row{
					{0, 1},
					{1, 2},
				},
			},
			{
				User:  "rand_user2",
				Host:  "127.0.0.1",
				Query: "SELECT * FROM mydb.test2;",
				Expected: []sql.Row{
					{0, 1},
					{1, 2},
				},
			},
			{
				User:           "rand_user2",
				Host:           "54.244.85.252",
				Query:          "SELECT * FROM mydb.test2;",
				ExpectedErrStr: "Access denied for user 'rand_user2' (errno 1045) (sqlstate 28000)",
			},
		},
	},
	{
		Name: "DROP USER without a host designation",
		SetUpScript: []string{
			"CREATE USER admin;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:  "root",
				Host:  "localhost",
				Query: "SELECT user FROM mysql.user",
				Expected: []sql.Row{
					{"root"},
					{"admin"},
				},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "DROP USER admin;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:  "root",
				Host:  "localhost",
				Query: "SELECT user FROM mysql.user",
				Expected: []sql.Row{
					{"root"},
				},
			},
		},
	},
	{
		Name: "information_schema.columns table 'privileges' column gets correct values",
		SetUpScript: []string{
			"CREATE TABLE checks (a INTEGER PRIMARY KEY, b INTEGER, c VARCHAR(20))",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, c VARCHAR(20), p POINT default (POINT(1,1)))",
			"CREATE USER tester@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT count(*) FROM inFORmation_ScHeMa.columns where table_schema = 'mydb' and table_name = 'test';",
				Expected: []sql.Row{{0}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT INSERT ON mydb.test TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT column_name, privileges FROM information_schema.columns where table_schema = 'mydb' and table_name = 'test'",
				Expected: []sql.Row{{"pk", "insert"}, {"c", "insert"}, {"p", "insert"}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT column_name, privileges FROM information_schema.columns where table_schema = 'mydb' and table_name = 'test'",
				Expected: []sql.Row{{"pk", "insert,select"}, {"c", "insert,select"}, {"p", "insert,select"}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT UPDATE ON mydb.checks TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "select table_name, column_name, privileges from information_schema.columns where table_schema = 'mydb' and table_name = 'checks';",
				Expected: []sql.Row{{"checks", "a", "select,update"}, {"checks", "b", "select,update"}, {"checks", "c", "select,update"}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT count(*) FROM information_schema.columns where table_schema = 'information_schema' and table_name = 'columns'",
				Expected: []sql.Row{{22}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "select table_name, column_name, privileges from information_schema.columns where table_schema = 'mydb' and table_name = 'checks';",
				Expected: []sql.Row{{"checks", "a", "insert,references,select,update"}, {"checks", "b", "insert,references,select,update"}, {"checks", "c", "insert,references,select,update"}},
			},
		},
	},
	{
		Name: "information_schema.column_statistics shows columns with privileges only",
		SetUpScript: []string{
			"CREATE TABLE two (i bigint primary key, j bigint, key(j))",
			"INSERT INTO two VALUES (1, 4), (2, 5), (3, 6)",
			"CREATE TABLE one (f double primary key)",
			"INSERT INTO one VALUES (1.25), (45.25), (7.5), (10.5)",
			"ANALYZE TABLE one",
			"ANALYZE TABLE two",
			"CREATE USER tester@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.one TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:  "tester",
				Host:  "localhost",
				Query: "SELECT table_name, column_name FROM information_schema.column_statistics where schema_name = 'mydb';",
				Expected: []sql.Row{
					{"one", "f"},
				},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mydb.two TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:  "tester",
				Host:  "localhost",
				Query: "SELECT table_name, column_name FROM information_schema.column_statistics where schema_name = 'mydb';",
				Expected: []sql.Row{
					{"one", "f"},
					{"two", "i"},
					{"two", "j"},
				},
			},
		},
	},
	{
		Name: "information_schema.statistics shows tables with privileges only",
		SetUpScript: []string{
			"CREATE TABLE checks (a INTEGER PRIMARY KEY, b INTEGER, c VARCHAR(20))",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, c VARCHAR(20), p POINT default (POINT(1,1)))",
			"CREATE USER tester@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT count(*) FROM information_schema.statistics where table_schema = 'mydb';",
				Expected: []sql.Row{{0}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT INSERT ON mydb.checks TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "select table_name, column_name, index_name from information_schema.statistics where table_schema = 'mydb';",
				Expected: []sql.Row{{"checks", "a", "PRIMARY"}},
			},
		},
	},
	{
		Name: "basic tests on information_schema.SCHEMA_PRIVILEGES table",
		SetUpScript: []string{
			"CREATE TABLE checks (a INTEGER PRIMARY KEY, b INTEGER, c VARCHAR(20))",
			"CREATE USER tester@localhost;",
			"CREATE USER admin@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "root",
				Host:     "localhost",
				Query:    "select * from information_schema.schema_privileges;",
				Expected: []sql.Row{},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT INSERT, REFERENCES ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT UPDATE, GRANT OPTION ON mydb.* TO admin@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "select * from information_schema.schema_privileges order by privilege_type, is_grantable;",
				Expected: []sql.Row{{"'tester'@'localhost'", "def", "mydb", "INSERT", "NO"}, {"'tester'@'localhost'", "def", "mydb", "REFERENCES", "NO"}, {"'admin'@'localhost'", "def", "mydb", "UPDATE", "YES"}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "select * from information_schema.schema_privileges order by privilege_type, is_grantable;",
				Expected: []sql.Row{{"'tester'@'localhost'", "def", "mydb", "INSERT", "NO"}, {"'tester'@'localhost'", "def", "mydb", "REFERENCES", "NO"}},
			},
			{
				User:     "admin",
				Host:     "localhost",
				Query:    "select * from information_schema.schema_privileges order by privilege_type, is_grantable;",
				Expected: []sql.Row{{"'admin'@'localhost'", "def", "mydb", "UPDATE", "YES"}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mysql.* TO admin@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "admin",
				Host:     "localhost",
				Query:    "select * from information_schema.schema_privileges order by privilege_type, is_grantable;",
				Expected: []sql.Row{{"'tester'@'localhost'", "def", "mydb", "INSERT", "NO"}, {"'tester'@'localhost'", "def", "mydb", "REFERENCES", "NO"}, {"'admin'@'localhost'", "def", "mysql", "SELECT", "NO"}, {"'admin'@'localhost'", "def", "mydb", "UPDATE", "YES"}},
			},
		},
	},
	{
		Name: "basic tests on information_schema.TABLE_PRIVILEGES table",
		SetUpScript: []string{
			"CREATE TABLE checks (a INTEGER PRIMARY KEY, b INTEGER, c VARCHAR(20))",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, c VARCHAR(20), p POINT default (POINT(1,1)))",
			"CREATE USER tester@localhost;",
			"CREATE USER admin@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "root",
				Host:     "localhost",
				Query:    "select * from information_schema.table_privileges;",
				Expected: []sql.Row{},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT INSERT ON mydb.checks TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT UPDATE, GRANT OPTION ON mydb.test TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "select * from information_schema.table_privileges order by privilege_type, is_grantable;/*root*/",
				Expected: []sql.Row{{"'tester'@'localhost'", "def", "mydb", "checks", "INSERT", "NO"}, {"'tester'@'localhost'", "def", "mydb", "test", "UPDATE", "YES"}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "select * from information_schema.table_privileges order by privilege_type, is_grantable;/*tester*/",
				Expected: []sql.Row{{"'tester'@'localhost'", "def", "mydb", "checks", "INSERT", "NO"}, {"'tester'@'localhost'", "def", "mydb", "test", "UPDATE", "YES"}},
			},
			{
				User:     "admin",
				Host:     "localhost",
				Query:    "select * from information_schema.table_privileges order by privilege_type, is_grantable;/*admin1*/",
				Expected: []sql.Row{},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT SELECT ON mysql.* TO admin@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "admin",
				Host:     "localhost",
				Query:    "select * from information_schema.table_privileges order by privilege_type, is_grantable;/*admin2*/",
				Expected: []sql.Row{{"'tester'@'localhost'", "def", "mydb", "checks", "INSERT", "NO"}, {"'tester'@'localhost'", "def", "mydb", "test", "UPDATE", "YES"}},
			},
		},
	},
	{
		Name: "basic tests on information_schema.USER_PRIVILEGES table",
		SetUpScript: []string{
			"CREATE TABLE checks (a INTEGER PRIMARY KEY, b INTEGER, c VARCHAR(20))",
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, c VARCHAR(20), p POINT default (POINT(1,1)))",
			"CREATE USER tester@localhost;",
			"CREATE USER admin@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:  "root",
				Host:  "localhost",
				Query: "select * from information_schema.user_privileges order by privilege_type LIMIT 4;/*root*/",
				Expected: []sql.Row{{"'root'@'localhost'", "def", "ALTER", "YES"},
					{"'root'@'localhost'", "def", "ALTER ROUTINE", "YES"},
					{"'root'@'localhost'", "def", "CREATE", "YES"},
					{"'root'@'localhost'", "def", "CREATE ROLE", "YES"}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT INSERT ON *.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "select * from information_schema.user_privileges order by privilege_type, is_grantable;/*tester1*/",
				Expected: []sql.Row{{"'tester'@'localhost'", "def", "INSERT", "NO"}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT UPDATE, GRANT OPTION ON *.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "select * from information_schema.user_privileges order by privilege_type, is_grantable;/*tester2*/",
				Expected: []sql.Row{{"'tester'@'localhost'", "def", "INSERT", "YES"}, {"'tester'@'localhost'", "def", "UPDATE", "YES"}},
			},
			{
				User:     "admin",
				Host:     "localhost",
				Query:    "select * from information_schema.user_privileges order by privilege_type, is_grantable;/*admin*/",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "basic tests on information_schema.USER_ATTRIBUTES table",
		SetUpScript: []string{
			"CREATE USER tester@localhost;",
			// TODO: attributes info is ignored in sqlparser
			`CREATE USER admin@localhost ATTRIBUTE '{"fname": "Josh", "lname": "Scott"}';`,
			"GRANT UPDATE ON mysql.* TO admin@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:  "root",
				Host:  "localhost",
				Query: "select * from information_schema.user_attributes order by user;/*root*/",
				Expected: []sql.Row{{"admin", "localhost", nil},
					{"root", "localhost", nil},
					{"tester", "localhost", nil}},
			},
			{
				User:  "admin",
				Host:  "localhost",
				Query: "select * from information_schema.user_attributes order by user;/*admin*/",
				Expected: []sql.Row{{"admin", "localhost", nil},
					{"root", "localhost", nil},
					{"tester", "localhost", nil}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "select * from information_schema.user_attributes order by user;/*tester*/",
				Expected: []sql.Row{{"tester", "localhost", nil}},
			},
		},
	},
	{
		Name: "basic privilege tests on information_schema.ROUTINES and PARAMETERS tables",
		SetUpScript: []string{
			"CREATE USER tester@localhost;",
			"CREATE PROCEDURE testabc(IN x DOUBLE, IN y FLOAT, OUT abc DECIMAL(5,1)) SELECT x*y INTO abc",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "select count(*) from information_schema.routines where routine_name = 'testabc'/*tester1*/;",
				Expected: []sql.Row{{0}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "select count(*) from information_schema.parameters where specific_name = 'testabc'/*tester1*/;",
				Expected: []sql.Row{{0}},
			},
			{
				User:     "root",
				Host:     "localhost",
				Query:    "GRANT CREATE ROUTINE ON mydb.* TO tester@localhost;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "select count(*) from information_schema.routines where routine_name = 'testabc';",
				Expected: []sql.Row{{1}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "select count(*) from information_schema.parameters where specific_name = 'testabc';",
				Expected: []sql.Row{{3}},
			},
		},
	},
	{
		Name: "information schema is queryable",
		SetUpScript: []string{
			"CREATE DATABASE testdb;",
			"CREATE USER testadmin@'%';",
			"GRANT ALL ON testdb.* TO testadmin@'%';",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "testadmin",
				Host:     "%",
				Query:    "USE testdb;",
				Expected: []sql.Row{},
			},
			{
				User:     "testadmin",
				Host:     "%",
				Query:    `SELECT SUM(found) FROM ((SELECT 1 as found FROM information_schema.tables) UNION (SELECT 1 as found FROM information_schema.events)) as all_found;`,
				Expected: []sql.Row{{1.0}},
			},
			{
				User:     "testadmin",
				Host:     "%",
				Query:    `(SELECT 1 as found FROM information_schema.tables) UNION (SELECT 1 as found FROM information_schema.events);`,
				Expected: []sql.Row{{1}},
			},
			{
				User:     "testadmin",
				Host:     "%",
				Query:    `SELECT SUM(found) FROM ((SELECT 1 as found FROM dual) UNION (SELECT 1 as found FROM dual)) as all_found;`,
				Expected: []sql.Row{{1.0}},
			},
			{
				User: "testadmin",
				Host: "%",
				Query: `SELECT SUM(found)
FROM ((SELECT 1 as found FROM information_schema.tables WHERE table_schema = 'testdb')
      UNION ALL
      (SELECT 1 as found FROM information_schema.views WHERE table_schema = 'testdb' LIMIT 1)
      UNION ALL
      (SELECT 1 as found FROM information_schema.table_constraints WHERE table_schema = 'testdb' LIMIT 1)
      UNION ALL
      (SELECT 1 as found FROM information_schema.triggers WHERE event_object_schema = 'testdb' LIMIT 1)
      UNION ALL
      (SELECT 1 as found FROM information_schema.routines WHERE routine_schema = 'testdb' LIMIT 1)
      UNION ALL
      (SELECT 1 as found FROM information_schema.events WHERE event_schema = 'testdb' LIMIT 1)) as all_found;`,
				Expected: []sql.Row{{nil}},
			},
		},
	},
	{
		Name: "Test user creation with hashed password",
		SetUpScript: []string{
			"CREATE USER 'lol'@'%' IDENTIFIED WITH mysql_native_password AS '*91D9861DFC07DD967611B8C96953474EF270AD5E';",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				Query: "SELECT User, plugin, authentication_string FROM mysql.user WHERE User = 'lol';",
				Expected: []sql.Row{
					{
						"lol",                   // User
						"mysql_native_password", // plugin
						"*91D9861DFC07DD967611B8C96953474EF270AD5E", // authentication_string
					},
				},
			},
		},
	},
}

// NoopPlaintextPlugin is used to authenticate plaintext user plugins
type NoopPlaintextPlugin struct{}

var _ mysql_db.PlaintextAuthPlugin = &NoopPlaintextPlugin{}

func (p *NoopPlaintextPlugin) Authenticate(db *mysql_db.MySQLDb, user string, userEntry *mysql_db.User, pass string) (bool, error) {
	return pass == "right-password", nil
}

// ServerAuthTests test the server authentication system. These tests always have the root account available, and the
// root account is used with any queries in the SetUpScript, along as being set to the context passed to SetUpFunc.
var ServerAuthTests = []ServerAuthenticationTest{
	{
		Name: "ALTER USER can change passwords",
		Assertions: []ServerAuthenticationTestAssertion{
			// Create test users, privileges, etc
			{
				Username:    "root",
				Password:    "",
				Query:       "CREATE TABLE mydb.test (pk BIGINT PRIMARY KEY);",
				ExpectedErr: false,
			}, {
				// Create a user with CREATE USER privileges
				Username:    "root",
				Password:    "",
				Query:       "CREATE USER `createUserUser`@`localhost` IDENTIFIED BY '';",
				ExpectedErr: false,
			}, {
				Username:    "root",
				Password:    "",
				Query:       "GRANT CREATE USER ON *.* TO `createUserUser`@`localhost`;",
				ExpectedErr: false,
			}, {
				// Create a user with UPDATE privileges on the mysql database
				Username:    "root",
				Password:    "",
				Query:       "CREATE USER `updateUser`@`localhost` IDENTIFIED BY '';",
				ExpectedErr: false,
			}, {
				Username:    "root",
				Password:    "",
				Query:       "GRANT UPDATE ON mysql.* TO `updateUser`@`localhost`;",
				ExpectedErr: false,
			}, {
				// Create a regular user named user1 with SELECT privileges
				Username:    "root",
				Password:    "",
				Query:       "CREATE USER `user1`@`localhost` IDENTIFIED BY '';",
				ExpectedErr: false,
			}, {
				Username:    "root",
				Password:    "",
				Query:       "GRANT SELECT ON *.* TO `user1`@`localhost`;",
				ExpectedErr: false,
			}, {
				// Create a regular user named user2 with SELECT privileges
				Username:    "root",
				Password:    "",
				Query:       "CREATE USER `user2`@`localhost` IDENTIFIED BY '';",
				ExpectedErr: false,
			}, {
				Username:    "root",
				Password:    "",
				Query:       "GRANT SELECT ON *.* TO `user2`@`localhost`;",
				ExpectedErr: false,
			},

			// When IF EXISTS is specified, an error isn't returned if the user doesn't exist
			{
				Username:    "root",
				Password:    "",
				Query:       "ALTER USER IF EXISTS nobody@localhost IDENTIFIED BY 'password';",
				ExpectedErr: false,
			}, {
				Username:       "root",
				Password:       "",
				Query:          "ALTER USER nobody@localhost IDENTIFIED BY 'password';",
				ExpectedErr:    true,
				ExpectedErrStr: "Error 1105 (HY000): Operation ALTER USER failed for 'nobody'@'localhost'",
			},

			// RANDOM PASSWORD is not supported yet, so an error should be returned
			{
				Username:    "root",
				Password:    "",
				Query:       "ALTER USER user2@localhost IDENTIFIED BY RANDOM PASSWORD;",
				ExpectedErr: true,
				ExpectedErrStr: "Error 1105 (HY000): random password generation is not currently supported; " +
					"you can request support at https://github.com/dolthub/dolt/issues/new",
			},

			// root super user can change other account passwords
			{
				Username:    "root",
				Password:    "",
				Query:       "ALTER USER `user1`@`localhost` IDENTIFIED BY 'password1';",
				ExpectedErr: false,
			}, {
				Username:    "user1",
				Password:    "",
				Query:       "SELECT * FROM mydb.test;",
				ExpectedErr: true,
			}, {
				Username:    "user1",
				Password:    "password1",
				Query:       "SELECT * FROM mydb.test;",
				ExpectedErr: false,
			},

			// Accounts with the CREATE USER privilege can change other account passwords
			{
				Username:    "createUserUser",
				Password:    "",
				Query:       "ALTER USER `user1`@`localhost` IDENTIFIED BY 'password2';",
				ExpectedErr: false,
			}, {
				Username:    "user1",
				Password:    "",
				Query:       "SELECT * FROM mydb.test;",
				ExpectedErr: true,
			}, {
				Username:    "user1",
				Password:    "password2",
				Query:       "SELECT * FROM mydb.test;",
				ExpectedErr: false,
			},

			// Accounts with the UPDATE privilege on the mysql db can change other account passwords
			{
				Username:    "updateUser",
				Password:    "",
				Query:       "ALTER USER `user2`@`localhost` IDENTIFIED BY 'password3';",
				ExpectedErr: false,
			}, {
				Username:    "user2",
				Password:    "",
				Query:       "SELECT * FROM mydb.test;",
				ExpectedErr: true,
			}, {
				Username:    "user2",
				Password:    "password3",
				Query:       "SELECT * FROM mydb.test;",
				ExpectedErr: false,
			},

			// Accounts can change their own password
			{
				Username:    "user1",
				Password:    "password2",
				Query:       "ALTER USER `user1`@`localhost` IDENTIFIED BY 'password4';",
				ExpectedErr: false,
			}, {
				Username:    "user1",
				Password:    "",
				Query:       "SELECT * FROM mydb.test;",
				ExpectedErr: true,
			}, {
				Username:    "user1",
				Password:    "password4",
				Query:       "SELECT * FROM mydb.test;",
				ExpectedErr: false,
			},

			// Accounts CANNOT change another account's password (without the CREATE USER or UPDATE privilege)
			{
				Username:    "user1",
				Password:    "password2",
				Query:       "ALTER USER `user2`@`localhost` IDENTIFIED BY 'password5';",
				ExpectedErr: true,
			},
		},
	},
	{
		Name: "DROP USER reports correct string for missing address",
		Assertions: []ServerAuthenticationTestAssertion{
			{
				Username:       "root",
				Password:       "",
				Query:          "DROP USER xyz;",
				ExpectedErrStr: "Error 1105 (HY000): Operation DROP USER failed for 'xyz'@'%'",
			},
		},
	},
	{
		Name: "CREATE USER with a random password is not supported",
		Assertions: []ServerAuthenticationTestAssertion{
			{
				Username:    "root",
				Password:    "",
				Query:       "CREATE USER foo1@localhost IDENTIFIED BY RANDOM PASSWORD;",
				ExpectedErr: true,
				ExpectedErrStr: "Error 1105 (HY000): random password generation is not currently supported; " +
					"you can request support at https://github.com/dolthub/dolt/issues/new",
			},
		},
	},
	{
		Name: "CREATE USER with an empty password",
		Assertions: []ServerAuthenticationTestAssertion{
			{
				Username:    "root",
				Password:    "",
				Query:       "CREATE TABLE mydb.test (pk BIGINT PRIMARY KEY);",
				ExpectedErr: false,
			},
			{
				Username:    "root",
				Password:    "",
				Query:       "CREATE USER rand_user@localhost IDENTIFIED BY '';",
				ExpectedErr: false,
			},
			{
				Username:    "root",
				Password:    "",
				Query:       "GRANT ALL ON *.* TO rand_user@localhost;",
				ExpectedErr: false,
			},
			{
				Username:    "rand_user",
				Password:    "",
				Query:       "SELECT * FROM mydb.test;",
				ExpectedErr: false,
			},
		},
	},
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
				Username:           "rand_user",
				Password:           "rand_pass",
				Query:              "SELECT * FROM mysql.user;",
				ExpectedAuthPlugin: "mysql_native_password",
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
		Name: "Create User explicitly with mysql_native_password plugin",
		SetUpScript: []string{
			"CREATE USER ranuse@localhost IDENTIFIED WITH mysql_native_password BY 'ranpas';",
			"GRANT ALL ON *.* TO ranuse@localhost WITH GRANT OPTION;",
		},
		Assertions: []ServerAuthenticationTestAssertion{
			{
				Username:           "ranuse",
				Password:           "ranpas",
				Query:              "SELECT * FROM mysql.user;",
				ExpectedAuthPlugin: "mysql_native_password",
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
		Name: "Create User explicitly with caching_sha2_password plugin",
		SetUpScript: []string{
			// testuser1 is created with a password
			"CREATE USER testuser1@localhost IDENTIFIED WITH caching_sha2_password BY 'mypassword3';",
			"GRANT ALL ON *.* TO testuser1@localhost WITH GRANT OPTION;",
			// testuser2 is created without a password
			"CREATE USER testuser2@localhost IDENTIFIED WITH caching_sha2_password;",
			"GRANT ALL ON *.* TO testuser2@localhost WITH GRANT OPTION;",
		},
		Assertions: []ServerAuthenticationTestAssertion{
			{
				Username:           "testuser1",
				Password:           "mypassword3",
				Query:              "SELECT * FROM mysql.user;",
				ExpectedAuthPlugin: "caching_sha2_password",
			},
			{
				Username:       "testuser1",
				Password:       "what",
				Query:          "SELECT * FROM mysql.user;",
				ExpectedErr:    true,
				ExpectedErrStr: "Error 1045 (28000): Access denied for user 'testuser1'",
			},
			{
				Username:       "testuser1",
				Password:       "",
				Query:          "SELECT * FROM mysql.user;",
				ExpectedErr:    true,
				ExpectedErrStr: "Error 1045 (28000): Access denied for user 'testuser1'",
			},
			{
				Username:       "testuser2",
				Password:       "wrong",
				Query:          "SELECT * FROM mysql.user;",
				ExpectedErr:    true,
				ExpectedErrStr: "Error 1045 (28000): Access denied for user 'testuser2'",
			},
			{
				Username:           "testuser2",
				Password:           "",
				Query:              "SELECT * FROM mysql.user;",
				ExpectedErr:        false,
				ExpectedAuthPlugin: "caching_sha2_password",
			},
		},
	},
	{
		Name: "Migrate user from mysql_native_password to caching_sha2_password",
		SetUpScript: []string{
			// testuser1 is created with a password
			"CREATE USER testuser1@localhost IDENTIFIED WITH mysql_native_password BY 'mypassword3';",
			"GRANT ALL ON *.* TO testuser1@localhost WITH GRANT OPTION;",
		},
		Assertions: []ServerAuthenticationTestAssertion{
			{
				Username:           "testuser1",
				Password:           "mypassword3",
				Query:              "SELECT * FROM mysql.user;",
				ExpectedAuthPlugin: "mysql_native_password",
			},
			{
				Username: "root",
				Query:    "ALTER USER testuser1@localhost IDENTIFIED WITH caching_sha2_password BY 'pass1';",
			},
			{
				Username:           "testuser1",
				Password:           "pass1",
				Query:              "SELECT * FROM mysql.user;",
				ExpectedAuthPlugin: "caching_sha2_password",
			},
			{
				Username:       "testuser1",
				Password:       "wrong",
				Query:          "SELECT * FROM mysql.user;",
				ExpectedErr:    true,
				ExpectedErrStr: "Error 1045 (28000): Access denied for user 'testuser1'",
			},
			{
				Username: "root",
				Query:    "ALTER USER testuser1@localhost IDENTIFIED WITH caching_sha2_password;",
			},
			{
				Username:           "testuser1",
				Password:           "",
				Query:              "SELECT * FROM mysql.user;",
				ExpectedAuthPlugin: "caching_sha2_password",
			},
			{
				Username:       "testuser1",
				Password:       "wrong",
				Query:          "SELECT * FROM mysql.user;",
				ExpectedErr:    true,
				ExpectedErrStr: "Error 1045 (28000): Access denied for user 'testuser1'",
			},
		},
	},
	{
		Name: "Create User with jwt plugin specification",
		SetUpScript: []string{
			"CREATE USER `test-user`@localhost IDENTIFIED WITH authentication_dolt_jwt AS 'jwks=testing,sub=test-user,iss=dolthub.com,aud=some_id';",
			"GRANT ALL ON *.* TO `test-user`@localhost WITH GRANT OPTION;",
		},
		SetUpFunc: func(ctx *sql.Context, t *testing.T, engine *sqle.Engine) {
			plugins := map[string]mysql_db.PlaintextAuthPlugin{"authentication_dolt_jwt": &NoopPlaintextPlugin{}}
			engine.EngineAnalyzer().Catalog.MySQLDb.SetPlugins(plugins)
		},
		Assertions: []ServerAuthenticationTestAssertion{
			{
				Username:       "test-user",
				Password:       "what",
				Query:          "SELECT * FROM mysql.user;",
				ExpectedErr:    true,
				ExpectedErrStr: "Error 1045 (28000): Access denied for user 'test-user'",
			},
			{
				Username:       "test-user",
				Password:       "",
				Query:          "SELECT * FROM mysql.user;",
				ExpectedErr:    true,
				ExpectedErrStr: "Error 1045 (28000): Access denied for user 'test-user'",
			},
			{

				Username:           "test-user",
				Password:           "right-password",
				Query:              "SELECT * FROM mysql.user;",
				ExpectedAuthPlugin: "authentication_dolt_jwt",
			},
		},
	},
	{
		Name: "Adding a Super User directly",
		SetUpFunc: func(ctx *sql.Context, t *testing.T, engine *sqle.Engine) {
			ed := engine.EngineAnalyzer().Catalog.MySQLDb.Editor()
			defer ed.Close()
			engine.EngineAnalyzer().Catalog.MySQLDb.AddSuperUser(ed, "bestuser", "localhost", "the_pass")
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

// QuickPrivTests are test that specifically attempt to test as many privileges against as many statements as possible,
// while being as succinct as possible. All tests here could be fully represented as a UserPrivilegeTest, however each
// equivalent test would comparatively take up many more lines. This is intended to have as many tests as possible that
// are as quick to write as possible.
var QuickPrivTests = []QuickPrivilegeTest{
	{
		Queries: []string{
			"GRANT SELECT ON *.* TO tester@localhost",
			"SELECT * FROM mydb.test",
		},
		Expected: []sql.Row{{0, 0}, {1, 1}},
	},
	{
		Queries: []string{
			"GRANT SELECT ON mydb.* TO tester@localhost",
			"SELECT * FROM mydb.test",
		},
		Expected: []sql.Row{{0, 0}, {1, 1}},
	},
	{
		Queries: []string{
			"GRANT SELECT ON mydb.* TO tester@localhost",
			"SELECT * FROM mydb.test2",
		},
		Expected: []sql.Row{{0, 1}, {1, 2}},
	},
	{
		Queries: []string{
			"GRANT SELECT ON mydb.test TO tester@localhost",
			"SELECT * FROM mydb.test",
		},
		Expected: []sql.Row{{0, 0}, {1, 1}},
	},
	{
		Queries: []string{
			"GRANT SELECT ON mydb.test TO tester@localhost",
			"SELECT * FROM mydb.test2",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT SELECT ON otherdb.* TO tester@localhost",
			"SELECT * FROM mydb.test",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT SELECT ON otherdb.test TO tester@localhost",
			"SELECT * FROM mydb.test",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT SELECT ON otherdb.test TO tester@localhost",
			"SELECT * FROM mydb.test",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT SELECT ON *.* TO tester@localhost",
			"USE mydb;",
			"SHOW TABLES;",
		},
		Expected: []sql.Row{{"test"}, {"test2"}},
	},
	{
		Queries: []string{
			"GRANT SELECT ON mydb.* TO tester@localhost",
			"USE mydb;",
			"SHOW TABLES;",
		},
		Expected: []sql.Row{{"test"}, {"test2"}},
	},
	{
		Queries: []string{
			"GRANT SELECT ON mydb.test TO tester@localhost",
			"USE mydb;",
			"SHOW TABLES;",
		},
		Expected: []sql.Row{{"test"}},
	},
	{
		Queries: []string{
			"GRANT SELECT ON mydb.non_exist TO tester@localhost",
			"USE mydb;",
			"SHOW TABLES;",
		},
		Expected: []sql.Row{},
	},
	{
		Queries: []string{
			"ALTER TABLE mydb.test ADD COLUMN new_column BIGINT;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT ALTER ON *.* TO tester@localhost",
			"ALTER TABLE mydb.test ADD COLUMN new_column BIGINT",
		},
	},
	{
		Queries: []string{
			"GRANT ALTER ON mydb.* TO tester@localhost",
			"ALTER TABLE mydb.test ADD COLUMN new_column BIGINT;",
		},
	},
	{
		Queries: []string{
			"GRANT ALTER ON mydb.test TO tester@localhost",
			"ALTER TABLE mydb.test ADD COLUMN new_column BIGINT;",
		},
	},
	{
		Queries: []string{
			"ALTER TABLE mydb.test RENAME TO mydb.new_test;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT ALTER ON *.* TO tester@localhost",
			"ALTER TABLE mydb.test RENAME TO mydb.new_test;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT ALTER, CREATE, DROP, INSERT ON *.* TO tester@localhost",
			"ALTER TABLE mydb.test RENAME TO mydb.new_test;",
		},
	},
	{
		Queries: []string{
			"GRANT ALTER, CREATE, DROP, INSERT ON mydb.* TO tester@localhost",
			"ALTER TABLE mydb.test RENAME TO mydb.new_test;",
		},
	},
	{
		Queries: []string{
			"GRANT ALTER, CREATE, DROP, INSERT ON mydb.test TO tester@localhost",
			"ALTER TABLE mydb.test RENAME TO mydb.new_test;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT ALTER, DROP ON mydb.test TO tester@localhost",
			"GRANT CREATE, INSERT ON mydb.new_test TO tester@localhost",
			"ALTER TABLE mydb.test RENAME TO mydb.new_test;",
		},
	},
	{
		Queries: []string{
			"GRANT ALTER ON mydb.test TO tester@localhost",
			"GRANT CREATE, INSERT ON mydb.new_test TO tester@localhost",
			"ALTER TABLE mydb.test RENAME TO mydb.new_test;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT DROP ON mydb.test TO tester@localhost",
			"GRANT CREATE, INSERT ON mydb.new_test TO tester@localhost",
			"ALTER TABLE mydb.test RENAME TO mydb.new_test;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT ALTER, DROP ON mydb.test TO tester@localhost",
			"GRANT CREATE ON mydb.new_test TO tester@localhost",
			"ALTER TABLE mydb.test RENAME TO mydb.new_test;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT ALTER, DROP ON mydb.test TO tester@localhost",
			"GRANT INSERT ON mydb.new_test TO tester@localhost",
			"ALTER TABLE mydb.test RENAME TO mydb.new_test;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"USE mydb;",
			"CREATE PROCEDURE new_proc (x DOUBLE, y DOUBLE) SELECT x*y;",
			"DROP PROCEDURE new_proc;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT ALTER ROUTINE ON *.* TO tester@localhost",
			"USE mydb;",
			"CREATE PROCEDURE new_proc (x DOUBLE, y DOUBLE) SELECT x*y;",
			"DROP PROCEDURE new_proc;",
		},
	},
	{
		Queries: []string{
			"GRANT ALTER ROUTINE ON mydb.* TO tester@localhost",
			"USE mydb;",
			"CREATE PROCEDURE new_proc (x DOUBLE, y DOUBLE) SELECT x*y;",
			"DROP PROCEDURE new_proc;",
		},
	},
	{
		Queries: []string{
			"CREATE DATABASE new_db;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"CREATE TABLE mydb.new_table (pk BIGINT PRIMARY KEY);",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT CREATE ON *.* TO tester@localhost",
			"CREATE DATABASE new_db2;",
			"GRANT DROP ON *.* TO tester@localhost",
			"drop database new_db2",
		},
	},
	{
		Queries: []string{
			"GRANT CREATE ON *.* TO tester@localhost",
			"CREATE TABLE mydb.new_table (pk BIGINT PRIMARY KEY);",
		},
	},
	{
		Queries: []string{
			"GRANT CREATE ON mydb.* TO tester@localhost",
			"CREATE DATABASE new_db3;",
			"GRANT DROP ON *.* TO tester@localhost",
			"drop database new_db3",
		},
	},
	{
		Queries: []string{
			"GRANT CREATE ON mydb.* TO tester@localhost",
			"CREATE TABLE mydb.new_table (pk BIGINT PRIMARY KEY);",
		},
	},
	{
		Queries: []string{
			"CREATE ROLE new_role;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT CREATE ROLE ON *.* TO tester@localhost",
			"CREATE ROLE new_role;",
		},
	},
	{
		Queries: []string{
			"USE mydb;",
			"CREATE PROCEDURE new_proc (x DOUBLE, y DOUBLE) SELECT x*y;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT CREATE ROUTINE ON *.* TO tester@localhost",
			"USE mydb;",
			"CREATE PROCEDURE new_proc (x DOUBLE, y DOUBLE) SELECT x*y;",
		},
	},
	{
		Queries: []string{
			"GRANT CREATE ROUTINE ON mydb.* TO tester@localhost",
			"USE mydb;",
			"CREATE PROCEDURE new_proc (x DOUBLE, y DOUBLE) SELECT x*y;",
		},
	},
	{
		Queries: []string{
			"CREATE USER new_user;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"CREATE USER new_user;",
			"DROP USER new_user;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT CREATE USER ON *.* TO tester@localhost",
			"CREATE USER new_user;",
		},
	},
	{
		Queries: []string{
			"GRANT CREATE USER ON *.* TO tester@localhost",
			"CREATE USER new_user;",
			"DROP USER new_user;",
		},
	},
	{
		Queries: []string{
			"GRANT CREATE USER ON *.* TO tester@localhost",
			"CREATE ROLE new_role;",
		},
	},
	{
		Queries: []string{
			"GRANT CREATE USER ON *.* TO tester@localhost",
			"CREATE ROLE new_role;",
			"DROP ROLE new_role;",
		},
	},
	{
		Queries: []string{
			"CREATE VIEW new_view AS SELECT 1;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT CREATE VIEW ON *.* TO tester@localhost",
			"CREATE VIEW new_view AS SELECT 1;",
		},
	},
	{
		Queries: []string{
			"DELETE FROM mydb.test WHERE pk >= 0;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT DELETE ON *.* TO tester@localhost",
			"DELETE FROM mydb.test WHERE pk >= 0;",
		},
	},
	{
		Queries: []string{
			"GRANT DELETE ON mydb.* TO tester@localhost",
			"DELETE FROM mydb.test WHERE pk >= 0;",
		},
	},
	{
		Queries: []string{
			"GRANT DELETE ON mydb.test TO tester@localhost",
			"DELETE FROM mydb.test WHERE pk >= 0;",
		},
	},
	{
		Queries: []string{
			"DELETE test, test2 FROM mydb.test join mydb.test2 where test.pk=test2.pk",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT DELETE ON mydb.test TO tester@localhost",
			"DELETE test, test2 FROM mydb.test join mydb.test2 where test.pk=test2.pk",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT DELETE ON mydb.test2 TO tester@localhost",
			"DELETE test, test2 FROM mydb.test join mydb.test2 where test.pk=test2.pk",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT DELETE ON mydb.test TO tester@localhost",
			"GRANT DELETE ON mydb.test2 TO tester@localhost",
			"DELETE test, test2 FROM mydb.test join mydb.test2 where test.pk=test2.pk",
		},
	},
	{
		Queries: []string{
			"CREATE DATABASE new_db4;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"CREATE TABLE mydb.new_table (pk BIGINT PRIMARY KEY);",
			"DROP TABLE mydb.new_table;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"CREATE VIEW new_view AS SELECT 1;",
			"DROP VIEW new_view;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT DROP ON *.* TO tester@localhost",
			"CREATE DATABASE new_db5;",
			"GRANT DROP ON *.* TO tester@localhost",
			"DROP DATABASE new_db5;",
		},
	},
	{
		Queries: []string{
			"GRANT DROP ON *.* TO tester@localhost",
			"CREATE TABLE mydb.new_table (pk BIGINT PRIMARY KEY);",
			"DROP TABLE mydb.new_table;",
		},
	},
	{
		Queries: []string{
			"GRANT DROP ON *.* TO tester@localhost",
			"CREATE TABLE mydb.new_table1 (pk BIGINT PRIMARY KEY);",
			"CREATE TABLE mydb.new_table2 (pk BIGINT PRIMARY KEY);",
			"DROP TABLE mydb.new_table1, mydb.new_table2;",
		},
	},
	{
		Queries: []string{
			"GRANT DROP ON *.* TO tester@localhost",
			"CREATE VIEW new_view AS SELECT 1;",
			"DROP VIEW new_view;",
		},
	},
	{
		Queries: []string{
			"GRANT DROP ON mydb.* TO tester@localhost",
			"CREATE TABLE mydb.new_table (pk BIGINT PRIMARY KEY);",
			"DROP TABLE mydb.new_table;",
		},
	},
	{
		Queries: []string{
			"GRANT DROP ON mydb.* TO tester@localhost",
			"CREATE TABLE mydb.new_table1 (pk BIGINT PRIMARY KEY);",
			"CREATE TABLE mydb.new_table2 (pk BIGINT PRIMARY KEY);",
			"DROP TABLE mydb.new_table1, mydb.new_table2;",
		},
	},
	{
		Queries: []string{
			"GRANT DROP ON mydb.new_table TO tester@localhost",
			"CREATE TABLE mydb.new_table (pk BIGINT PRIMARY KEY);",
			"DROP TABLE mydb.new_table;",
		},
	},
	{
		Queries: []string{
			"GRANT DROP ON mydb.new_table1 TO tester@localhost",
			"CREATE TABLE mydb.new_table1 (pk BIGINT PRIMARY KEY);",
			"CREATE TABLE mydb.new_table2 (pk BIGINT PRIMARY KEY);",
			"DROP TABLE mydb.new_table1, mydb.new_table2;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT DROP ON mydb.new_table2 TO tester@localhost",
			"CREATE TABLE mydb.new_table1 (pk BIGINT PRIMARY KEY);",
			"CREATE TABLE mydb.new_table2 (pk BIGINT PRIMARY KEY);",
			"DROP TABLE mydb.new_table1, mydb.new_table2;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT DROP ON mydb.new_table1 TO tester@localhost",
			"GRANT DROP ON mydb.new_table2 TO tester@localhost",
			"CREATE TABLE mydb.new_table1 (pk BIGINT PRIMARY KEY);",
			"CREATE TABLE mydb.new_table2 (pk BIGINT PRIMARY KEY);",
			"DROP TABLE mydb.new_table1, mydb.new_table2;",
		},
	},
	{
		Queries: []string{
			"CREATE ROLE new_role;",
			"DROP ROLE new_role;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT DROP ROLE ON *.* TO tester@localhost",
			"CREATE ROLE new_role;",
			"DROP ROLE new_role;",
		},
	},
	{
		Queries: []string{
			"CREATE INDEX new_idx ON mydb.test (v1);",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"CREATE INDEX new_idx ON mydb.test (v1);",
			"DROP INDEX new_idx ON mydb.test;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT INDEX ON *.* TO tester@localhost",
			"CREATE INDEX new_idx ON mydb.test (v1);",
		},
	},
	{
		Queries: []string{
			"GRANT INDEX ON *.* TO tester@localhost",
			"CREATE INDEX new_idx ON mydb.test (v1);",
			"DROP INDEX new_idx ON mydb.test;",
		},
	},
	{
		Queries: []string{
			"GRANT INDEX ON mydb.* TO tester@localhost",
			"CREATE INDEX new_idx ON mydb.test (v1);",
		},
	},
	{
		Queries: []string{
			"GRANT INDEX ON mydb.* TO tester@localhost",
			"CREATE INDEX new_idx ON mydb.test (v1);",
			"DROP INDEX new_idx ON mydb.test;",
		},
	},
	{
		Queries: []string{
			"GRANT INDEX ON mydb.test TO tester@localhost",
			"CREATE INDEX new_idx ON mydb.test (v1);",
		},
	},
	{
		Queries: []string{
			"GRANT INDEX ON mydb.test TO tester@localhost",
			"CREATE INDEX new_idx ON mydb.test (v1);",
			"DROP INDEX new_idx ON mydb.test;",
		},
	},
	{
		Queries: []string{
			"INSERT INTO mydb.test VALUES (9, 9);",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT INSERT ON *.* TO tester@localhost",
			"INSERT INTO mydb.test VALUES (9, 9);",
		},
	},
	{
		Queries: []string{
			"GRANT INSERT ON mydb.* TO tester@localhost",
			"INSERT INTO mydb.test VALUES (9, 9);",
		},
	},
	{
		Queries: []string{
			"GRANT INSERT ON mydb.test TO tester@localhost",
			"INSERT INTO mydb.test VALUES (9, 9);",
		},
	},
	{
		Queries: []string{
			"CREATE TRIGGER new_trig BEFORE INSERT ON mydb.test2 FOR EACH ROW SET NEW.v1 = NEW.pk * NEW.v1;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"CREATE TRIGGER new_trig BEFORE INSERT ON mydb.test2 FOR EACH ROW SET NEW.v1 = NEW.pk * NEW.v1;",
			"DROP TRIGGER new_trig;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT TRIGGER ON *.* TO tester@localhost",
			"CREATE TRIGGER new_trig BEFORE INSERT ON mydb.test2 FOR EACH ROW SET NEW.v1 = NEW.pk * NEW.v1;",
		},
	},
	{
		Queries: []string{
			"GRANT TRIGGER ON *.* TO tester@localhost",
			"CREATE TRIGGER new_trig BEFORE INSERT ON mydb.test2 FOR EACH ROW SET NEW.v1 = NEW.pk * NEW.v1;",
			"DROP TRIGGER new_trig;",
		},
	},
	{
		Queries: []string{
			"GRANT TRIGGER ON mydb.* TO tester@localhost",
			"CREATE TRIGGER new_trig BEFORE INSERT ON mydb.test2 FOR EACH ROW SET NEW.v1 = NEW.pk * NEW.v1;",
		},
	},
	{
		Queries: []string{
			"GRANT TRIGGER ON mydb.* TO tester@localhost",
			"CREATE TRIGGER new_trig BEFORE INSERT ON mydb.test2 FOR EACH ROW SET NEW.v1 = NEW.pk * NEW.v1;",
			"DROP TRIGGER new_trig;",
		},
	},
	{
		Queries: []string{
			"UPDATE mydb.test SET v1 = 0;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT UPDATE ON *.* TO tester@localhost",
			"UPDATE mydb.test SET v1 = 0;",
		},
	},
	{
		Queries: []string{
			"GRANT UPDATE ON mydb.* TO tester@localhost",
			"UPDATE mydb.test SET v1 = 0;",
		},
	},
	{
		Queries: []string{
			"GRANT UPDATE ON mydb.test TO tester@localhost",
			"UPDATE mydb.test SET v1 = 0;",
		},
	},
	{
		Queries: []string{
			"FLUSH PRIVILEGES;",
		},
		ExpectingErr: true,
	},
	{
		Queries: []string{
			"GRANT RELOAD ON *.* TO tester@localhost",
			"FLUSH PRIVILEGES;",
		},
	},
}
