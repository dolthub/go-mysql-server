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
	"github.com/dolthub/go-mysql-server/sql/plan"
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
	Username    string
	Password    string
	Query       string
	ExpectedErr bool
}

// UserPrivTests test the user and privilege systems. These tests always have the root account available, and the root
// account is used with any queries in the SetUpScript.
var UserPrivTests = []UserPrivilegeTest{
	{
		Name: "Basic database and table name visibility",
		SetUpScript: []string{
			"CREATE TABLE mydb.test (pk BIGINT PRIMARY KEY);",
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
				Query:       "SELECT * FROM mydb.test2;/*1*/",
				ExpectedErr: sql.ErrDatabaseAccessDeniedForUser,
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "SELECT * FROM mydb.test;/*6*/",
				Expected: []sql.Row{{1}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.OkResult{
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.OkResult{
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
		},
	},
	{
		Name: "Basic revoke all global static privileges",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY);",
			"INSERT INTO test VALUES (1), (2), (3);",
			"CREATE USER tester@localhost;",
			"GRANT ALL ON *.* TO tester@localhost;",
		},
		Assertions: []UserPrivilegeTestAssertion{
			{
				User:     "tester",
				Host:     "localhost",
				Query:    "INSERT INTO test VALUES (4);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Query:    "SELECT User, Host, Select_priv, Insert_priv FROM mysql.user WHERE User = 'tester';",
				Expected: []sql.Row{{"tester", "localhost", "N", "N"}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
			"CREATE DATABASE new_db;",
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
			"CREATE DATABASE new_db;",
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
			"CREATE DATABASE new_db;",
			"DROP DATABASE new_db;",
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
			"CREATE DATABASE new_db;",
			"DROP DATABASE new_db;",
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
